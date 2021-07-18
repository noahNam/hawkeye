import json
import os
import logging
import boto3

import psycopg2
from datetime import datetime
from typing import List

from psycopg2.extensions import STATUS_BEGIN
from psycopg2.extras import execute_values

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# set rds
host = os.environ.get("HOST")
user = os.environ.get("USER")
password = os.environ.get("PASSWORD")
database = os.environ.get("DATABASE")
port = int(os.environ.get("PORT"))

AWS_ACCESS_KEY = os.environ.get("ACCESS_KEY")
AWS_SECRET_ACCESS_KEY = os.environ.get("SECRET_ACCESS_KEY")
AWS_REGION_NAME = os.environ.get("AWS_REGION_NAME")

# SQS
SQS_BASE = os.environ.get("SQS_BASE")
SQS_NAME = os.environ.get("SQS_NAME")

ENDPOINT_PREFIX = os.environ.get("ENDPOINT_PREFIX")

conn = None


def openConnection():
    """
    conn.closed
    1 -> STATUS_BEGIN
    0 -> STATUS_READY
    """
    global conn
    try:
        logger.info("Opening Connection")
        if conn is None:
            conn = psycopg2.connect(
                host=host, dbname=database, user=user, password=password, port=port, connect_timeout=5)
            logger.info("conn.status is %s", conn.status)
        elif conn.status == STATUS_BEGIN:
            conn = psycopg2.connect(
                host=host, dbname=database, user=user, password=password, port=port, connect_timeout=5)
            logger.info("conn.status is %s", conn.status)

    except Exception as e:
        logger.exception("Unexpected error: Could not connect to RDS instance. %s", e)
        raise e


def update_notification_schema(query_result: List):
    logger.info("Update notification schema start")
    update_data = list()
    for data in query_result:
        body = json.loads(data['Body'])
        endpoint_arn = json.loads(body['Message'])['EndpointArn']
        user_endpoint = endpoint_arn.split(ENDPOINT_PREFIX)[1]

        # stats = 2 (failure)
        update_data.append((user_endpoint, datetime.now(), 2))
    try:
        openConnection()
        with conn.cursor() as cur:
            execute_values(cur,
                           """
                           update notifications
                           set status=data.status, updated_at=data.updated_at
                           from (VALUES %s) as data (endpoint, updated_at, status)
                           where notifications.endpoint=data.endpoint
                           """, update_data)
            conn.commit()
        logger.info("Update notification schema end")
    except Exception as e:
        logger.exception("Error while opening connection or processing. %s", e)
    finally:
        logger.info("Closing Connection")
        if conn is not None and conn.status == STATUS_BEGIN:
            conn.close()


def receive_sqs():
    _sqs = boto3.client(
        "sqs",
        region_name=AWS_REGION_NAME,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )

    total = 0
    msg_list = []
    try:
        for message in get_sqs_message(_sqs):
            if message['Body']:
                msg_list.append(message)
                total += 1
                logger.info("[receive_sqs] Target Message %s", message['Body'])
    except Exception as e:
        logger.debug("SQS Fail : {}".format(e))

    # 처리 로직
    update_notification_schema(msg_list)
    ##########################################

    if msg_list:
        entries = [
            {'Id': msg['MessageId'], 'ReceiptHandle': msg['ReceiptHandle']}
            for msg in msg_list
        ]
        try:
            resp = _sqs.delete_message_batch(
                QueueUrl=SQS_BASE + "/" + SQS_NAME, Entries=entries
            )
        except Exception as e:
            logger.exception("Error while delete messages or processing. %s", e)

        if len(resp['Successful']) != len(entries):
            raise RuntimeError(
                f"Failed to delete messages: entries={entries!r} resp={resp!r}"
            )

    dict_ = {
        'result': True, 'total': total,
    }
    return dict_


def get_sqs_message(sqs_client) -> bool:
    _sqs = sqs_client
    __target_q = None

    while True:
        if not __target_q:
            __target_q = (
                    SQS_BASE
                    + "/"
                    + SQS_NAME
            )
            logger.debug(
                "[receive_after_delete] Target Queue {0}".format(__target_q)
            )

        # SQS에 큐가 비워질때까지 메세지 조회
        resp = _sqs.receive_message(
            QueueUrl=__target_q,
            AttributeNames=['All'],
            MaxNumberOfMessages=10
        )

        try:
            """
            제너레이터는 함수 끝까지 도달하면 StopIteration 예외가 발생. 
            마찬가지로 return도 함수를 끝내므로 return을 사용해서 함수 중간에 빠져나오면 StopIteration 예외가 발생.
            특히 제너레이터 안에서 return에 반환값을 지정하면 StopIteration 예외의 에러 메시지로 들어감
            """
            yield from resp['Messages']
        except KeyError:
            # not has next
            return False


def lambda_handler(event, context):
    result = receive_sqs()
    return result
