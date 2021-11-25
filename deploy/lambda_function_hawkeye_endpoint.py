import json
import os
import logging

from datetime import datetime
from typing import List

import psycopg2
import requests

from psycopg2.extensions import STATUS_BEGIN

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# set rds
host = os.environ.get("HOST")
user = os.environ.get("USER")
password = os.environ.get("PASSWORD")
database = os.environ.get("DATABASE")
port = int(os.environ.get("PORT"))

# SLACK
SLACK_TOKEN = os.environ.get("SLACK_TOKEN")
CHANNEL = os.environ.get("CHANNEL")

endpoint_prefix = os.environ.get("ENDPOINT_PREFIX")

conn = None


def send_slack_message(message, title):
    text = title + " -> " + message
    requests.post(
        "https://slack.com/api/chat.postMessage",
        headers={"Authorization": "Bearer " + SLACK_TOKEN},
        data={"channel": CHANNEL, "text": text},
    )


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
                host=host,
                dbname=database,
                user=user,
                password=password,
                port=port,
                connect_timeout=5,
            )
            logger.info("conn.status is %s", conn.status)
        elif conn.status == STATUS_BEGIN:
            conn = psycopg2.connect(
                host=host,
                dbname=database,
                user=user,
                password=password,
                port=port,
                connect_timeout=5,
            )
            logger.info("conn.status is %s", conn.status)

    except Exception as e:
        logger.info("Unexpected error: Could not connect to RDS instance. %s", e)
        raise e


def update_user_endpoint(msg_list: List):
    try:
        openConnection()
        with conn.cursor() as cur:
            for msg in msg_list:
                data = json.loads(msg)
                try:
                    query = """
                           update devices 
                           set endpoint=%s, updated_at=%s
                           where user_id=%s
                    """
                    record_to_update = (data['endpoint'].split(endpoint_prefix)[1], datetime.now(), int(data['user_id']))
                    cur.execute(query, record_to_update)
                    conn.commit()
                except Exception as e:
                    logger.exception("Error while update endpoint to user devices schema %s", e)
                    # 처리되지 못한 message list에서 삭제처리 -> message delete 처리 차단
                    msg_list.remove(msg)

        logger.info("update endpoint to user devices schema end")
    except Exception as e:
        logger.exception("Error while opening connection or processing. %s", e)
        send_slack_message(
            message="Exception: {}".format(str(e)),
            title="☠️ [FAIL] Hawkeye endpoint lambda : "
        )
    finally:
        if conn or conn.is_connected():
            conn.close()
            logger.info("Closing Connection")


def receive_sqs(event):
    total = 0
    msg_list = []
    try:
        for message in event["Records"]:
            if message["body"]:
                msg_list.append(message["body"])
                total += 1
                logger.info("[receive_sqs] Target Message %s", message["body"])
    except Exception as e:
        logger.info("SQS Fail : {}".format(e))
        send_slack_message(
            message="Exception: {}".format(str(e)),
            title="🚀 [FAIL] Hawkeye endpoint lambda : "
        )

    # 처리 로직 ###############################
    update_user_endpoint(msg_list)
    #########################################

    dict_ = {
        "result": True,
        "total": total,
    }
    return dict_


def lambda_handler(event, context):
    result: dict = receive_sqs(event)
    return result
