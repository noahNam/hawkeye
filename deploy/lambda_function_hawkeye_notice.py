import collections
import json
import time
from http import HTTPStatus
from typing import List, Union
from datetime import datetime

import psycopg2
import os
import boto3
import logging

from botocore.exceptions import ClientError

import requests
from psycopg2.extensions import STATUS_BEGIN
from psycopg2.extras import execute_values

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# set rds
host = os.environ.get("HOST")
user = os.environ.get("USER")
password = os.environ.get("PASSWORD")
database = os.environ.get("DATABASE")
port = os.environ.get("PORT")

# set sns
topic_arn = os.environ.get("TOPIC_ARN")
application_arn = os.environ.get("APPLICATION_ARN")
endpoint_prefix = os.environ.get("ENDPOINT_PREFIX")

# set enum
TOPIC = "apt001"

# status
WAIT = 0
SUCCESS = 1
FAILURE = 2

# set sqs
sqs_name = os.environ.get("SQS_NAME")

# SLACK
SLACK_TOKEN = os.environ.get("SLACK_TOKEN")
CHANNEL = os.environ.get("CHANNEL")

# admin_user_id
ADMIN_USER_ID = os.environ.get("ADMIN_USER_ID")

custom_user_data_prefix = os.environ.get("CUSTOM_USER_DATA_PREFIX")

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


def send_sns_notification(query_result: List, target_user_to_update_endpoint: List):
    """
    Reference structure
    Topic subscription –> Platform application ARN –> User Device Token
    """
    # set boto3
    sns_client = boto3.client("sns")
    sns_resource = boto3.resource("sns")

    # set platform application
    platform_application = sns_resource.PlatformApplication(application_arn)
    # set topic
    topic = sns_resource.Topic(topic_arn)

    for data in query_result:
        endpoint_attributes = None
        update_needed = False
        create_needed = True if (data[1] == endpoint_prefix) else False

        if not create_needed:
            try:
                # get SNS endpoint
                endpoint_attributes = sns_client.get_endpoint_attributes(
                    EndpointArn=data[1]
                )

            except Exception as e:
                logger.info("Could not get endpoint attributes. %s", e)
                create_needed = True

        if create_needed:
            try:
                endpoint_attributes = create_endpoint(
                    platform_application, data, sns_client, topic
                )
            except Exception as e:
                logger.info(
                    "user_id(%s)-already exists with the same Token, but different attributes. %s",
                    data[3],
                    e,
                )
                # DB status update
                set_data_to_update_to_database(data=data, idx=5, value=FAILURE)
                continue
            except UnboundLocalError as e:
                logger.info("topic.subscribe error. %s", e)
                # DB status update
                set_data_to_update_to_database(data=data, idx=5, value=FAILURE)
                continue

        """
            Token update 경우
            1. 토큰이 다름 (notifications.user_id가 push 보내기 전 update 된 경우)
            - Token update
            2. 토큰이 같음
            - endpoint enabled == false 라도, Token update 의미가 없음 (바로 다시 비활성화 되기 때문에)

        """
        if endpoint_attributes["Attributes"]["Token"] != data[4]:
            update_needed = True
        else:
            if endpoint_attributes["Attributes"]["Enabled"] == "false":
                # DB status update
                set_data_to_update_to_database(data=data, idx=5, value=FAILURE)
                continue

        if update_needed:
            try:
                params = dict()
                params["Token"] = data[4]
                params["Enabled"] = "true"
                sns_client.set_endpoint_attributes(
                    EndpointArn=data[1], Attributes=params
                )
            except Exception as e:
                logger.info("Set_endpoint_attributes Failure reason. %s", e)
                logger.info("Set_endpoint_attributes Failure [id]: %s", data[0])

                # DB status update
                set_data_to_update_to_database(data=data, idx=5, value=FAILURE)
                continue

        # DB status update
        set_data_to_update_to_database(data=data, idx=5, value=SUCCESS)
        logger.info("Push notification Success [id]: %s", data[0])

        # endpoint 업데이트가 필요한 유저
        if create_needed or update_needed:
            target_user_to_update_endpoint.append(
                dict(user_id=data[3], endpoint=data[1])
            )

    try:
        # set message
        if query_result:
            msg = query_result[0][2]
            message = json.dumps(msg, ensure_ascii=False)

            # push message to topic
            topic.publish(Message=message, MessageStructure="json")

    except ClientError as e:
        logger.info("Push notification Failure [id]: %s", query_result[0][0])
        logger.info(
            "Could not push notification to platform application endpoint. %s", e
        )

        # DB status update
        set_data_to_update_to_database(data=data, idx=5, value=FAILURE)


def create_endpoint(platform_application, data: List, sns_client, topic):
    """
    CustomUserData : token과 mapping 되는 unique 값
    - 엔드포인트에 연결할 임의의 사용자 데이터. Amazon SNS는 이 데이터를 사용 안한다. 이 데이터는 UTF-8 형식이어야 하며 2KB 미만이어야 한다.
    - 만약, token이 이미 등록 되어 있고, CustomUserData가 다르면 InvalidParameter Exception 반환(already exists with the same Token, but different attributes.)
    - 값이 없으면 중복된 값이 어느 한계선까지 등록된다.
    """
    # application endpoint 등록 -> data[6] == device.uuid
    # custom_user_data_prefix -> prod: THP, dev: THD
    platform_application_endpoint = platform_application.create_platform_endpoint(
        CustomUserData=str(custom_user_data_prefix + "-" + str(data[3])),
        Token=str(data[4]),
    ).arn

    # DB endpoint update
    set_data_to_update_to_database(
        data=data, idx=1, value=platform_application_endpoint
    )

    # get SNS endpoint
    endpoint_attributes = sns_client.get_endpoint_attributes(
        EndpointArn=platform_application_endpoint
    )

    # 구독 생성
    topic.subscribe(Protocol="application", Endpoint=platform_application_endpoint)

    return endpoint_attributes


def set_data_to_update_to_database(data: List, idx: int, value: Union[str, int]):
    data[idx] = value


def update_notification_schema(query_result: List):
    logger.info("Update notification schema start")
    update_data = list()
    for data in query_result:
        update_data.append(
            (data[0], data[1].split(endpoint_prefix)[1], data[5], datetime.now())
        )
    try:
        openConnection()
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                           update notifications 
                           set endpoint=data.endpoint,status=data.status, updated_at=data.updated_at
                           from (VALUES %s) as data (id, endpoint, status, updated_at)
                           where notifications.id=data.id
                           """,
                update_data,
            )
            conn.commit()
        logger.info("Update notification schema end")
    except Exception as e:
        logger.info("Error while opening connection or processing. %s", e)


def get_push_target_user(type_: str):
    item_count = 0
    query_result = list()
    try:
        openConnection()
        if type_ == "admin":
            with conn.cursor() as cur:
                cur.execute(
                    f"select id, endpoint, data, user_id, token, status, uuid from notifications where topic='{TOPIC}' and status = '{WAIT}' and user_id in {ADMIN_USER_ID} "
                )
                for row in cur:
                    rslt = list()
                    for idx, data in enumerate(row):
                        if idx == 1:
                            # endpoint convert
                            data = endpoint_prefix + data
                        rslt.append(data)

                    query_result.append(rslt)
                    item_count += 1
        elif type_ == "all":
            with conn.cursor() as cur:
                cur.execute(
                    f"select id, endpoint, data, user_id, token, status, uuid from notifications where topic='{TOPIC}' and status = '{WAIT}'  "
                )
                for row in cur:
                    rslt = list()
                    for idx, data in enumerate(row):
                        if idx == 1:
                            # endpoint convert
                            data = endpoint_prefix + data
                        rslt.append(data)

                    query_result.append(rslt)
                    item_count += 1
    except Exception as e:
        logger.info("Error while opening connection or processing. %s", e)

    return dict(item_count=item_count, query_result=query_result)


def send_sqs_for_update_endpoint(target_user_to_update_endpoint: List):
    logger.info("Send sqs for update endpoint start")
    try:
        # 동일 유저 dict data 제거
        push_list = list(
            map(
                dict,
                collections.OrderedDict.fromkeys(
                    tuple(sorted(d.items())) for d in target_user_to_update_endpoint
                ),
            )
        )

        sqs_resource = boto3.resource("sqs")
        queue = sqs_resource.get_queue_by_name(QueueName=sqs_name)

        max_batch_size = 10  # current maximum allowed
        chunks = [
            push_list[x : x + max_batch_size]
            for x in range(0, len(push_list), max_batch_size)
        ]
        count = 0
        for chunk in chunks:
            entries = []
            for x in chunk:
                count += 1
                entry = {
                    "Id": str(count),
                    "MessageAttributes": {},
                    "MessageBody": json.dumps(x),
                }
                entries.append(entry)

            response = queue.send_messages(Entries=entries)

        if response["ResponseMetadata"]["HTTPStatusCode"] != HTTPStatus.OK:
            logger.info("SQS Fail : Send sqs for update endpoint")
            send_slack_message(
                message="Exception:Send sqs for update endpoint / [TARGET_QUEUE] USER_DATA_SYNC_TO_ENDPOINT",
                title="☠️ [FAIL] SQS Send : ",
            )
        else:
            logger.info("Send sqs for update endpoint end")
    except Exception as e:
        logger.info("Error while send sqs processing for update endpoint. %s", e)


def lambda_handler(event, context):
    type_ = event.get("type")
    if not type_:
        return "Type is None"

    start = time.time()
    result: dict = get_push_target_user(type_=type_)
    print("select_query_time :", time.time() - start)

    if len(result["query_result"]) > 0:
        target_user_to_update_endpoint = list()
        start = time.time()
        send_sns_notification(
            query_result=result["query_result"],
            target_user_to_update_endpoint=target_user_to_update_endpoint,
        )
        print("send_push_time :", time.time() - start)

        start = time.time()
        update_notification_schema(query_result=result["query_result"])
        print("update_query_time :", time.time() - start)

        send_sqs_for_update_endpoint(
            target_user_to_update_endpoint=target_user_to_update_endpoint
        )

    return "Selected %d items from RDS table" % result["item_count"]
