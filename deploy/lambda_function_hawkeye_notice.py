import json
import time
from typing import List
from datetime import datetime

import psycopg2
import os
import boto3
import logging

from botocore.exceptions import ClientError
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

# admin_user_id
ADMIN_USER_ID = os.environ.get("ADMIN_USER_ID")

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


def send_sns_notification(query_result: List):
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
                    "already exists with the same Token, but different attributes. %s",
                    e,
                )
                # DB status update
                set_data_to_update_to_database(data, 5, FAILURE)
                continue
            except UnboundLocalError as e:
                logger.info("topic.subscribe error. %s", e)
                # DB status update
                set_data_to_update_to_database(data, 5, FAILURE)
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
                set_data_to_update_to_database(data, 5, FAILURE)
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
                set_data_to_update_to_database(data, 5, FAILURE)
                continue

        try:
            # set message
            message = json.dumps(data[2], ensure_ascii=False)
            # push message to topic
            topic.publish(Message=message, MessageStructure="json")
            # DB status update
            set_data_to_update_to_database(data, 5, SUCCESS)
            logger.info("Push notification Success [id]: %s", data[0])
        except ClientError as e:
            logger.info("Push notification Failure [id]: %s", data[0])
            logger.info(
                "Could not push notification to platform application endpoint. %s", e
            )

            # DB status update
            set_data_to_update_to_database(data, 5, FAILURE)


def create_endpoint(platform_application, data: List, sns_client, topic):
    """
    CustomUserData : token과 mapping 되는 unique 값
    - 엔드포인트에 연결할 임의의 사용자 데이터. Amazon SNS는 이 데이터를 사용 안한다. 이 데이터는 UTF-8 형식이어야 하며 2KB 미만이어야 한다.
    - 만약, token이 이미 등록 되어 있고, CustomUserData가 다르면 InvalidParameter Exception 반환(already exists with the same Token, but different attributes.)
    - 값이 없으면 중복된 값이 어느 한계선까지 등록된다.
    """
    # application endpoint 등록 -> data[6] == device.uuid
    platform_application_endpoint = platform_application.create_platform_endpoint(
        CustomUserData=str(data[6]), Token=str(data[4])
    ).arn

    # DB endpoint update
    set_data_to_update_to_database(data, 1, platform_application_endpoint)

    # get SNS endpoint
    endpoint_attributes = sns_client.get_endpoint_attributes(
        EndpointArn=platform_application_endpoint
    )

    # 구독 생성
    topic.subscribe(Protocol="application", Endpoint=platform_application_endpoint)

    return endpoint_attributes


def set_data_to_update_to_database(data: List, idx: int, value: int):
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
    finally:
        logger.info("Closing Connection")
        if conn and conn.status == STATUS_BEGIN:
            conn.close()


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
    finally:
        logger.info("Closing Connection")
        if conn and conn.status == STATUS_BEGIN:
            conn.close()

    return dict(item_count=item_count, query_result=query_result)


def lambda_handler(event, context):
    type_ = event.get("type")
    if not type_:
        return "Type is None"

    start = time.time()
    result: dict = get_push_target_user(type_=type_)
    print("select_query_time :", time.time() - start)

    if len(result["query_result"]) > 0:
        start = time.time()
        send_sns_notification(result["query_result"])
        print("send_push_time :", time.time() - start)

        start = time.time()
        update_notification_schema(result["query_result"])
        print("update_query_time :", time.time() - start)

    return "Selected %d items from RDS table" % result["item_count"]
