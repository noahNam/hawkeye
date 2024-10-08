import json
from typing import List
from datetime import datetime

import boto3
import logging

import psycopg2
from botocore.exceptions import ClientError
from psycopg2.extensions import STATUS_BEGIN
from psycopg2.extras import execute_values

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# log 출력 형식
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

# log 출력
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

host = "test-tanos-rds.cluster-cmib56uiilha.ap-northeast-2.rds.amazonaws.com"
user = "postgres"
password = "skarlgur117"
database = "tanos"
port = "5432"

# set enum
TOPIC = ["apt002", "apt003"]

# status
WAIT = 0
SUCCESS = 1
FAILURE = 2

# set sns
topic_arn = "arn:aws:sns:ap-northeast-2:208389685150:PUSH_PRIVATE"
application_arn = "arn:aws:sns:ap-northeast-2:208389685150:app/GCM/dev-hawkeye-fcm"
endpoint_prefix = (
    "arn:aws:sns:ap-northeast-2:208389685150:endpoint/GCM/dev-hawkeye-fcm/"
)

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
        logger.exception("Unexpected error: Could not connect to RDS instance. %s", e)
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
                logger.exception("Could not get endpoint attributes. %s", e)
                create_needed = True

        if create_needed:
            try:
                endpoint_attributes = create_endpoint(
                    platform_application, data, sns_client, topic
                )
            except Exception as e:
                logger.exception(
                    "already exists with the same Token, but different attributes. %s",
                    e,
                )
                # DB status update
                set_data_to_update_to_database(data, 5, FAILURE)
                continue
            except UnboundLocalError as e:
                logger.exception("topic.subscribe error. %s", e)
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

                # todo. device_tokens.endpoint update -> platform-application-sns-sqs-lambda 로 receive 처리
                # 비활성화 상태더라도 endpoint가 생성안되있을 경우 생성 후에는 Enabled 이 True로 내려오기 때문에 false 처리 필요

            except Exception as e:
                logger.exception("Set_endpoint_attributes Failure reason. %s", e)
                logger.exception("Set_endpoint_attributes Failure [id]: %s", data[0])

                # DB status update
                set_data_to_update_to_database(data, 5, FAILURE)
                continue

        try:
            # set message
            message = json.dumps(data[2], ensure_ascii=False)
            # push message to topic

            # message_attributes 의 경우음 SNS 주제에 구독한 SQS에서 확인이 안되는 문제가 있음
            # message_attributes = {
            #     "Type": {
            #       "DataType": "String",
            #       "StringValue": "Orchestration.Services.Model.Pollution.PollutionMessage"
            #     }
            # }
            # topic.publish(Message=message, MessageStructure='json', MessageAttributes=message_attributes)

            topic.publish(Message=message, MessageStructure="json")
            # DB status update
            set_data_to_update_to_database(data, 5, SUCCESS)
            logger.info("Push notification Success [id]: %s", data[0])
        except ClientError as e:
            logger.exception("Push notification Failure [id]: %s", data[0])
            logger.exception(
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
    # application endpoint 등록
    platform_application_endpoint = platform_application.create_platform_endpoint(
        CustomUserData=str(data[3]), Token=str(data[4])
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


def set_data_to_update_to_database(data: List, idx: int, value: str):
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
        logger.exception("Error while opening connection or processing. %s", e)
    finally:
        logger.info("Closing Connection")
        if conn is not None and conn.status == STATUS_BEGIN:
            conn.close()


def get_push_target_user():
    item_count = 0
    query_result = list()
    try:
        openConnection()
        with conn.cursor() as cur:
            cur.execute(
                f"select id, endpoint, data, user_id, token, status from notifications where topic='{TOPIC[0]}' and status = '{WAIT}' "
                f"union select id, endpoint, data, user_id, token, status from notifications where topic='{TOPIC[1]}' and status = '{WAIT}'  "
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
        logger.exception("Error while opening connection or processing. %s", e)
    finally:
        logger.info("Closing Connection")
        if conn is not None and conn.status == STATUS_BEGIN:
            conn.close()

    return dict(item_count=item_count, query_result=query_result)


def delete_application_endpoint(query_result: List):
    # set boto3
    sns_client = boto3.client("sns")

    for data in query_result:
        logger.info("Delete endpoint: ", str(data[1]))
        sns_client.delete_endpoint(EndpointArn=data[1])


def run():
    result: dict = get_push_target_user()

    # delete endpoint
    # delete_application_endpoint(result['query_result'])
    # return

    if len(result["query_result"]) > 0:
        send_sns_notification(result["query_result"])
        update_notification_schema(result["query_result"])

    return "Selected %d items from RDS table" % result["item_count"]


if __name__ == "__main__":
    run()
