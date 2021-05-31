import json
from typing import List
from datetime import datetime

import psycopg2
import boto3
import logging

from botocore.exceptions import ClientError
from psycopg2.extensions import STATUS_BEGIN
from psycopg2.extras import execute_values

logger = logging.getLogger()
logger.setLevel(logging.INFO)

host = "localhost"
user = "tanos"
password = "!Dkvkxhr117"
database = "tanos"
port = "5432"

# set enum
CATEGORY = "apt01"
STATUS = "wait"
SUCCESS = "success"
FAILURE = "failure"

# set sns
topic_arn = "arn:aws:sns:ap-northeast-2:208389685150:test-notification-topic"
application_arn = "arn:aws:sns:ap-northeast-2:208389685150:app/GCM/test-app"

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


def send_sns_notification(query_result: List):
    """
        Reference structure
        Topic subscription –> Platform application ARN –> User Device Token
    """
    # set boto3
    sns_client = boto3.client("sns")
    sns_resource = boto3.resource('sns')

    # set platform application
    platform_application = sns_resource.PlatformApplication(application_arn)
    # set topic
    topic = sns_resource.Topic(topic_arn)

    for data in query_result:
        is_endpoint_available = True
        try:
            # get SNS endpoint
            endpoint_attributes = sns_client.get_endpoint_attributes(EndpointArn=data[1])

        except Exception as e:
            logger.exception("Could not get endpoint attributes. %s", e)
            is_endpoint_available = False

        if is_endpoint_available is False:
            try:
                """
                    CustomUserData : token과 mapping 되는 unique 값
                    - 엔드포인트에 연결할 임의의 사용자 데이터. Amazon SNS는 이 데이터를 사용 안한다. 이 데이터는 UTF-8 형식이어야 하며 2KB 미만이어야 한다.
                    - 만약, token이 이미 등록 되어 있고, CustomUserData가 다르면 InvalidParameter Exception 반환(already exists with the same Token, but different attributes.)
                """
                # application endpoint 등록
                platform_application_endpoint = platform_application.create_platform_endpoint(
                    CustomUserData=str(data[3]),
                    Token=str(data[4])
                ).arn

                # get SNS endpoint
                endpoint_attributes = sns_client.get_endpoint_attributes(EndpointArn=platform_application_endpoint)

                # 구독 생성
                topic.subscribe(Protocol='application', Endpoint=platform_application_endpoint)

                # DB endpoint update
                set_data_to_update_to_database(data, 1, platform_application_endpoint)
            except Exception as e:
                logger.exception("already exists with the same Token, but different attributes. %s", e)
                continue
            except UnboundLocalError as e:
                logger.exception("topic.subscribe error. %s", e)
                continue

        try:
            # 활성화 여부 체크
            if endpoint_attributes['Attributes']['Enabled'] == 'false':
                set_data_to_update_to_database(data, 5, FAILURE)
                logger.info("endpoint_attributes['Attributes']['Enabled'] is False %s", data[0])
                continue

            # set message
            message = json.dumps(data[2], ensure_ascii=False)
            # push message to topic
            topic.publish(Message=message, MessageStructure='json')
            # DB status update
            set_data_to_update_to_database(data, 5, SUCCESS)
            logger.info("Push notification Success [id]: %s", data[3])
        except ClientError as e:
            logger.exception("Push notification Failure [id]: %s", data[0])
            logger.exception("Could not push notification to platform application endpoint. %s", e)

            # DB status update
            set_data_to_update_to_database(data, 5, FAILURE)


def set_data_to_update_to_database(data: List, idx: int, value: str):
    data[idx] = value


def update_notification_schema(query_result: List):
    logger.info("Update notification schema start")
    update_data = list()
    for data in query_result:
        update_data.append((data[0], data[1], data[5], datetime.now()))
    try:
        openConnection()
        with conn.cursor() as cur:
            execute_values(cur,
                           """
                           update notifications 
                           set endpoint=data.endpoint,status=data.status, updated_at=data.updated_at
                           from (VALUES %s) as data (id, endpoint, status, updated_at)
                           where notifications.id=data.id
                           """, update_data)
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
                f"select id, endpoint, data, user_id, token, status from notifications where category='{CATEGORY}' and status = '{STATUS}' ")
            for row in cur:
                rslt = list()
                for data in row:
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


def run():
    result: dict = get_push_target_user()
    if len(result['query_result']) > 0:
        send_sns_notification(result['query_result'])
        update_notification_schema(result['query_result'])

    return "Selected %d items from RDS table" % result['item_count']


if __name__ == "__main__":
    run()
