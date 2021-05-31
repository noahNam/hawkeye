# What is [hawkeye](https://bitbucket.org/apartalk/hawkeye/src)?
* notification role of Apartalk
* serverless service
* version 
    * python : 3.8
## Architecture
### cloudwatch(driven crontab batch) -> Lambda -> SNS -> Push notification to user device
* Cloudwatch (crontab batch event) 가 trigger
* Role of Lambda function 
    * Tanos Database의 notifications 테이블 참조하여 status wait인 메세지를 가져온다.
    * SNS Service에 유저 엔드포인트(토큰 기반)을 생성하고 push 메세지를 Topic에 Publish 한다.
    * notifications.endpoint 정보가 없을 시 생성된 endpoint를 업데이트하고, publish 성공여부를 notifications.status에 업데이트한다. 
* SNS Service가 publish된 푸쉬 메세지를 endpoint에 전송한다.
## Deploy
* build.sh 실행 하여 생성된 lambda.zip을 AWS <hawkeye_lambda> Service로 업로드 한다.
## A point of caution
* Lambda Service 특성상 필요 패키지는 root 경로에 둔다.
* boto3 등 AWS Lambda Service에서 기본적으로 제공해주는 패키지들은 lambda.zip에서 제거한다.(로컬 테스트용)
## Test
* Dev RDS, SNS arn setting 
* local_test.py 실행
## Todo-list
### 우선순위 Highest
* Device Test(아이폰 Push Test)
* fail over시 dead letter queue 관리
* 람다의 실행최대시간 Test (12 Min) 

### 우선순위 Low
* S3 업로드를 통한 배포 자동화
* dirty package structure 정리 (Local)