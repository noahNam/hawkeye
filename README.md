# What is [hawkeye](https://bitbucket.org/apartalk/hawkeye/src)?
* notification role of Apartalk
* serverless service
* version 
    * python : 3.8
## Architecture
* cloudwatch(driven crontab batch) -> Lambda -> SNS -> Push notification to user device
## Deploy
* build.sh 실행 하여 생성된 lambda.zip을 AWS <hawkeye_lambda> Service로 업로드 한다.
## A point of caution
* Lambda Service 특성상 필요 패키지는 root 경로에 둔다.
* boto3 등 AWS Lambda Service에서 기본적으로 제공해주는 패키지들은 lambda.zip에서 제거한다.(로컬 테스트용)
## Test
* Dev RDS, SNS arn setting 
* local_test.py 실행
## Todo-list
* S3 업로드를 통한 배포 자동화
* dirty package structure 정리 (Local)