cp ./deploy/lambda_function_hawkeye_notice.py ./lambda_function.py
zip -r ./deploy/lambda_function_hawkeye_notice.zip ./psycopg2 ./requests ./chardet ./certifi ./idna ./lambda_function.py