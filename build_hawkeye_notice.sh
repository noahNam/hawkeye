cp ./deploy/lambda_function_hawkeye_notice.py ./lambda_function.py
zip -r ./deploy/lambda_function_hawkeye_notice.zip ./requests ./psycopg2 ./lambda_function.py