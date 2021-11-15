cp ./deploy/lambda_function_hawkeye_dlq.py ./lambda_function.py
zip -r ./deploy/lambda_function_hawkeye_dlq.zip ./psycopg2 ./requests ./lambda_function.py