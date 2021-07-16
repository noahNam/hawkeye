cp ./deploy/lambda_function_hawkeye.py ./lambda_function.py
zip -r lambda.zip ./psycopg2 ./lambda_function.py