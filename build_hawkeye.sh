cp ./deploy/lambda_function_hawkeye.py ./lambda_function.py
zip -r ./deploy/lambda_function_hawkeye.zip ./psycopg2 ./requests ./chardet ./certifi ./idna ./lambda_function.py