cp ./deploy/lambda_function_hawkeye_endpoint.py ./lambda_function.py
zip -r ./deploy/lambda_function_hawkeye_endpoint.zip ./certifi ./chardet ./idna ./psycopg2 ./requests ./lambda_function.py