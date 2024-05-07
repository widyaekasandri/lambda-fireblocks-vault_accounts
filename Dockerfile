FROM public.ecr.aws/lambda/python:3.11

# Copy requirements.txt
COPY requirements.txt ${LAMBDA_TASK_ROOT}

# Copy code
COPY *.py ${LAMBDA_TASK_ROOT}

# Install the specified packages
RUN pip install -r requirements.txt


ENV DEBUG yes
ENV ENVIRONMENT test
ENV REGION_NAME ap-northeast-1
ENV API_URL https://api.fireblocks.io       
ENV S3_RESULT_BUCKET liquidityone-test-data-processed/vault-accounts
ENV DB_ATHENA test_processed
ENV TABLE_NAME test_fireblock_vault_accounts


CMD ["main.handler"]
