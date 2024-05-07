import json
import logging
import pandas as pd
import awswrangler as wr
import cryptography.hazmat.primitives.serialization as serialization
from fireblocks_sdk import FireblocksSDK, PagedVaultAccountsRequestFilters
import base64
import boto3
import os

log = logging.getLogger()

def getSecret(secretId: str, key: str):
    secmanager = boto3.client('secretsmanager')
    secret = secmanager.get_secret_value(
        SecretId=secretId)
    return json.loads(secret['SecretString'])[key]

def json_extract(obj, key):
    """Recursively fetch values from nested JSON."""
    arr = []

    def extract(obj, arr, key):
        """Recursively search for values of key in JSON tree."""
        if isinstance(obj, dict):
            for k, v in obj.items():
                if isinstance(v, (dict, list)):
                    extract(v, arr, key)
                elif k == key:
                    arr.append(v)
        elif isinstance(obj, list):
            for item in obj:
                extract(item, arr, key)
        return arr

    values = extract(obj, arr, key)

    if values:
        return values[0]
    else:
        return ""

def process_vault_accounts(df):
  # Create an empty list to store the processed data
  processed_data = []

  # Loop through each row in the DataFrame
  for index, row in df.iterrows():
    # Extract parent data (id, name, customer_ref_id)
    parent_data = {
      "id": row["id"],
      "name": row["name"]
    }

    # Loop through each item in the "assets" list
    for asset in row["assets"]:
      # Extract asset-specific data using json_extract
      asset_data = {
        "vault_account_id": json_extract(asset, "id"),
        "vault_account_balance": json_extract(asset, "balance"),
        "vault_account_total": json_extract(asset, "total"),
        "vault_account_available": json_extract(asset, "available"),
        "vault_account_locked_amount": json_extract(asset, "lockedAmount"),
        "vault_account_pending": json_extract(asset, "pending"),
        "vault_account_frozen": json_extract(asset, "frozen"),
        "vault_account_staked": json_extract(asset, "staked"),
        "vault_account_block_hash": json_extract(asset, "blockHash"),
      }

      # Combine parent and asset data into a single dictionary
      combined_data = {**parent_data, **asset_data}

      # Append the combined data to the processed_data list
      processed_data.append(combined_data)

  # Create a new DataFrame from the processed data
  return pd.DataFrame(processed_data)


def process_json_column(row, json_column):
    new_rows = []
    for item in row[json_column]:
        new_row = {key: row[key] for key in row.index if key != json_column}
        new_row[json_column] = item
        new_rows.append(new_row)
    return new_rows


# Define a function to process a list of JSON objects and create new rows
def process_custom_json_column(row, json_column):
    new_rows = []
    for obj in row[json_column]:
        new_row = {key: row[key] for key in row.index if key != json_column}
        for attr_name, attr_value in obj.items():
            if attr_name == 'name':
                # Add a prefix to the 'name' attribute inside the custom JSON
                new_row[f"{json_column}_{attr_name}"] = attr_value
            if attr_name == 'type':
                # Add a prefix to the 'type' attribute inside the custom JSON
                new_row[f"{json_column}_{attr_name}"] = attr_value
            else:
                if attr_name == 'name':
                    continue
                new_row[attr_name] = attr_value
        new_rows.append(new_row)
    return new_rows

def handler(event, context):

    # define env variables
    debug = os.environ['DEBUG']
    environment = os.environ['ENVIRONMENT']
    api_url = os.environ['API_URL']
    region_name = os.environ['REGION_NAME']
    s3_results_bucket = os.environ['S3_RESULT_BUCKET']
    db_athena = os.environ['DB_ATHENA']
    table_name = os.environ['TABLE_NAME']
    private_key_base64 = getSecret(f"/lambdas/{environment}/fireblocks", 'fireblocks.privateKey')
    api_key = getSecret(f"/lambdas/{environment}/fireblocks", 'fireblocks.apiKey')
    password = getSecret(f"/lambdas/{environment}/fireblocks", 'fireblocks.password')
    errors = []
    if debug:
        log.setLevel(logging.DEBUG)

    log.debug(f"event: {event}")
    log.debug(f"context: {context}")

    session = boto3.Session(region_name=region_name)

    # API Authorization
    try:
        password_bytes = bytes(password, "utf-8")
        encrypted_key_base64 = base64.b64decode(private_key_base64)
        private_key = serialization.load_pem_private_key(
            encrypted_key_base64, password=password_bytes)

        fireblocks = FireblocksSDK(private_key, api_key, api_base_url=api_url)

        print("API Authorization success")
        next_page = True
        filters = PagedVaultAccountsRequestFilters
        filters.limit = 200
        filters.min_amount_threshold = 0.000000000000001
        filters.order_by = "ASC"
        filters.name_prefix = None
        filters.name_suffix = None
        filters.asset_id = None
        filters.before = None
        filters.after = None

        while next_page:
            vaults = fireblocks.get_vault_accounts_with_page_info(filters)
            paging = vaults['paging']
            accounts = vaults['accounts']

            print("Paging information:")
            print(paging)
            print("Vault Accounts:")

            # Verify if there is an additional page of vaults
            if 'after' not in paging:
                # Update pointer to next page
                # paged_filter.after = paging['after']
                next_page = False

    except Exception as ex:
        error = f"API Authorization error, ex:{ex}"
        log.error(error)
        errors.append(error)

    va = json.dumps(accounts)
    data = json.loads(va)

    df = pd.DataFrame(data)
    # Create a new DataFrame from the list of dictionaries
    va_df = process_vault_accounts(df)
    print(va_df)
    try:
        # write data lp assets to Athena table
        wr.s3.to_parquet(
            df=va_df,
            path=s3_results_bucket,
            index=False,
            compression="snappy",
            sanitize_columns=False,
            dataset=True,
            mode="overwrite",
            catalog_versioning=False,
            schema_evolution=True,
            database=db_athena,
            table=table_name,
            boto3_session=session,
        )
        print("Done writing Fireblocks Vault Accounts to S3")

    except Exception as ex:
        error = f"error while writing to S3, ex:{ex}"
        print(ex)
        log.error(error)
        errors.append(error)

    if errors:
        return {
            'statusCode': 500,
            'body': f"errors: {errors}"
        }
    return {
        'statusCode': 200,
        'body': "ok"
    }
