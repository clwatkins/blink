from config import *

import boto3
import psycopg2
from psycopg2.extras import RealDictCursor

DYNAMO_DB = boto3.resource('dynamodb', region_name=AWS_REGION)
POSTGRES_DB = psycopg2.connect(dbname=AWS_POSTGRES_DB, user=AWS_POSTGRES_USER, password=AWS_POSTGRES_PASSWORD,
                               host=AWS_POSTGRES_URL)
POSTGRES_DB.autocommit = True
POSTGRES_DB_CURSOR = POSTGRES_DB.cursor(cursor_factory=RealDictCursor)

CLOUDSEARCH_CLIENT = boto3.client('cloudsearch')


def create_nosql_tables():
    """
    Creates necessary DynamoDB tables with appropriate key fields.
    :return: list of DynamoDB tables
    """

    current_table_list = [table.name for table in DYNAMO_DB.tables.all()]
    print(current_table_list)

    # create user table if not present
    if USER_TABLE_NAME not in current_table_list:
        user_table = DYNAMO_DB.create_table(
            TableName=USER_TABLE_NAME,
            KeySchema=[
                {
                    'AttributeName': USER_EMAIL_FIELD,
                    'KeyType': 'HASH'
                }
            ],

            AttributeDefinitions=[
                {
                    'AttributeName': USER_EMAIL_FIELD,
                    'AttributeType': 'S'
                }
            ],

            ProvisionedThroughput={
                'ReadCapacityUnits': READ_CAPACITY_UNITS,
                'WriteCapacityUnits': WRITE_CAPACITY_UNITS
            }
        )

        user_table.meta.client.get_waiter('table_exists').wait(TableName=USER_TABLE_NAME)

    # create photos table if not present
    if PHOTOS_TABLE_NAME not in current_table_list:
        photos_table = DYNAMO_DB.create_table(
            TableName=PHOTOS_TABLE_NAME,
            KeySchema=[
                {
                    'AttributeName': PHOTO_ID_FIELD,
                    'KeyType': 'HASH'
                }
            ],

            AttributeDefinitions=[
                {
                    'AttributeName': PHOTO_ID_FIELD,
                    'AttributeType': 'S'
                }
            ],

            ProvisionedThroughput={
                'ReadCapacityUnits': READ_CAPACITY_UNITS,
                'WriteCapacityUnits': WRITE_CAPACITY_UNITS
            }
        )

        photos_table.meta.client.get_waiter('table_exists').wait(TableName=PHOTOS_TABLE_NAME)

    return [table.name for table in DYNAMO_DB.tables.all()]


def create_sql_tables():
    """
    Creates necessary PostgreSQL tables and schema if not present in DB.
    :return: list of PostgreSQL tables
    """

    POSTGRES_DB_CURSOR.execute(
        f"""create table if not exists {OFFER_TABLE_NAME} ({OFFER_ID_FIELD} SERIAL PRIMARY KEY, 
        {OFFER_BRAND_FIELD} text, {OFFER_BRAND_LOGO_FIELD} text, {OFFER_DISCOUNT_FIELD} real, 
        {OFFER_POINTS_FIELD} integer, {OFFER_CODE_FIELD} text);"""
    )

    POSTGRES_DB_CURSOR.execute(
        f"""create table if not exists {REDEMPTION_TABLE_NAME} ({REDEMPTION_ID_FIELD} SERIAL PRIMARY KEY, 
        {REDEEMED_USER_FIELD} TEXT, {REDEEMED_POINTS_FIELD} INTEGER, 
        {REDEEMED_BRAND_FIELD} TEXT, {REDEEMED_DT_FIELD} bigint);"""
    )

    POSTGRES_DB_CURSOR.execute(
        f"""create table if not exists {LOCATION_INFO_TABLE_NAME} ({LOCATION_ID_FIELD} text PRIMARY KEY,
        {LOCATION_NAME_FIELD} text, {LOCATION_BRAND_FIELD} text);"""
    )

    POSTGRES_DB_CURSOR.execute(
        f"""create table if not exists {PETITION_TABLE_NAME} ({LOCATION_ID_FIELD} text PRIMARY KEY, {PETITION_COUNT_FIELD} 
        integer);"""
    )

    POSTGRES_DB_CURSOR.execute(
        f"""create table if not exists {LOGIN_HISTORY_TABLE_NAME} ({LOGIN_HISTORY_ID_FIELD} SERIAL PRIMARY KEY, 
        {LOGIN_HISTORY_DT_FIELD} bigint, {LOGIN_HISTORY_USER_FIELD} text);""")

    POSTGRES_DB_CURSOR.execute(
        f"""create table if not exists {LIKE_HISTORY_TABLE_NAME} ({LIKE_HISTORY_ID_FIELD} SERIAL PRIMARY KEY, 
        {LIKE_HISTORY_PHOTO_ID_FIELD} text, {LIKE_HISTORY_USER_ID_FIELD} text, {LIKE_TYPE_FIELD} text, 
        {LIKE_HISTORY_DT_FIELD} bigint);""")

    POSTGRES_DB_CURSOR.execute("""SELECT table_name FROM information_schema.tables WHERE table_schema='public'
       AND table_type='BASE TABLE';""")

    postgres_table_names = [t['table_name'] for t in POSTGRES_DB_CURSOR.fetchall()]

    POSTGRES_DB.close()

    return postgres_table_names


if __name__ == '__main__':
    print("Updating databases...")
    print(f"DynamoDB tables: {create_nosql_tables()}")
    print('\n')
    print(f"PostreSQL tables: {create_sql_tables()}")
