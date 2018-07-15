from config import *

import json
import datetime as dt

import boto3
from boto3.dynamodb.types import Decimal

CLOUDSEARCH_DOMAIN = boto3.client('cloudsearchdomain', endpoint_url=CLOUD_SEARCH_DOMAIN_URL)
DYNAMO_DB = boto3.resource('dynamodb', region_name=AWS_REGION)
PHOTOS_TABLE = DYNAMO_DB.Table(PHOTOS_TABLE_NAME)


def delete_cloudsearch_recs():
    """
    Deletes all current records in the CloudSearch domain.
    :return: None
    """

    while True:
        # iterate through records in blocks of 10,000 until no records exist, triggering KeyError
        try:
            all_hits = CLOUDSEARCH_DOMAIN.search(
                queryParser="structured",
                query="matchall",
                returnFields="photo_id",
                size=9999
            )

            current_records = [_['fields']['photo_id'][0] for _ in all_hits['hits']['hit']]

            docs_to_upload = []

            for rec in current_records:
                new_doc = dict()
                new_doc['id'] = rec
                new_doc['type'] = 'delete'
                docs_to_upload.append(new_doc)

            cloudsearch_upload_response = CLOUDSEARCH_DOMAIN.upload_documents(
                documents=json.dumps(docs_to_upload).encode(),
                contentType='application/json'
            )

            print(cloudsearch_upload_response)

        except KeyError:
            print("All records deleted")
            return


def upload_all_photos_to_cloudsearch():
    """
    Upload all current photos in DynamoDB table to CloudSearch domain.
    :return: None
    """

    all_photos = PHOTOS_TABLE.scan()['Items']

    docs_to_upload = []

    for photo in all_photos:
        new_doc = dict()
        new_doc['id'] = photo[PHOTO_ID_FIELD]
        new_doc['type'] = 'add'

        new_doc['fields'] = dict()

        for field in [PHOTO_DT_FIELD, PHOTO_TAGS_FIELD, PHOTO_ID_FIELD, PHOTO_LATLON_FIELD, PHOTO_LIKES_FIELD,
                      PHOTO_OWNER_FIELD, PHOTO_LOCATION_ID_FIELD]:

            if field == PHOTO_DT_FIELD:
                # wrangle into cloudsearch-compliant dt format
                raw_dt_format = photo[field]
                photo_dt = dt.datetime.strptime(raw_dt_format, DT_FORMAT)
                new_doc['fields'][field] = photo_dt.isoformat("T") + "Z"
            else:
                # handle DynamoDB data types for JSON-encoding and CloudSearch upload
                if isinstance(photo[field], set):
                    new_doc['fields'][field] = list(photo[field])
                elif isinstance(photo[field], Decimal):
                    new_doc['fields'][field] = int(photo[field])
                else:
                    new_doc['fields'][field] = photo[field]

        docs_to_upload.append(new_doc)

    cloudsearch_upload_response = CLOUDSEARCH_DOMAIN.upload_documents(
                documents=json.dumps(docs_to_upload).encode(),
                contentType='application/json'
            )

    print(cloudsearch_upload_response)


def remove_orphaned_s3_files():
    # get all files from S3 bucket
    # get list of ids from photos table
    # delete any photo not found in photos table
    pass