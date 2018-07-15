from config import *

import json
import datetime as dt

import boto3

CLOUDSEARCH_DOMAIN = boto3.client('cloudsearchdomain', endpoint_url=CLOUD_SEARCH_DOMAIN_URL)


def update_cloudsearch(event=None, context=None):
    """
    Handles DynamoDB Stream events, automatically uploading new photo records to CloudSearch.
    :param event: JSON-encoded API Gateway pass-through.
    :param context: JSON-encoded API Gateway pass-through. Not used.
    :return: Response from CloudSearch server.
    """

    docs_to_upload = []

    # properly format each document in the event record for CloudSearch upload.
    for record in event['Records']:
        new_doc = dict()

        try:
            new_doc['id'] = record['dynamodb']['Keys']['photo_id']['S']
        except KeyError:
            print(f"Error processing following record: \n {str(record)}")
            continue

        # handle photo delete events
        if record['eventName'] == 'REMOVE':
            new_doc['type'] = 'delete'
            docs_to_upload.append(new_doc)
            continue

        new_doc['type'] = 'add'

        try:
            new_doc['fields'] = dict()

            for field in [PHOTO_DT_FIELD, PHOTO_TAGS_FIELD, PHOTO_ID_FIELD, PHOTO_LATLON_FIELD, PHOTO_LIKES_FIELD,
                          PHOTO_OWNER_FIELD, PHOTO_LOCATION_ID_FIELD]:
                for k in record['dynamodb']['NewImage'][field].keys():
                    if field == PHOTO_DT_FIELD:
                        # wrangle into cloudsearch-compliant dt format
                        raw_dt_format = record['dynamodb']['NewImage'][field][k]
                        photo_dt = dt.datetime.strptime(raw_dt_format, DT_FORMAT)
                        new_doc['fields'][field] = photo_dt.isoformat("T")+"Z"
                    else:
                        new_doc['fields'][field] = record['dynamodb']['NewImage'][field][k]
        except KeyError as e:
            print(e)
            continue

        docs_to_upload.append(new_doc)

    try:
        cloudsearch_upload_response = CLOUDSEARCH_DOMAIN.upload_documents(
            documents=json.dumps(docs_to_upload).encode(),
            contentType='application/json'
        )

    except Exception as err:
        return {'error': err, 'data': docs_to_upload}

    return cloudsearch_upload_response
