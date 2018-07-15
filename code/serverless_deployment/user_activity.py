from config import *
from api_management import validate_params, auth_session, APIResponseForbidden, APIResponseSuccess, APIResponse, APIResponseError

import json
import io
import datetime as dt
import base64
import re


import boto3
from botocore.exceptions import ClientError
import psycopg2
from psycopg2.extras import RealDictCursor

# build AWS service and tables handlers
DYNAMO_DB = boto3.resource('dynamodb', region_name=AWS_REGION)
S3_STORAGE = boto3.client('s3')
REKO_CLIENT = boto3.client('rekognition')

USER_TABLE = DYNAMO_DB.Table(USER_TABLE_NAME)
PHOTOS_TABLE = DYNAMO_DB.Table(PHOTOS_TABLE_NAME)


@validate_params(API_USER_EMAIL, API_USER_SESSION_KEY, API_PHOTO_ID)
@auth_session()
def user_like_photo(event_body, context):
    f"""
    Handles user photo likes, updating photo like count, user record of liked photos, and logging of like event.
    :param event: (Auto) JSON-encoded API Gateway pass-through.
    :param {API_USER_EMAIL}: (Req) User identifier
    :param {API_USER_SESSION_KEY}: (Req) Temporary user-specific session key for authentication
    :param {API_PHOTO_ID}: (Req) ID of photo to be liked
    :param context: (Auto) JSON-encoded API Gateway pass-through. Not used.
    :return: number of likes under photo ID key if successful
    """

    # get photo information from database, throwing error if the item doesn't exist
    try:
        photo_info = PHOTOS_TABLE.get_item(Key={PHOTO_ID_FIELD: event_body[API_PHOTO_ID]})['Item']
    except KeyError:
        return APIResponseError(message=f"PHOTO_DOESNT_EXIST", status_code=404)

    # check attempted like isn't from photo owner
    if photo_info[PHOTO_OWNER_FIELD] == event_body[API_USER_EMAIL]:
        return APIResponseForbidden(message="USER_CANNOT_LIKE_OWN").send()

    # check user hasn't already liked photo
    current_user_data = USER_TABLE.get_item(Key={API_USER_EMAIL: event_body[API_USER_EMAIL]})
    try:
        if event_body[API_PHOTO_ID] in current_user_data['Item'][USER_PHOTO_LIKES_FIELD]:
            return APIResponseForbidden(message="LIKED_PHOTO_ALREADY", status_code=409).send()
    # catch KeyError (could occur if user has no liked photos, as is not a required field on user account creation)
    except KeyError:
        pass

    # increment photo like count -- this is set to 0 on photo upload
    like_count = photo_info[PHOTO_LIKES_FIELD] + 1

    # update likes on photo in photo table
    PHOTOS_TABLE.update_item(
        Key={PHOTO_ID_FIELD: event_body[API_PHOTO_ID]},
        UpdateExpression=f'SET {PHOTO_LIKES_FIELD} = :new_like_count',
        ExpressionAttributeValues={
            ':new_like_count': like_count
        })

    # save like to user in user table
    try:
        user_photo_likes_list = current_user_data['Item'][USER_PHOTO_LIKES_FIELD]
    except KeyError:
        user_photo_likes_list = []

    user_photo_likes_list.append(event_body[API_PHOTO_ID])

    USER_TABLE.update_item(
        Key={
            API_USER_EMAIL: event_body[API_USER_EMAIL]
        },
        UpdateExpression=f'SET {USER_PHOTO_LIKES_FIELD} = :user_photo_likes_list',
        ExpressionAttributeValues={
            ':user_photo_likes_list': user_photo_likes_list
        })

    # save like activity to history table
    postgres_db = psycopg2.connect(dbname=AWS_POSTGRES_DB, user=AWS_POSTGRES_USER, password=AWS_POSTGRES_PASSWORD,
                                   host=AWS_POSTGRES_URL)
    postgres_db.autocommit = True
    postgres_db_cursor = postgres_db.cursor(cursor_factory=RealDictCursor)

    postgres_db_cursor.execute(f"""
    INSERT INTO {LIKE_HISTORY_TABLE_NAME} ({LIKE_HISTORY_PHOTO_ID_FIELD}, {LIKE_HISTORY_USER_ID_FIELD}, 
    {LIKE_TYPE_FIELD}, {LIKE_HISTORY_DT_FIELD}) VALUES (%(photo)s, %(user)s, %(type)s, %(dt)s);""",
                               {'photo': event_body[API_PHOTO_ID], 'user': event_body[API_USER_EMAIL], 'type': 'like',
                                'dt': int(dt.datetime.strftime(dt.datetime.utcnow(), DT_FORMAT))})

    postgres_db_cursor.close()
    postgres_db.close()

    return APIResponseSuccess(response_content={event_body[API_PHOTO_ID]: like_count}).send()


@validate_params(API_USER_EMAIL, API_USER_SESSION_KEY, API_PHOTO_ID)
@auth_session()
def user_unlike_photo(event_body, context):
    f"""
    Handles user unliking of photo, updating photo like count, user record of liked photos, and logging of unlike event.
    :param event: (Auto) JSON-encoded API Gateway pass-through.
    :param {API_USER_EMAIL}: (Req) User identifier
    :param {API_USER_SESSION_KEY}: (Req) Temporary user-specific session key for authentication
    :param {API_PHOTO_ID}: (Req) ID of photo to be liked
    :param context: (Auto) JSON-encoded API Gateway pass-through. Not used.
    :return: number of likes under photo ID key if successful
    """

    # check that user has liked photo
    current_user_data = USER_TABLE.get_item(Key={API_USER_EMAIL: event_body[API_USER_EMAIL]})

    try:
        user_photo_likes_list = current_user_data['Item'][USER_PHOTO_LIKES_FIELD]
        user_photo_likes_list.remove(event_body[API_PHOTO_ID])
    # KeyError or ValueError raised in case user doesn't have any liked photos or photo_id not in list
    except (KeyError, ValueError):
        return APIResponse(status_code=409, message="USER_HASNT_LIKED").send()

    # get photo info from database
    photo_info = PHOTOS_TABLE.get_item(Key={PHOTO_ID_FIELD: event_body[API_PHOTO_ID]})

    # decrement photo like count, catching case where likes are already at 0 (shouldn't be possible given above test)
    if photo_info['Item'][PHOTO_LIKES_FIELD] <= 0:
        return APIResponseForbidden(message="LIKE_COUNT_0").send()

    like_count = photo_info['Item'][PHOTO_LIKES_FIELD] - 1

    # update likes on photo in photo table
    PHOTOS_TABLE.update_item(
        Key={PHOTO_ID_FIELD: event_body[API_PHOTO_ID]},
        UpdateExpression='SET {pl_field} = :new_like_count'.format(pl_field=PHOTO_LIKES_FIELD),
        ExpressionAttributeValues={
            ':new_like_count': like_count
        })

    # update user record to remove photo like
    USER_TABLE.update_item(
        Key={
            API_USER_EMAIL: event_body[API_USER_EMAIL]
        },
        UpdateExpression='SET {upl_field} = :user_photo_likes_list'.format(upl_field=USER_PHOTO_LIKES_FIELD),
        ExpressionAttributeValues={
            ':user_photo_likes_list': user_photo_likes_list
        })

    # save unlike activity to like history table
    postgres_db = psycopg2.connect(dbname=AWS_POSTGRES_DB, user=AWS_POSTGRES_USER, password=AWS_POSTGRES_PASSWORD,
                                   host=AWS_POSTGRES_URL)
    postgres_db.autocommit = True
    postgres_db_cursor = postgres_db.cursor(cursor_factory=RealDictCursor)

    postgres_db_cursor.execute(f"""
        INSERT INTO {LIKE_HISTORY_TABLE_NAME} ({LIKE_HISTORY_PHOTO_ID_FIELD}, {LIKE_HISTORY_USER_ID_FIELD}, 
        {LIKE_TYPE_FIELD}, {LIKE_HISTORY_DT_FIELD}) VALUES (%(photo)s, %(user)s, %(type)s, %(dt)s);""",
                               {'photo': event_body[API_PHOTO_ID], 'user': event_body[API_USER_EMAIL], 'type': 'unlike',
                                'dt': int(dt.datetime.strftime(dt.datetime.utcnow(), DT_FORMAT))})

    postgres_db_cursor.close()
    postgres_db.close()

    return APIResponseSuccess(response_content={event_body[API_PHOTO_ID]: like_count}).send()


@validate_params(API_USER_EMAIL, API_USER_SESSION_KEY, API_PHOTO_DATE, API_PHOTO_DATA, API_PHOTO_LAT, API_PHOTO_LON,
                 API_LOCATION_ID)
@auth_session()
def user_upload_photo(event_body, context):
    f"""
    Handles user photo upload, saving file to S3, adding record to photo and user databases (and triggering update
    of CloudSearch index to make photo available for viewing).
    :param event: (Auto) JSON-encoded API Gateway pass-through.
    :param {API_USER_EMAIL}: (Req) User identifier
    :param {API_USER_SESSION_KEY}: (Req) Temporary user-specific session key for authentication
    :param {API_PHOTO_DATE}: (Req) photo capture timestamp, formatted as an integer (e.g. 20180131235959)
    :param {API_PHOTO_DATA}: (Req) Base64-encoded string of photo
    :param {API_PHOTO_LAT}: (Req) Latitude of photo capture location
    :param {API_PHOTO_LON}: (Req) Longitude of photo capture location
    :param {API_LOCATION_ID}: (Req) Google Location ID indicating where the photo was taken.
    :param {API_PHOTO_COMMENT}: (Optional) User-provided comments to accompany photo.
    :param context: (Auto) JSON-encoded API Gateway pass-through. Not used.
    :return: photo ID if successful (usable as pointer to S3 image location for display)
    """

    current_user_data = USER_TABLE.get_item(Key={API_USER_EMAIL: event_body[API_USER_EMAIL]})

    # generate photo filename (unique user id + photo timestamp)
    photo_filename = current_user_data['Item'][USER_ID_FIELD] + '_' + str(event_body[API_PHOTO_DATE]) + '.jpg'

    try:
        # check that photo doesn't look like a duplicate based on user ID and photo timestamp
        photo_info = PHOTOS_TABLE.get_item(Key={PHOTO_ID_FIELD: photo_filename})['Item']
    except KeyError:
        pass
    else:
        return APIResponse(status_code=409, message="Duplicate photo detected.").send()

    # decode from base64 and upload photo to s3 bucket, make publicly available
    photo_data_decoded = base64.b64decode(event_body[API_PHOTO_DATA])
    S3_STORAGE.upload_fileobj(Fileobj=io.BytesIO(photo_data_decoded), Bucket=S3_IMAGE_UPLOAD_BUCKET, Key=photo_filename,
                              ExtraArgs={'ACL': 'public-read'})

    photo_info_upload = dict()
    photo_info_upload[PHOTO_ID_FIELD] = photo_filename
    photo_info_upload[PHOTO_OWNER_FIELD] = event_body[API_USER_EMAIL]
    photo_info_upload[PHOTO_LIKES_FIELD] = 0
    photo_info_upload[PHOTO_DT_FIELD] = int(event_body[API_PHOTO_DATE])
    photo_info_upload[PHOTO_LATLON_FIELD] = f"{str(event_body[API_PHOTO_LAT])}, {str(event_body[API_PHOTO_LON])}"
    photo_info_upload[PHOTO_LOCATION_ID_FIELD] = event_body[API_LOCATION_ID]
    photo_info_upload[PHOTO_VIEWS_FIELD] = 0

    # load optional parameters once session authentication confirmed (exit function earlier if not)
    try:
        if event_body[PHOTO_COMMENTS_FIELD] != "":
            photo_info_upload[PHOTO_COMMENTS_FIELD] = str(event_body[PHOTO_COMMENTS_FIELD])
    except (KeyError, IndexError, json.JSONDecodeError):
        pass  # don't add a photo comments field

    # get list of photo tags from AWS Rekognition
    try:
        image_tag_response_raw = REKO_CLIENT.detect_labels(Image={"S3Object": {
            "Bucket": S3_IMAGE_UPLOAD_BUCKET,
            "Name": photo_filename}})

        # build list of labels from Rekognition response
        # Add tags as a set so that CloudSearch can search field as text-array
        photo_info_upload[PHOTO_TAGS_FIELD] = {re.sub("[^a-z]", "", label['Name'].lower()) for
                                               label in image_tag_response_raw['Labels']}

    except ClientError as e:
        print("Couldn't get image tags")
        print(e)
        photo_info_upload[PHOTO_TAGS_FIELD] = {"None"}

    try:
    # add photo details to photos table
        PHOTOS_TABLE.put_item(
            Item=photo_info_upload
        )
    except Exception as e:
        print(e)
        print(photo_info_upload)
        return APIResponseError(message="Error uploading the attached photo to DynamoDB",
                                response_content=photo_info_upload).send()

    # add image filename to list of user photos
    try:
        user_photos_list = current_user_data['Item'][USER_PHOTOS_FIELD]
    except KeyError:
        user_photos_list = []

    user_photos_list.append(photo_filename)

    # update user profile with record of photo
    USER_TABLE.update_item(
        Key={
            API_USER_EMAIL: event_body[API_USER_EMAIL]
        },
        UpdateExpression='SET {up_field} = :user_photos_list'.format(up_field=USER_PHOTOS_FIELD),
        ExpressionAttributeValues={
            ':user_photos_list': user_photos_list
        }
    )

    return APIResponseSuccess(status_code=201, response_content={"photo_id": photo_filename}).send()


@validate_params(API_USER_EMAIL, API_USER_SESSION_KEY, API_PHOTO_ID)
@auth_session()
def user_delete_photo(event_body, context):
    f"""
    Handles user deletion of photo, removing from S3, user and photos databases.
    :param event: (Auto) JSON-encoded API Gateway pass-through.
    :param {API_USER_EMAIL}: (Req) User identifier
    :param {API_USER_SESSION_KEY}: (Req) Temporary user-specific session key for authentication
    :param {API_PHOTO_ID}: (Req) ID of photo to be deleted
    :param context: (Auto) JSON-encoded API Gateway pass-through. Not used.
    :return: "Photo deleted" message if successful
    """

    # get list of current user photos -- run this first to check that user originally uploaded the photo to be deleted
    current_user_data = USER_TABLE.get_item(Key={API_USER_EMAIL: event_body[API_USER_EMAIL]})

    try:
        user_photos_list = current_user_data['Item'][USER_PHOTOS_FIELD]
        # remove image filename from list of user photos
        user_photos_list.remove(event_body[API_PHOTO_ID])
    except (KeyError, ValueError):
        return APIResponseForbidden(message="User has not posted this image.").send()

    USER_TABLE.update_item(
        Key={
            API_USER_EMAIL: event_body[API_USER_EMAIL]
        },
        UpdateExpression=f'SET {USER_PHOTOS_FIELD} = :user_photos_list',
        ExpressionAttributeValues={
            ':user_photos_list': user_photos_list
        })

    # remove image from photos table
    PHOTOS_TABLE.delete_item(
        Key={PHOTO_ID_FIELD: event_body[API_PHOTO_ID]}
    )

    # delete photo from storage
    r = S3_STORAGE.delete_object(
        Bucket=S3_IMAGE_UPLOAD_BUCKET,
        Key=event_body[API_PHOTO_ID])

    return APIResponseSuccess(message="Photo deleted").send()


@validate_params(API_USER_EMAIL, API_USER_SESSION_KEY, API_LOCATION_ID)
@auth_session()
def user_submit_petition(event_body, context):
    f"""
    Handles user petitioning for a location to add offers.
    :param event: (Auto) JSON-encoded API Gateway pass-through.
    :param {API_USER_EMAIL}: (Req) User identifier
    :param {API_USER_SESSION_KEY}: (Req) Temporary user-specific session key for authentication
    :param {API_LOCATION_ID}: (Req) Google Location ID for petition location
    :param context: (Auto) JSON-encoded API Gateway pass-through. Not used.
    :return: number of petitions under location ID key if successful
    """

    # get user's current petitions
    user_info = USER_TABLE.get_item(Key={USER_EMAIL_FIELD: event_body[API_USER_EMAIL]})

    try:
        user_petitioned_locations_list = user_info['Item'][USER_PETITIONED_LOCATIONS_FIELD]
    except KeyError:
        user_petitioned_locations_list = []

    # check user hasn't already petitioned location
    if event_body[API_LOCATION_ID] in user_petitioned_locations_list:
        return APIResponse(status_code=409, message="User has already petitioned this location.").send()

    # update user petitions
    user_petitioned_locations_list.append(event_body[API_LOCATION_ID])

    USER_TABLE.update_item(
        Key={USER_EMAIL_FIELD: event_body[API_USER_EMAIL]},
        UpdateExpression=f'SET {USER_PETITIONED_LOCATIONS_FIELD} = :user_petitioned_locations',
        ExpressionAttributeValues={
            ':user_petitioned_locations': user_petitioned_locations_list
        })

    postgres_db = psycopg2.connect(dbname=AWS_POSTGRES_DB, user=AWS_POSTGRES_USER, password=AWS_POSTGRES_PASSWORD,
                                   host=AWS_POSTGRES_URL)
    postgres_db.autocommit = True
    postgres_db_cursor = postgres_db.cursor(cursor_factory=RealDictCursor)

    # save petition to petitions table
    postgres_db_cursor.execute(f"select {PETITION_COUNT_FIELD} from {PETITION_TABLE_NAME} where "
                               f"{LOCATION_ID_FIELD} = %(loc_id)s", {'loc_id': event_body[API_LOCATION_ID]})
    r = postgres_db_cursor.fetchall()

    if len(r) == 0:
        new_petition_count = 1
        postgres_db_cursor.execute(f"insert into {PETITION_TABLE_NAME} values (%(loc_id)s, %(new_count)s);",
                                   {'new_count': new_petition_count, 'loc_id': event_body[API_LOCATION_ID]})
    else:
        new_petition_count = r[0][LOCATION_ID_FIELD] + 1
        postgres_db_cursor.execute(f"update {PETITION_TABLE_NAME} set {PETITION_COUNT_FIELD} = %(new_count)s where "
                                   f"{LOCATION_ID_FIELD} = %(loc_id)s;", {'new_count': new_petition_count,
                                                                          'loc_id': event_body[API_LOCATION_ID]})

    postgres_db_cursor.close()
    postgres_db.close()

    return APIResponseSuccess(response_content={event_body[API_LOCATION_ID]: str(new_petition_count)}).send()
