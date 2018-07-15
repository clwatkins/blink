from api_management import validate_params, generate_user_key, auth_session, session_authoriser, \
    APIResponse, APIResponseError, APIResponseUnauthorized, APIResponseSuccess
from config import *

import json
import datetime as dt

import psycopg2
from psycopg2.extras import RealDictCursor
import boto3

DYNAMO_DB = boto3.resource('dynamodb', region_name=AWS_REGION)
USER_TABLE = DYNAMO_DB.Table(USER_TABLE_NAME)


@validate_params(API_USER_EMAIL, API_USER_PASSWORD)
def create_new_user(event_body, context):
    f"""
    Handles creation of a new user account.
    :param event: (Auto) JSON-encoded API Gateway pass-through.
    :param {API_USER_EMAIL}: (Req) User identifier
    :param {API_USER_PASSWORD}: (Req) HASHED user password that will be directly stored in DB
    :param context: (Auto) JSON-encoded API Gateway pass-through. Not used.
    :return: temporary session key if successful for future request authentication.
    """

    # check for existing user
    if "Item" in USER_TABLE.get_item(Key={USER_EMAIL_FIELD: event_body[API_USER_EMAIL]}).keys():
        return APIResponse(status_code=409, message="User already exists").send()

    # else add a new one
    new_user_response = USER_TABLE.put_item(
        Item={
            USER_EMAIL_FIELD: event_body[API_USER_EMAIL],
            USER_PASSWORD_FIELD: event_body[API_USER_PASSWORD],
            USER_CREATE_DT_FIELD: int(dt.datetime.strftime(dt.datetime.utcnow(), DT_FORMAT)),
            USER_ACCOUNT_TYPE_FIELD: 'email',
            USER_ID_FIELD: 'user_' + generate_user_key(USER_KEY_LENGTH),
            USER_LAST_ACCESSED_FIELD: int(dt.datetime.strftime(dt.datetime.utcnow(), DT_FORMAT))
        }
    )

    # check that adding user was successful
    if new_user_response['ResponseMetadata']['HTTPStatusCode'] == 200:
        # if so, log them in
        return user_session_login(event_body, context)
    else:
        return APIResponseError(status_code=500, message="Server error on user addition").send()


@validate_params(API_USER_EMAIL, API_USER_SESSION_KEY, API_USER_AGE, API_USER_GENDER, API_USER_SIZE, API_USER_NAME)
@auth_session()
def set_user_preferences(event_body, context):
    f"""
    Handles setting of additional user information / preferences.
    :param event: (Auto) JSON-encoded API Gateway pass-through.
    :param {API_USER_EMAIL}: (Req) User identifier
    :param {API_USER_SESSION_KEY}: (Req) Temporary user-specific session key for authentication
    :param {API_USER_AGE}: (Req) User age to be stored in DB (empty string if none)
    :param {API_USER_GENDER}: (Req) User gender to be stored in DB (M/F/O, empty string if none)
    :param {API_USER_SIZE}: (Req) User size ot be stored in DB (S/M/L/etc, empty string if none)
    :param {API_USER_NAME}: (Req) Name of user to be displayed (empty string if none)
    :param context: (Auto) JSON-encoded API Gateway pass-through. Not used.
    :return: User information if update successful
    """

    # update user age, size, gender, name
    USER_TABLE.update_item(
        Key={
            API_USER_EMAIL: event_body[API_USER_EMAIL]
        },
        UpdateExpression=f"""SET {USER_AGE_FIELD} = :user_age, {USER_SIZE_FIELD} = :user_size, {USER_GENDER_FIELD} = 
        :user_gender, {USER_NAME_FIELD} = :user_name""",
        ExpressionAttributeValues={
            ':user_age': event_body[API_USER_AGE],
            ':user_size': event_body[API_USER_SIZE],
            ':user_gender': event_body[API_USER_GENDER],
            ':user_name': event_body[API_USER_NAME]
        })

    # return success message, getting fresh user info from database for response
    return APIResponseSuccess(message="User preferences updated.",
                              response_content={"user_info": json.loads(
                                  get_user_preferences(event_body, context)['body'])['user_info']}).send()


@validate_params(API_USER_EMAIL, API_USER_SESSION_KEY)
@auth_session()
def get_user_preferences(event_body, context):
    f"""
    Handles retrieval of additional user information / preferences on demand.
    :param event: (Auto) JSON-encoded API Gateway pass-through.
    :param {API_USER_EMAIL}: (Req) User identifier
    :param {API_USER_SESSION_KEY}: (Req) Temporary user-specific session key for authentication
    :param context: (Auto) JSON-encoded API Gateway pass-through. Not used.
    :return: User information if request successful
    """

    # get user information
    user_info = USER_TABLE.get_item(Key={API_USER_EMAIL: event_body[API_USER_EMAIL]})

    # remove sensitive fields that shouldn't be returned
    cleaned_user_info = user_info['Item']
    cleaned_user_info.pop(USER_SESSION_KEYS_FIELD)
    cleaned_user_info.pop(USER_PASSWORD_FIELD)
    cleaned_user_info.pop(USER_EMAIL_FIELD)

    return APIResponseSuccess(response_content={"user_info": cleaned_user_info}).send()


@validate_params(API_USER_EMAIL, API_USER_SESSION_KEY)
@auth_session()
def get_user_photos(event_body, context):
    f"""
    Handles retrieval of list of photos a user has posted.
    :param event: (Auto) JSON-encoded API Gateway pass-through.
    :param {API_USER_EMAIL}: (Req) User identifier
    :param {API_USER_SESSION_KEY}: (Req) Temporary user-specific session key for authentication
    :param context: (Auto) JSON-encoded API Gateway pass-through. Not used.
    :return: Array of photo ids belonging to user if successful, under "user_photos" key
    """

    # get user information
    user_info = USER_TABLE.get_item(Key={API_USER_EMAIL: event_body[API_USER_EMAIL]})

    try:
        return APIResponseSuccess(response_content={"user_photos": user_info['Item'][USER_PHOTOS_FIELD]}).send()
    except KeyError:
        return APIResponseSuccess(message="User has no uploaded photos", response_content={"user_photos": []}).send()


@validate_params(API_USER_EMAIL, API_USER_SESSION_KEY)
@auth_session()
def get_user_likes(event_body, context):
    f"""
    Handles retrieval of list of photos a user has liked.
    :param event: (Auto) JSON-encoded API Gateway pass-through.
    :param {API_USER_EMAIL}: (Req) User identifier
    :param {API_USER_SESSION_KEY}: (Req) Temporary user-specific session key for authentication
    :param context: (Auto) JSON-encoded API Gateway pass-through. Not used.
    :return: Array of photo ids that the user has liked if successful, under "user_likes" key
    """

    # get user information
    user_info = USER_TABLE.get_item(Key={API_USER_EMAIL: event_body[API_USER_EMAIL]})

    try:
        return APIResponseSuccess(response_content={"user_likes": user_info['Item'][USER_PHOTO_LIKES_FIELD]}).send()

    except KeyError:
        return APIResponseSuccess(message="User has liked no photos", response_content={"user_likes": []}).send()


@validate_params(API_USER_EMAIL, API_USER_PASSWORD)
def user_session_login(event_body, context):
    f"""
    Handles user session log-ins, generating temporary session key for further API requests. Keys auto-expire
    after a defined period, requiring a new session log in.
    :param event: (Auto) JSON-encoded API Gateway pass-through.
    :param {API_USER_EMAIL}: (Req) User identifier
    :param {API_USER_PASSWORD}: (Req) HASHED user password that will be directly stored in DB
    :param context: (Auto) JSON-encoded API Gateway pass-through. Not used.
    :return: Temporary session key under "session_key" key if successful
    """

    # get stored password for email
    db_password_response = USER_TABLE.get_item(
        Key={
            API_USER_EMAIL: event_body[API_USER_EMAIL]
        })

    # verify password
    try:
        db_password = db_password_response['Item'][API_USER_PASSWORD]
    except KeyError:
        return APIResponseUnauthorized().send()

    if event_body[API_USER_PASSWORD] != db_password:
        return APIResponseUnauthorized().send()

    # get list of current session keys
    current_user_data = USER_TABLE.get_item(Key={API_USER_EMAIL: event_body[API_USER_EMAIL]})
    try:
        session_keys_list = current_user_data['Item'][USER_SESSION_KEYS_FIELD]
    except KeyError:
        session_keys_list = []

    # generate and store session key in user table
    new_session_key = generate_user_key(USER_KEY_LENGTH)

    session_keys_list.append(new_session_key)

    USER_TABLE.update_item(
        Key={
            API_USER_EMAIL: event_body[API_USER_EMAIL]
        },
        UpdateExpression=f'SET {USER_SESSION_KEYS_FIELD} = :session_keys_list',
        ExpressionAttributeValues={
            ':session_keys_list': session_keys_list
        })

    # add login to user login history table
    postgres_db = psycopg2.connect(dbname=AWS_POSTGRES_DB, user=AWS_POSTGRES_USER,
                                   password=AWS_POSTGRES_PASSWORD,
                                   host=AWS_POSTGRES_URL)
    postgres_db.autocommit = True
    postgres_db_cursor = postgres_db.cursor(cursor_factory=RealDictCursor)

    postgres_db_cursor.execute(f"""INSERT INTO {LOGIN_HISTORY_TABLE_NAME} ({LOGIN_HISTORY_DT_FIELD}, 
    {LOGIN_HISTORY_USER_FIELD}) VALUES (%(dt)s, %(user)s)""",
                               {'dt': int(dt.datetime.strftime(dt.datetime.utcnow(), DT_FORMAT)),
                                'user': event_body[API_USER_EMAIL]})

    postgres_db_cursor.close()
    postgres_db.close()

    return APIResponseSuccess(response_content={"session_key": new_session_key}).send()


@validate_params(API_USER_EMAIL, API_USER_SESSION_KEY)
@auth_session()
def user_session_leave(event_body, context):
    f"""
    Handles deletion of session key for a particular user if requested (manual ending of session).
    :param event: (Auto) JSON-encoded API Gateway pass-through.
    :param {API_USER_EMAIL}: (Req) User identifier
    :param {API_USER_SESSION_KEY}: (Req) Temporary user-specific session key for authentication
    :return: "API key destroyed" message if successful
    """

    if session_authoriser(event_body[API_USER_EMAIL], event_body[API_USER_SESSION_KEY], context):
        USER_TABLE.update_item(
            Key={
                API_USER_EMAIL: event_body[API_USER_EMAIL]
            },
            UpdateExpression=f'REMOVE {USER_SESSION_KEYS_FIELD}'
        )
        return APIResponseSuccess(message="API key destroyed").send()
    else:
        return APIResponseUnauthorized().send()
