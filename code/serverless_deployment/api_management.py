from config import *

import datetime as dt
import json
import random
import string
from functools import wraps

import boto3
from boto3.dynamodb.conditions import Attr

DYNAMO_DB = boto3.resource('dynamodb', region_name=AWS_REGION)
USER_TABLE = DYNAMO_DB.Table(USER_TABLE_NAME)


class APIResponse:
    """
    Template class to represent and return Lambda-compatible API responses.
    """
    def __init__(self, message: str, status_code: int, response_content=None):
        self.message = str(message)
        self.status_code = int(status_code)
        self.response_content = response_content

    def send(self):
        response = dict()
        response["statusCode"] = self.status_code

        if self.response_content:
            response_body = self.response_content
            response_body["message"] = self.message
        else:
            response_body = dict()
            response_body["message"] = self.message

        response["body"] = json.dumps(response_body)

        if not str(self.status_code).startswith("2"):  # log any unsuccessful calls
            print(response)

        return response


class APIResponseSuccess(APIResponse):
    def __init__(self, message="OKAY", status_code=200, response_content=None):
        super().__init__(message=message, status_code=status_code, response_content=response_content)


class APIResponseError(APIResponse):
    def __init__(self, message, status_code=400, response_content=None):
        super().__init__(message=message, status_code=status_code, response_content=response_content)


class APIResponseUnauthorized(APIResponse):
    def __init__(self, message="Couldn't authenticate session", status_code=401, response_content=None):
        super().__init__(message=message, status_code=status_code, response_content=response_content)


class APIResponseForbidden(APIResponse):
    def __init__(self, message="Forbidden", status_code=403, response_content=None):
        super().__init__(message=message, status_code=status_code, response_content=response_content)


def validate_params(*req_params):
    """
    Parameter validation decorator. Takes a list of required parameters encoded in the Lambda event request body
    and verifies they are A. Present and B. Not empty. Passes the JSON-decoded event body as a dict to the calling func.
    :param req_params: *args
    :return: decoded event body as a dict, context object
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*event_context):
            # see if the event has already been decoded (via 2nd order call). JSON-encoded bodies are recognised as str
            try:
                if type(event_context[0]) == dict and 'body' in event_context[0].keys():
                    event_body = json.loads(event_context[0]['body'])
                else:
                    event_body = event_context[0]
            except (json.JSONDecodeError, KeyError) as e:
                    print(e)
                    return APIResponseError(message=e).send()

            for req_param in req_params:
                if req_param not in event_body.keys() or len(str(event_body[req_param])) == 0:
                    msg = f"Bad required parameter: {req_param}"
                    print(msg)
                    return APIResponseError(message=msg).send()
            return func(event_body, event_context[1])
        return wrapper
    return decorator


def auth_session():
    """
    Authorisation decorator to ensure users are authorised and have a current session token to make calls.
    :return: None
    """
    def decorator(func):
        @wraps(func)
        def wrapper(params, context):
            if not session_authoriser(params[API_USER_EMAIL], params[API_USER_SESSION_KEY], context):
                return bad_auth()
            return func(params, context)
        return wrapper
    return decorator


def session_authoriser(user_email, user_session_key, context):
    f"""
    Handles authentication of user session keys, updating last accessed timestamp in database. Includes currently
    unused context-verification function that can assure source of requests.
    :param event: (Auto) JSON-encoded API Gateway pass-through.
    :param {API_USER_EMAIL}: (Req) User identifier
    :param {API_USER_SESSION_KEY}: (Req) Temporary user-specific session key for authentication
    :return: User ID if successful, False otherwise
    """

    if not verify_context_key(context):
        return False

    current_user_data = USER_TABLE.get_item(
        Key={
            API_USER_EMAIL: user_email
        })

    try:
        session_keys_list = current_user_data['Item'][USER_SESSION_KEYS_FIELD]
    except KeyError:
        return False

    # verify app key is in list of current session keys
    if user_session_key in session_keys_list:
        # if so, update the user's account last accessed timestamp
        USER_TABLE.update_item(
            Key={
                API_USER_EMAIL: user_email
            },
            UpdateExpression=f'SET {USER_LAST_ACCESSED_FIELD} = :last_accessed_ts',
            ExpressionAttributeValues={
                ':last_accessed_ts': int(dt.datetime.strftime(dt.datetime.utcnow(), DT_FORMAT))
            })
        return current_user_data['Item'][USER_ID_FIELD]
    else:
        return False


def remove_inactive_keys(event=None, context=None):
    f"""
        Delete any user keys if they haven't interacted with the server within timeout period.
        :param event: (Auto) JSON-encoded API Gateway pass-through.
        :param context: (Auto) JSON-encoded API Gateway pass-through. Not used.
        :return: None
        """

    # generate int representation of time before which all user keys should be deleted
    timeout_ts = dt.datetime.utcnow() - dt.timedelta(seconds=DEFAULT_USER_TIMEOUT)
    timeout_ts_int = int(dt.datetime.strftime(timeout_ts, DT_FORMAT))

    # scan user table for any users who haven't accessed the server since the timeout int
    inactive_user_results = USER_TABLE.scan(
        FilterExpression=Attr(USER_LAST_ACCESSED_FIELD).lt(timeout_ts_int))

    # iterate through results removing session keys list
    for user in inactive_user_results['Items']:
        USER_TABLE.update_item(
            Key={
                USER_EMAIL_FIELD: user[USER_EMAIL_FIELD]
            },
            UpdateExpression=f'REMOVE {USER_SESSION_KEYS_FIELD}'
        )


def verify_context_key(context=None):
    """Placeholder for context verification function"""
    return True


def generate_user_key(key_length):
    """Utility function to generate a random alphanumeric key"""
    return ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(key_length))


def bad_auth():
    """Utility function to return an authentication error message"""
    return APIResponseUnauthorized().send()
