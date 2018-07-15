from config import *
from api_management import validate_params, auth_session, APIResponseSuccess, APIResponse

import json
from math import sqrt

import boto3.dynamodb.types

DYNAMO_DB = boto3.resource('dynamodb', region_name=AWS_REGION)
CLOUDSEARCH_DOMAIN = boto3.client('cloudsearchdomain', endpoint_url=CLOUD_SEARCH_DOMAIN_URL)

USER_TABLE = DYNAMO_DB.Table(USER_TABLE_NAME)
PHOTOS_TABLE = DYNAMO_DB.Table(PHOTOS_TABLE_NAME)

LAMBDA_CLIENT = boto3.client('lambda')


@validate_params(API_USER_EMAIL, API_USER_SESSION_KEY, API_USER_LAT, API_USER_LON, API_USER_RADIUS, API_FILTER_TERMS,
                 API_SEARCH_SIZE, API_SEARCH_START)
@auth_session()
def search_photos(event_body, context):
    f"""
    Core photo search function, handling search and display of user-relevant photos.
    :param event: (Auto) JSON-encoded API Gateway pass-through.
    :param {API_USER_EMAIL}: (Req) User identifier
    :param {API_USER_SESSION_KEY}: (Req) Temporary user-specific session key for authentication
    :param {API_USER_LAT}: (Req) Latitude to search around
    :param {API_USER_LON}: (Req) Longitude to search around
    :param {API_USER_RADIUS}: (Req) Radius (in walking minutes) to search around current location
    :param {API_FILTER_TERMS}: (Req) Array of terms to limit results to. If blank, will search for all results.
    :param {API_SEARCH_SIZE}: (Req) Number of results to return.
    :param {API_SEARCH_START}: (Req) Hit number to return for given query.
    :param {API_LIGHTWEIGHT_FLAG}: (Optional) Flag whether or not to return only list of relevant photo IDs instead of
    all info (speed gains and data savings).
    :param context: (Auto) JSON-encoded API Gateway pass-through. Not used.
    :return: list of photo ids and fields with core data according to search results
    """

    try:
        if event_body[API_LIGHTWEIGHT_FLAG] == "True":
            lightweight_return_flag = True
        else:
            lightweight_return_flag = False
    except KeyError:
            lightweight_return_flag = False

    # determine reasonable user_radius walking distance to degree conversion
    degree_mile_conversion = 69
    walking_speed_mph = 3
    degrees_per_minute = walking_speed_mph / degree_mile_conversion / 60
    preferred_degree_radius = degrees_per_minute * float(event_body[API_USER_RADIUS])

    # build polygon array around user point -- 45-45-90 triangle rule
    move_dist = preferred_degree_radius * sqrt(2)
    loc_bottom_right_bound = ', '.join([str(float(event_body[API_USER_LAT]) - move_dist), str(float(event_body[API_USER_LON]) + move_dist)])
    loc_top_left_bound = ', '.join([str(float(event_body[API_USER_LAT]) + move_dist), str(float(event_body[API_USER_LON]) - move_dist)])

    if len(list(event_body[API_FILTER_TERMS])) == 0:
        text_query = "matchall"
    else:
        text_query = "(or '{}')".format("' '".join(list(event_body[API_FILTER_TERMS])))

    # Execute CloudSearch query -- return will be sorted according to ranking expression on domain
    photos_return = CLOUDSEARCH_DOMAIN.search(
        size=int(event_body[API_SEARCH_SIZE]),
        start=int(event_body[API_SEARCH_START]),
        queryParser="structured",
        filterQuery=f"{PHOTO_LATLON_FIELD}:['{loc_top_left_bound}', '{loc_bottom_right_bound}']",
        query=text_query,
        sort=CLOUD_SEARCH_SORT_EXPRESSION + ' desc',
    )

    # filter out all except photo ID field if lightweight flag in call
    if lightweight_return_flag:
        photos_list_return = [{'id': _['id']} for _ in photos_return['hits']['hit']]
    else:
        photos_list_return = photos_return['hits']['hit']

    # invoke function to update photo views (without waiting for return)
    LAMBDA_CLIENT.invoke(FunctionName="UpdatePhotoViews",
                         InvocationType="Event",
                         Payload=json.dumps({"photos": photos_list_return}))

    return APIResponseSuccess(response_content={"photos": photos_list_return}).send()


def update_photo_views(event, context):
    f"""
    Helper function to update view count on displayed photos without slowing down search return.
    :param event: (Auto) JSON-encoded API Gateway pass-through.
    :param context: (Auto) JSON-encoded API Gateway pass-through. Not used.
    :return: None
    """

    try:
        # decode list of photo IDs sent by the search function
        photos_list = event['photos']

        for photo in photos_list:
            # get_photo_data
            current_photo_data = PHOTOS_TABLE.get_item(Key={PHOTO_ID_FIELD: photo['id']})

            # increment photo view count
            try:
                view_count = current_photo_data['Item'][PHOTO_VIEWS_FIELD] + 1
            except KeyError:
                view_count = 1

            # update views on photo in photo table
            PHOTOS_TABLE.update_item(
                Key={PHOTO_ID_FIELD: photo['id']},
                UpdateExpression=f'SET {PHOTO_VIEWS_FIELD} = :new_view_count',
                ExpressionAttributeValues={
                    ':new_view_count': view_count
                })

        return "Updated views"

    except Exception as e:
        print(e)
        print(event)


@validate_params(API_USER_EMAIL, API_USER_SESSION_KEY, API_PHOTO_ID)
@auth_session()
def get_photo_info(event_body, context):
    f"""
    Handles request for all information about a particular photo.
    :param event: (Auto) JSON-encoded API Gateway pass-through.
    :param {API_USER_EMAIL}: (Req) User identifier
    :param {API_USER_SESSION_KEY}: (Req) Temporary user-specific session key for authentication
    :param {API_PHOTO_ID}: (Req) ID fo photo to fetch all information for
    :param context: (Auto) JSON-encoded API Gateway pass-through. Not used.
    :return: dictionary with photo information under "photo_info" key.
    """

    photo_info_raw = PHOTOS_TABLE.get_item(Key={PHOTO_ID_FIELD: event_body[API_PHOTO_ID]})

    try:
        photo_info_raw['Item']
    except KeyError:
        return APIResponse(status_code=404, message="Photo doesn't exist for this ID").send()

    photo_info = dict()

    # convert data types from DynamDB to Python and JSON-friendly
    for k, v in photo_info_raw['Item'].items():
        if isinstance(v, set):
            photo_info[k] = list(v)
        elif isinstance(v, boto3.dynamodb.types.Decimal):
            photo_info[k] = int(v)
        else:
            photo_info[k] = v

    return APIResponseSuccess(response_content={"photo_info": photo_info}).send()
