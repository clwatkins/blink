from config import *
from api_management import validate_params, auth_session, APIResponseSuccess, APIResponse

import json
import datetime as dt
import urllib.request
import urllib.parse

import psycopg2
from psycopg2.extras import RealDictCursor, execute_batch
import boto3
from boto3.dynamodb.conditions import Attr

CLOUDSEARCH_DOMAIN = boto3.client('cloudsearchdomain', endpoint_url=CLOUD_SEARCH_DOMAIN_URL)
DYNAMO_DB = boto3.resource('dynamodb', region_name=AWS_REGION)
PHOTOS_TABLE = DYNAMO_DB.Table(PHOTOS_TABLE_NAME)


@validate_params(API_USER_EMAIL, API_USER_SESSION_KEY)
@auth_session()
def get_user_points(event_body, context):
    f"""
    Handles user petitioning for a location to add offers.
    :param event: (Auto) JSON-encoded API Gateway pass-through.
    :param {API_USER_EMAIL}: (Req) User identifier
    :param {API_USER_SESSION_KEY}: (Req) Temporary user-specific session key for authentication
    :param context: (Auto) JSON-encoded API Gateway pass-through. Not used.
    :return: list of brand: points pairings for all brands where a user has points
    """

    postgres_db = psycopg2.connect(dbname=AWS_POSTGRES_DB, user=AWS_POSTGRES_USER, password=AWS_POSTGRES_PASSWORD,
                                   host=AWS_POSTGRES_URL)
    postgres_db.autocommit = True
    postgres_db_cursor = postgres_db.cursor(cursor_factory=RealDictCursor)

    # get list of current location ids with offers
    postgres_db_cursor.execute(f"""
        SELECT * from {LOCATION_INFO_TABLE_NAME} where lower({LOCATION_BRAND_FIELD}) in
        (select distinct(lower({OFFER_BRAND_FIELD})) from {OFFER_TABLE_NAME}) ORDER BY {LOCATION_BRAND_FIELD};""")
    current_offers_list = postgres_db_cursor.fetchall()

    if len(current_offers_list) == 0:
        return APIResponseSuccess(message="There are no current offers").send()

    # get list of user photos where location id is in the list of brands with current offers
    user_photo_results = PHOTOS_TABLE.scan(
        FilterExpression=Attr(PHOTO_OWNER_FIELD).eq(event_body[API_USER_EMAIL]) &
                         Attr(PHOTO_LOCATION_ID_FIELD).is_in(list({_[LOCATION_ID_FIELD] for _ in current_offers_list}))
    )

    # set of location ids where user has posted a photo and there's an offer on
    user_location_ids = {_[LOCATION_ID_FIELD] for _ in user_photo_results['Items']}

    # catch situation where user hasn't posted any photos at locations with offers
    if len(user_location_ids) == 0:
        postgres_db_cursor.close()
        postgres_db.close()
        return APIResponseSuccess(message="No user photos have been posted at locations with offers.").send()

    # set of the brands represented by that set of location ids (where user has posted photos)
    brands_to_query = {_[LOCATION_BRAND_FIELD].lower() for _ in current_offers_list if
                           _[LOCATION_ID_FIELD] in user_location_ids}

    # tally likes on the user's photos for each of those brands
    current_user_points = {}
    brand_loc_map = {_[PHOTO_LOCATION_ID_FIELD]: _[LOCATION_BRAND_FIELD] for _ in current_offers_list}

    for brand in brands_to_query:
        temp_brand_points = []
        for user_photo in user_photo_results['Items']:
            if brand_loc_map[user_photo[PHOTO_LOCATION_ID_FIELD]].lower() == brand.lower():
                temp_brand_points.append(user_photo[PHOTO_LIKES_FIELD])

        current_user_points[brand] = int(sum(temp_brand_points))

    # remove any spent points on previous offer redemptions from point total
    # get list of all past user points transactions
    postgres_db_cursor.execute(f"SELECT * from {REDEMPTION_TABLE_NAME} where {USER_EMAIL_FIELD}=%(email)s;",
                               {'email': event_body[API_USER_EMAIL]})
    user_points_list = postgres_db_cursor.fetchall()

    postgres_db_cursor.close()
    postgres_db.close()

    # iterate through past transactions, checking whether it was for a relevant brand and if so subtracting points
    # from points total
    for transaction in user_points_list:
        if transaction[REDEEMED_BRAND_FIELD] in brands_to_query:
            try:
                current_user_points[transaction[REDEEMED_BRAND_FIELD]] -= transaction[REDEEMED_POINTS_FIELD]
            except KeyError:
                continue

    return APIResponseSuccess(response_content={"user_points": current_user_points}).send()


@validate_params(API_USER_EMAIL, API_USER_SESSION_KEY)
@auth_session()
def get_current_offers(event_body, context):
    f"""
    Handles user petitioning for a location to add offers.
    :param event: (Auto) JSON-encoded API Gateway pass-through.
    :param {API_USER_EMAIL}: (Req) User identifier
    :param {API_USER_SESSION_KEY}: (Req) Temporary user-specific session key for authentication
    :param {API_USER_OFFERS_FLAG}: (Optional) Flag indicating whether to return all offers, or only ones relevant
    to brands a user has points at
    :param context: (Auto) JSON-encoded API Gateway pass-through. Not used.
    :return: list of current offers (each including an offer id, brand it applies to, discount amount, req points)
    """

    try:
        user_relevant = str(event_body[API_USER_OFFERS_FLAG])
        if user_relevant == "True":
            user_relevant_flag = True
        else:
            user_relevant_flag = False
    except KeyError:
        user_relevant_flag = False

    postgres_db = psycopg2.connect(dbname=AWS_POSTGRES_DB, user=AWS_POSTGRES_USER, password=AWS_POSTGRES_PASSWORD,
                                   host=AWS_POSTGRES_URL)
    postgres_db.autocommit = True
    postgres_db_cursor = postgres_db.cursor(cursor_factory=RealDictCursor)

    # get list of current location ids with offers, either all or only those where a user has points
    if not user_relevant_flag:
        postgres_db_cursor.execute(f"SELECT {OFFER_ID_FIELD}, {OFFER_BRAND_FIELD}, {OFFER_DISCOUNT_FIELD}, "
                                   f"{OFFER_POINTS_FIELD} from {OFFER_TABLE_NAME} ORDER BY {OFFER_BRAND_FIELD};")
    else:
        # get list of user photos where location id is in the list of brands with current offers
        user_photos = PHOTOS_TABLE.scan(FilterExpression=Attr(PHOTO_OWNER_FIELD).eq(event_body[API_USER_EMAIL]))

        # set of location ids where user has posted a photo and there's an offer on
        user_location_ids = {_[LOCATION_ID_FIELD] for _ in user_photos['Items']}

        postgres_db_cursor.execute(f"""SELECT {OFFER_ID_FIELD}, {OFFER_BRAND_FIELD}, {OFFER_DISCOUNT_FIELD}, 
        {OFFER_POINTS_FIELD} from {OFFER_TABLE_NAME} WHERE LOWER({OFFER_BRAND_FIELD}) IN 
        (SELECT DISTINCT(LOWER({LOCATION_BRAND_FIELD})) FROM {LOCATION_INFO_TABLE_NAME} WHERE {LOCATION_ID_FIELD} 
        IN %(loc_list)s) ORDER BY {OFFER_BRAND_FIELD};""", {'loc_list': tuple(user_location_ids)})

    user_offers = postgres_db_cursor.fetchall()

    postgres_db_cursor.close()
    postgres_db.close()

    return APIResponseSuccess(response_content={"current_offers": user_offers}).send()


@validate_params(API_USER_EMAIL, API_USER_SESSION_KEY, API_OFFER_ID_TO_REDEEM)
@auth_session()
def redeem_offer(event_body, context):
    f"""
    Handles user redemption of a particular offer.
    :param event: (Auto) JSON-encoded API Gateway pass-through.
    :param {API_USER_EMAIL}: (Req) User identifier
    :param {API_USER_SESSION_KEY}: (Req) Temporary user-specific session key for authentication
    :param {API_OFFER_ID_TO_REDEEM}: (Req) ID of offer to redeem for user
    to brands a user has points at
    :param context: (Auto) JSON-encoded API Gateway pass-through. Not used.
    :return: discount code under "discount_code" key, offer id confirmation under "offer_id" key if successful
    """

    # get latest user points values
    user_point_return = get_user_points(event_body, context)

    postgres_db = psycopg2.connect(dbname=AWS_POSTGRES_DB, user=AWS_POSTGRES_USER, password=AWS_POSTGRES_PASSWORD,
                                   host=AWS_POSTGRES_URL)
    postgres_db.autocommit = True
    postgres_db_cursor = postgres_db.cursor(cursor_factory=RealDictCursor)

    # get points required for redemption, catching invalid offer ID
    postgres_db_cursor.execute(f"""SELECT * from {OFFER_TABLE_NAME} where {OFFER_ID_FIELD}=%(offer_id)s;""",
                               {'offer_id': event_body[API_OFFER_ID_TO_REDEEM]})
    try:
        offer_info = postgres_db_cursor.fetchone()
        required_offer_points = offer_info[OFFER_POINTS_FIELD]
    except (IndexError, KeyError):
        postgres_db_cursor.close()
        postgres_db.close()

        return APIResponse(status_code=400, message="Discount for this brand doesn't exist").send()

    # return valid discount code from db, checking that user has enough points for redemption
    if json.loads(user_point_return["body"])["user_points"][offer_info[OFFER_BRAND_FIELD]] < required_offer_points:
        postgres_db_cursor.close()
        postgres_db.close()

        return APIResponse(status_code=400, message="User doesn't have sufficient points for this offer").send()

    # add transaction to points table
    postgres_db_cursor.execute(f"""
    INSERT INTO {REDEMPTION_TABLE_NAME} ({REDEEMED_USER_FIELD}, 
    {REDEEMED_POINTS_FIELD}, {REDEEMED_BRAND_FIELD}, {REDEEMED_DT_FIELD}) VALUES 
    (%(email)s, %(points)s, %(brand)s, %(dt)s)""", {'email': event_body[API_USER_EMAIL], 'points': required_offer_points,
                                                    'brand': offer_info[OFFER_BRAND_FIELD],
                                                    'dt': int(dt.datetime.strftime(dt.datetime.utcnow(), DT_FORMAT))})

    postgres_db_cursor.close()
    postgres_db.close()

    # return offer discount code to client
    return APIResponseSuccess(response_content={"offer_id": event_body[API_OFFER_ID_TO_REDEEM],
                                                "discount_code": offer_info[OFFER_CODE_FIELD]}).send()


def update_location_info(event, context):
    f"""
    Handles user redemption of a particular offer.
    :param event: (Auto) JSON-encoded API Gateway pass-through. Not used.
    :param context: (Auto) JSON-encoded API Gateway pass-through. Not used.
    :return: string "Update successful" if successful.
    """

    postgres_db = psycopg2.connect(dbname=AWS_POSTGRES_DB, user=AWS_POSTGRES_USER, password=AWS_POSTGRES_PASSWORD,
                                   host=AWS_POSTGRES_URL)
    postgres_db.autocommit = True
    postgres_db_cursor = postgres_db.cursor(cursor_factory=RealDictCursor)

    # get list of current location ids that we've already gotten name info for (testing for null or empty values)
    postgres_db_cursor.execute(f"""SELECT * from {LOCATION_INFO_TABLE_NAME} WHERE 
    COALESCE({LOCATION_NAME_FIELD}, '') <> '';""")
    current_location_id_set = {_[LOCATION_ID_FIELD] for _ in postgres_db_cursor.fetchall()}

    if len(current_location_id_set) == 0:
        req_loc_query = "matchall"
    else:
        req_loc_query = "(not (or '{}'))".format("' '".join(current_location_id_set))

    # get list of locations to parse by excluding current those locations with current info
    req_loc_search = CLOUDSEARCH_DOMAIN.search(
        size=10000,
        start=0,
        queryParser="structured",
        query=req_loc_query,
        queryOptions=json.dumps({"fields": [LOCATION_ID_FIELD]}),
        returnFields=LOCATION_ID_FIELD)

    # make sure there are results to process
    if req_loc_search['hits']['found'] == 0:
        postgres_db_cursor.close()
        postgres_db.close()

        return APIResponseSuccess(message="Update successful").send()

    # build list of Google location ids to query based on Cloudsearch results
    req_loc_list = []
    for result in req_loc_search['hits']['hit']:
        try:
            req_loc_list.append(result['fields'][LOCATION_ID_FIELD][0])
        except KeyError:
            continue

    # find unique location values with set
    req_loc_set_list = list(set(req_loc_list))

    # get location name for each of the locations from google maps
    new_loc_names = []
    req_loc_iter_list = req_loc_set_list.copy()
    for loc_id in req_loc_iter_list:
        loc_details_event_body = urllib.parse.urlencode({
            'key': GOOGLE_MAPS_API_KEY,
            'placeid': loc_id})

        f = urllib.request.urlopen("https://maps.googleapis.com/maps/api/place/details/json?{}".format(
            loc_details_event_body))
        r = f.read()

        try:
            # get location name
            loc_name = json.loads(r.decode())['result']['name']
            new_loc_names.append(loc_name)

        # catch any unexpected errors where a google location id doesn't return a place name -- remove from list of IDs
        except KeyError:
            req_loc_set_list.remove(loc_id)

    # update database with new location information (if we don't have names for the locations they won't be inserted)
    execute_batch(postgres_db_cursor, f"""INSERT INTO {LOCATION_INFO_TABLE_NAME} ({LOCATION_ID_FIELD}, 
    {LOCATION_NAME_FIELD}) VALUES (%s, %s);""", list(zip(req_loc_set_list, new_loc_names)))

    #############################################################
    # Update brands
    # get list of location names that we've haven't matched to a brand
    postgres_db_cursor.execute(f"""SELECT * from {LOCATION_INFO_TABLE_NAME} WHERE 
    COALESCE({LOCATION_BRAND_FIELD}, '') = '' AND COALESCE({LOCATION_NAME_FIELD}, '') <> '';""")
    req_brand_loc_info = postgres_db_cursor.fetchall()
    req_brand_loc_id_list = [_[LOCATION_ID_FIELD] for _ in req_brand_loc_info]

    # parse list of location names for names of brands with offers
    postgres_db_cursor.execute(f"SELECT DISTINCT({OFFER_BRAND_FIELD}) from {OFFER_TABLE_NAME};")
    current_brand_name_set = {_[OFFER_BRAND_FIELD] for _ in postgres_db_cursor.fetchall()}

    new_loc_brands = []

    # find brands within new location names
    for loc in req_brand_loc_info:
        found = False
        for brand in current_brand_name_set:
            if loc[LOCATION_NAME_FIELD].lower().find(brand.lower()) >= 0 and found is False:
                new_loc_brands.append(brand)
                found = True

        # don't upload to database if we don't find a brand in the location name
        if found is False:
            req_brand_loc_id_list.remove(loc[LOCATION_ID_FIELD])

    # upload new list of location ids, brands to location table
    execute_batch(postgres_db_cursor, f"""UPDATE {LOCATION_INFO_TABLE_NAME} SET {LOCATION_BRAND_FIELD} = %s WHERE
    {LOCATION_ID_FIELD} = %s""", list(zip(new_loc_brands, req_brand_loc_id_list)))

    postgres_db_cursor.close()
    postgres_db.close()

    return APIResponseSuccess(message="Update successful").send()
