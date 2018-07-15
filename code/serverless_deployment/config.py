# CENTRAL CONFIGURATION FOR PROJECT SETTINGS AND DATABASE SCHEMAS

# aws and general configuration
DT_FORMAT = "%Y%m%d%H%M%S"  # dt string formatting style for string storage of dt stamps
USER_KEY_LENGTH = 20  # length of randomly-generated keys for authentication and user ids
AWS_REGION = 'eu-west-1'
DEFAULT_USER_TIMEOUT = 600  # set length of time in seconds before inactive session keys are deleted
S3_IMAGE_UPLOAD_BUCKET = 'blink-image-upload-storage'
READ_CAPACITY_UNITS = 5
WRITE_CAPACITY_UNITS = 5
CLOUD_SEARCH_DOMAIN_NAME = 'blink-photo-search'
CLOUD_SEARCH_DOMAIN_URL = 'http://search-blink-photo-search-msg725fs7ns2z5n5ssbpgssozy.eu-west-1.cloudsearch.amazonaws.com'
GOOGLE_MAPS_API_KEY = 'AIzaSyCivKWCdPjRuUGX_f9fX-JFMk-BF4nHstk'
CLOUD_SEARCH_SORT_EXPRESSION = 'blink_default'

# rds db config
AWS_POSTGRES_URL = "browsepostgres.cinxgia6szkc.eu-west-1.rds.amazonaws.com"
AWS_POSTGRES_USER = "clwmaster"
AWS_POSTGRES_PASSWORD = "getbrowse!"
AWS_POSTGRES_DB = "browse"

#########################
# dynamodb config

# photo table
PHOTOS_TABLE_NAME = 'photos_table'
PHOTO_ID_FIELD = 'photo_id'
PHOTO_LIKES_FIELD = 'photo_likes'
PHOTO_OWNER_FIELD = 'photo_owner'
PHOTO_TAGS_FIELD = 'photo_tags'
PHOTO_DT_FIELD = 'photo_taken_dt'
PHOTO_LATLON_FIELD = 'latlon'
PHOTO_LOCATION_ID_FIELD = 'google_location_id'
PHOTO_COMMENTS_FIELD = 'photo_comments'
PHOTO_VIEWS_FIELD = 'photo_views'

# user table
USER_TABLE_NAME = 'user_table'
USER_LAST_ACCESSED_FIELD = 'user_last_accessed'
USER_SESSION_KEYS_FIELD = 'user_session_keys'
USER_ACCOUNT_TYPE_FIELD = 'account_type'
USER_EMAIL_FIELD = 'user_email'
USER_PASSWORD_FIELD = 'user_password'
USER_CREATE_DT_FIELD = 'user_create_dt'
USER_ID_FIELD = 'user_id'
USER_LOGIN_HISTORY_ID_FIELD = 'user_login_history_id'
USER_PHOTOS_FIELD = 'user_photos'
USER_PHOTO_LIKES_FIELD = 'user_photo_likes'
USER_AGE_FIELD = 'user_age'
USER_SIZE_FIELD = 'user_size'
USER_GENDER_FIELD = 'user_gender'
USER_NAME_FIELD = 'user_name'
USER_PETITIONED_LOCATIONS_FIELD = 'user_petitioned_locations'

#################################
# rds table config
# offers table
OFFER_TABLE_NAME = 'offers_table'
OFFER_BRAND_FIELD = 'brand'
OFFER_ID_FIELD = 'offer_id'
OFFER_DISCOUNT_FIELD = 'discount_amount'
OFFER_POINTS_FIELD = 'discount_points_req'
OFFER_CODE_FIELD = 'discount_code'
OFFER_BRAND_LOGO_FIELD = 'brand_logo_url'

# points table
REDEMPTION_TABLE_NAME = 'redemption_table'
REDEEMED_POINTS_FIELD = 'redeemed_points'
REDEEMED_BRAND_FIELD = OFFER_BRAND_FIELD
REDEEMED_USER_FIELD = USER_EMAIL_FIELD
REDEEMED_DT_FIELD = 'redemption_dt'
REDEMPTION_ID_FIELD = 'redemption_id'

# location info table
LOCATION_INFO_TABLE_NAME = 'location_info_table'
LOCATION_ID_FIELD = PHOTO_LOCATION_ID_FIELD
LOCATION_NAME_FIELD = 'location_name'
LOCATION_BRAND_FIELD = 'location_brand'

# petition table
PETITION_TABLE_NAME = 'petition_table'
PETITION_COUNT_FIELD = 'location_petitions_count'

# login history table
LOGIN_HISTORY_TABLE_NAME = 'login_history_table'
LOGIN_HISTORY_ID_FIELD = 'login_history_id'
LOGIN_HISTORY_DT_FIELD = 'login_dt'
LOGIN_HISTORY_USER_FIELD = USER_EMAIL_FIELD

# like history table
LIKE_HISTORY_TABLE_NAME = 'like_history_table'
LIKE_HISTORY_ID_FIELD = 'like_history_id'
LIKE_HISTORY_PHOTO_ID_FIELD = 'liked_photo_id'
LIKE_HISTORY_USER_ID_FIELD = 'like_user'
LIKE_TYPE_FIELD = 'like_type'
LIKE_HISTORY_DT_FIELD = 'like_dt'

#########################
# api field names
API_USER_EMAIL = USER_EMAIL_FIELD
API_USER_PASSWORD = USER_PASSWORD_FIELD
API_USER_SESSION_KEY = 'session_key'
API_PHOTO_DATE = 'photo_capture_datetime'
API_PHOTO_DATA = 'photo_data'  # sent as base64 encoded image
API_PHOTO_COMMENT = 'photo_comments'
API_USER_LAT = 'user_lat'
API_USER_LON = 'user_lon'
API_FILTER_TERMS = 'filter_terms'
API_PHOTO_LAT = 'photo_lat'
API_PHOTO_LON = 'photo_lon'
API_USER_RADIUS = 'user_radius'  # search radius in minutes of walking
API_USER_AGE = USER_AGE_FIELD
API_USER_SIZE = USER_SIZE_FIELD
API_USER_GENDER = USER_GENDER_FIELD
API_USER_NAME = USER_NAME_FIELD
API_SEARCH_SIZE = 'search_size'  # number of results to send
API_SEARCH_START = 'search_start'  # result number to start at
API_PHOTO_ID = PHOTO_ID_FIELD
API_LOCATION_ID = 'google_place_id'  # Google Place ID
API_OFFER_ID_TO_REDEEM = 'redeem_offer_id'
API_LIGHTWEIGHT_FLAG = 'lightweight_return'
API_USER_OFFERS_FLAG = 'user_offers_only'
