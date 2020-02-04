import os
import logging
import pymongo
from dotenv import load_dotenv
from mediacloud.api import MediaCloud
from cliff.api import Cliff
from genderize import Genderize
import datetime as dt

load_dotenv()  # load config from .env file

# set up logging
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(name)s | %(message)s')
logger = logging.getLogger(__name__)
logger.info("------------------------------------------------------------------------")
logger.info("Starting up Quote Worker")

DB_NAME = "mc-headline-mentions"
COLLECTION_NAME = "stories"

BROKER_URL = os.environ['BROKER_URL']
logger.info("BROKER_URL: {}".format(BROKER_URL))

CLIFF_URL = os.environ['CLIFF_URL']
logger.info("CLIFF_URL: {}".format(CLIFF_URL))

MONGO_DSN = os.environ['MONGO_DSN']
logger.info("MONGO_DSN: {}".format(MONGO_DSN))

CACHE_REDIS_URL = os.environ['CACHE_REDIS_URL']
logger.info("CACHE_REDIS_URL: {}".format(CACHE_REDIS_URL))

MC_API_KEY = os.environ['MC_API_KEY']

places = [
 {'name': 'India', 'iso_code': 'IN', 'sources': [65929, 67799, 39872],},
 {'name': 'Kenya', 'iso_code': 'KE', 'sources': [41687, 59728, 106918]},
 {'name': 'Nigeria', 'iso_code': 'NG', 'sources': [144805, 18021, 299178]},
 {'name': 'South Africa', 'iso_code': 'ZA', 'sources': [85940, 40262]},
 {'name': 'UK', 'iso_code': 'GB', 'sources': [1094, 41150, 1750]},
 {'name': 'US', 'iso_code': 'US', 'sources': [1095, 1092, 1]},
]
for p in places:
    p['query'] = "media_id:({}) AND language:en".format(" ".join([str(mid) for mid in p['sources']]))
    p['date_query'] = MediaCloud.publish_date_query(dt.date(2019, 1, 1), dt.date(2019, 12, 31))

def get_db_client():
    client = pymongo.MongoClient(MONGO_DSN)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    return collection


def get_mc_client():
    return MediaCloud(MC_API_KEY)


def get_cliff_client():
    return Cliff(CLIFF_URL)


def get_genderize_client():
    return Genderize()
