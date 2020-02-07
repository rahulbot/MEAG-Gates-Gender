import os
import logging
import pymongo
import csv
import sys
from dotenv import load_dotenv
from mediacloud.api import MediaCloud
from cliff.api import Cliff
from genderize import Genderize
import datetime as dt

load_dotenv()  # load config from .env file

from worker.cache import cache

# set up logging
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(name)s | %(message)s')
logger = logging.getLogger(__name__)
logger.info("------------------------------------------------------------------------")
logger.info("Starting up Quote Worker")

DB_NAME = "mc-gates-headlines"
COLLECTION_NAME = "test-stories"

BROKER_URL = os.environ['BROKER_URL']
logger.info("BROKER_URL: {}".format(BROKER_URL))

CLIFF_URL = os.environ['CLIFF_URL']
logger.info("CLIFF_URL: {}".format(CLIFF_URL))

MONGO_DSN = os.environ['MONGO_DSN']
logger.info("MONGO_DSN: {}".format(MONGO_DSN))

CACHE_REDIS_URL = os.environ['CACHE_REDIS_URL']
logger.info("CACHE_REDIS_URL: {}".format(CACHE_REDIS_URL))

MC_API_KEY = os.environ['MC_API_KEY']

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


# load in places from CSV file
themes_tag_ids = [
    9360836,  # politics and government
    9360989,  # economic conditions and trends
    9360837,  # law and legislation
    9360912,  # crime and criminals
    9361116,  # violence
    9360911,  # media
    9360939,  # music
    9360845,  # motion pictures
    9361016,  # theater
    9360844,  # books and literature
    9360879,  # television
    9361065,  # culture
    9361080,  # science and technology
    9360852,  # medicine and health
    9361137,  # athletics and sports
]
@cache.cache_on_arguments()
def cached_theme(tags_id):
    mc = get_mc_client()
    return mc.tag(tags_id)
theme_tags = [cached_theme(tags_id) for tags_id in themes_tag_ids]

places = [
    {'name': 'India', 'iso_code': 'IN', 'sources': []},
    {'name': 'Kenya', 'iso_code': 'KE', 'sources': []},
    {'name': 'Nigeria', 'iso_code': 'NG', 'sources': []},
    {'name': 'South Africa', 'iso_code': 'ZA', 'sources': []},
    {'name': 'United Kingdom', 'iso_code': 'GB', 'sources': []},
    {'name': 'United States', 'iso_code': 'US', 'sources': []},
]
INPUT_FILE = os.path.join('data', "Gates Gender Selected Media.csv")
with open(INPUT_FILE, 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        country = row['PUB COUNTRY']
        try:
            place = [p for p in places if p['name'].strip() == country][0]
            place['sources'].append({'media_id': row['Media Cloud ID'], 'sample_size': row[' Sample Size Needed (95% conf, 2% MOE) ']})
        except IndexError:
            logger.error('No place matching {}'.format(country))
            sys.exit()
for p in places:
    p['date_query'] = MediaCloud.publish_date_query(dt.date(2019, 1, 1), dt.date(2019, 12, 31))
    p['themes'] = theme_tags

