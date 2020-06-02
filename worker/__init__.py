import os
import logging
import pymongo
import csv
from dotenv import load_dotenv
from mediacloud.api import MediaCloud
from genderize import Genderize

load_dotenv()  # load config from .env file

from worker.cache import cache

# set up logging
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(name)s | %(message)s')
logger = logging.getLogger(__name__)
logger.info("------------------------------------------------------------------------")
logger.info("Starting up Quote Worker")

DATA_FILE_NAME = "Gates Covid Chapter Quotes - for Processing - NOCOVID.csv"

BROKER_URL = os.environ['BROKER_URL']
logger.info("BROKER_URL: {}".format(BROKER_URL))

MONGO_DSN = os.environ['MONGO_DSN']
logger.info("MONGO_DSN: {}".format(MONGO_DSN))

DB_NAME = os.environ['MONGO_DB']
logger.info("DB_NAME: {}".format(DB_NAME))

COLLECTION_NAME = os.environ['MONGO_COLLECTION']
logger.info("COLLECTION_NAME: {}".format(COLLECTION_NAME))

MONGO_COLLECTION = os.environ['MONGO_COLLECTION']
logger.info("MONGO_COLLECTION: {}".format(MONGO_COLLECTION))

CACHE_REDIS_URL = os.environ['CACHE_REDIS_URL']
logger.info("CACHE_REDIS_URL: {}".format(CACHE_REDIS_URL))

GENDERIZE_API_KEY = os.environ['GENDERIZE_API_KEY']
logger.info("GENDERIZE_API_KEY: {}".format(GENDERIZE_API_KEY))

VERSION = os.environ['VERSION']
logger.info("VERSION: {}".format(VERSION))

MC_API_KEY = os.environ['MC_API_KEY']


def get_db_client():
    client = pymongo.MongoClient(MONGO_DSN)
    db = client[DB_NAME]
    collection = db[MONGO_COLLECTION]
    return collection


def get_mc_client():
    return MediaCloud(MC_API_KEY)


def get_genderize_client():
    return Genderize(api_key=GENDERIZE_API_KEY)


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
for p in places:
    p['themes'] = theme_tags

INPUT_FILE = os.path.join('data', "replacements.csv")
replacement_lookup = {}
with open(INPUT_FILE, 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        replacement_lookup[row['original'].lower()] = row['replacement']
logger.info("  loaded {} replacements".format(len(replacement_lookup.keys())))

place_replacements = {
    'us': 'United States',
    'uk': 'United Kingdom'
}
INPUT_FILE = os.path.join('data', DATA_FILE_NAME)
data = []
unique_story_ids = []
with open(INPUT_FILE, 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        if (row['Checked (y/n)'].lower() == 'y') and (row['stories_id'] not in unique_story_ids):
            if row['place'].lower() in place_replacements.keys():
                row['place'] = place_replacements[row['place'].lower()]
            data.append(row)
            unique_story_ids.append(row['stories_id'])
logger.info("  loaded {} stories".format(len(data)))
