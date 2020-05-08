import os
import logging
import pymongo
import re
import csv
import sys
from dotenv import load_dotenv
import datetime
import mediacloud.api
from cliff.api import Cliff
from genderize import Genderize

load_dotenv()  # load config from .env file

from worker.cache import cache

# set up logging
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(name)s | %(message)s')
logger = logging.getLogger(__name__)
logger.info("------------------------------------------------------------------------")
logger.info("Starting up Quote Worker")

BROKER_URL = os.environ['BROKER_URL']
logger.info("BROKER_URL: {}".format(BROKER_URL))

CLIFF_URL = os.environ['CLIFF_URL']
logger.info("CLIFF_URL: {}".format(CLIFF_URL))

MONGO_DSN = os.environ['MONGO_DSN']
logger.info("MONGO_DSN: {}".format(MONGO_DSN))

DB_NAME = os.environ['MONGO_DB']
logger.info("DB_NAME: {}".format(DB_NAME))

COLLECTION_NAME = os.environ['MONGO_COLLECTION']
logger.info("COLLECTION_NAME: {}".format(COLLECTION_NAME))

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
    collection = db[COLLECTION_NAME]
    return collection


def get_mc_client():
    return mediacloud.api.MediaCloud(MC_API_KEY)


def get_cliff_client():
    return Cliff(CLIFF_URL)


def get_genderize_client():
    return Genderize(api_key=GENDERIZE_API_KEY)


SINGLE_NAME_GENDERS_CSV = os.path.join('data', 'manually coded single names.csv')
def get_single_name_manual_lookup():
    accepted_genders = ['male', 'female']  # most ironic variable name ever?
    custom_gender_data = csv.DictReader(open(SINGLE_NAME_GENDERS_CSV, 'r', encoding='utf-8-sig'))
    gender_lookup = {r['name']: r['gender'].lower() for r in custom_gender_data if r['gender'].lower() in accepted_genders}
    return gender_lookup


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
    {'name': 'India', 'iso_code': 'IN' },
    {'name': 'Kenya', 'iso_code': 'KE' },
    {'name': 'Nigeria', 'iso_code': 'NG' },
    {'name': 'South Africa', 'iso_code': 'ZA' },
    {'name': 'United Kingdom', 'iso_code': 'GB' },
    {'name': 'United States', 'iso_code': 'US' },
]
INPUT_FILE = os.path.join('data', "Gender and Corona Supplement Data Split.csv")
with open(INPUT_FILE, 'r') as f:
    mc = get_mc_client()
    reader = csv.reader(f)
    next(reader)
    for row in reader:
        country = row[0]
        try:
            place = [p for p in places if p['name'].strip() == country][0]
            # 4/5 for WITH corona
            # 8/9 for NO corona
            place['sample_size'] = re.sub("[^0-9]", "", row[8].strip())  # remove commas and such
            place['q'] = row[9] + " AND language:en"
            place['start_date'] = datetime.date(2020, 3, 1)
            place['end_date'] = datetime.date(2020, 4, 15)
        except IndexError:
            logger.error('No place matching {}'.format(country))
            sys.exit()
for p in places:
    p['themes'] = theme_tags
