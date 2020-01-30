from celery.utils.log import get_task_logger
from typing import Dict, List

from worker import get_db_client, get_cliff_client, get_genderize_client, places
from worker.celery import app
from worker.cache import cache

logger = get_task_logger(__name__)
SAVE_TO_DB = True  # really save results to the DB?


@cache.cache_on_arguments()
def fetch_cliff_results(text: str) -> Dict[str, Dict]:
    cliff = get_cliff_client()
    results = cliff.parse_text(text)
    return results


@app.task(serializer='json', bind=True)
def parse_with_cliff(self, story):
    cliff_results = {}
    if 'text' not in story:
        logger.error('{} - no text')
        return
    elif len(story['text']) == 0:
        logger.warning('{} - no chars in text')
        # OK to save the empty list of quotes here because we don't have any text in story
    else:
        cliff_results = fetch_cliff_results(story['text'])
    # make a local connection to DB, because this is in its own thread
    collection = get_db_client()
    if SAVE_TO_DB:  # write all the quotes to the DB
        collection.update_one({'stories_id': story['stories_id']},
                              {'$set': {'raw_cliff_results': cliff_results}})
        logger.info('{} - Saved results to DB '.format(story['stories_id']))
    else:
        logger.info('{} - NOT SAVED'.format(story['stories_id']))


@cache.cache_on_arguments()
def fetch_genderize_results(names: List[str], country_id) -> List[Dict]:
    genderize = get_genderize_client()
    results = genderize.get(names, country_id=country_id)
    return results


@app.task(serializer='json', bind=True)
def parse_with_genderize(self, story):
    names = [p['name'] for p in story['people']]
    if len(names) > 0:  # don't send it to genderize if there aren't any people
        results = fetch_genderize_results(names, story['place_iso_code'])
        first_names = [n.split( )[0] for n in names]
        first_name_results = fetch_genderize_results(first_names, story['place_iso_code'])
    else:
        results = []
        first_name_results = []
    # make a local connection to DB, because this is in its own thread
    collection = get_db_client()
    if SAVE_TO_DB:  # write all the data to the DB
        collection.update_one({'stories_id': story['stories_id']},
                              {'$set': {
                                  'raw_genderize_results': results,
                                  'raw_first_name_genderize_results': first_name_results,
                              }})
        logger.info('{} - Saved results to DB '.format(story['stories_id']))
    else:
        logger.info('{} - NOT SAVED'.format(story['stories_id']))