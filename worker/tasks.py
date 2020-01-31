from celery.utils.log import get_task_logger
from typing import Dict, List
import copy

from worker import get_db_client, get_cliff_client, get_genderize_client
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
    # parse out people
    people = copy.deepcopy(cliff_results['results']['people'])
    for p in people:
        name_parts = p['name'].split()
        only_one_name = len(name_parts) == 1
        p['name_parts'] = name_parts
        p['first_name'] = name_parts[0] if not only_one_name else None
        p['last_name'] = " ".join(name_parts[1:]) if not only_one_name else p['name']
        p['only_one_name'] = only_one_name
        p['multi_part_name'] = len(name_parts) > 1
    if SAVE_TO_DB:  # write all the quotes to the DB
        collection.update_one({'stories_id': story['stories_id']},
                              {'$set': {'raw_cliff_results': cliff_results, 'people': people}})
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
    people_with_first_names = [p for p in story['people'] if p['multi_part_name']]
    first_names = [p['first_name'] for p in people_with_first_names]
    if len(first_names) > 0:  # don't send it to genderize if there aren't any people
        results = fetch_genderize_results(first_names, story['place_iso_code'])
    else:
        results = []
    # merge data
    people_with_gender = people_with_first_names
    for idx in range(0, len(people_with_gender)):
        del results[idx]['name']
        people_with_gender[idx].update(results[idx])
    # make a local connection to DB, because this is in its own thread
    collection = get_db_client()
    if SAVE_TO_DB:  # write all the data to the DB
        collection.update_one({'stories_id': story['stories_id']},
                              {'$set': {'people_with_gender': people_with_gender}})
        logger.info('{} - Saved results to DB '.format(story['stories_id']))
    else:
        logger.info('{} - NOT SAVED'.format(story['stories_id']))