from celery.utils.log import get_task_logger
from typing import Dict, List
import copy
import json

from worker import get_db_client, get_genderize_client, replacement_lookup
from worker.celery import app
from worker.cache import cache

logger = get_task_logger(__name__)
SAVE_TO_DB = True  # really save results to the DB?


@cache.cache_on_arguments()
def fetch_genderize_results(names: List[str], country_id) -> List[Dict]:
    genderize = get_genderize_client()
    results = genderize.get(names, country_id=country_id)
    return results


@app.task(serializer='json', bind=True)
def parse_with_genderize(self, story):
    collection = get_db_client()
    needs_gender = False
    # figure out what we know already
    speaker = story['quote']['Speaker']
    if speaker.lower() in replacement_lookup:
        speaker = replacement_lookup[speaker.lower()]
    name_parts = speaker.split()
    only_one_name = len(name_parts) == 1
    person = {'name': speaker}
    person['name_parts'] = name_parts
    person['first_name'] = name_parts[0] if not only_one_name else None
    person['last_name'] = " ".join(name_parts[1:]) if not only_one_name else person['name']
    person['only_one_name'] = only_one_name
    person['multi_part_name'] = len(name_parts) > 1
    starts_with_ms_mr = speaker[:2].lower() in ['mr', 'ms']
    person['starts_with_ms_mr'] = starts_with_ms_mr
    is_pronoun = speaker.lower() in ['he', 'she']
    starts_with_a_or_the = (speaker[:1].lower() in ['a ']) or (speaker[:2].lower() in ['the'])
    person['starts_with_a_or_the'] = starts_with_a_or_the
    person['is_pronoun'] = is_pronoun
    people = [person]
    # skip genderize if this has been manually gender coded already
    people_with_gender = None
    if starts_with_ms_mr or is_pronoun:
        people_with_gender = copy.deepcopy(people)
        for p in people_with_gender:
            gender = None
            if starts_with_ms_mr:
                if p['name'][:2].lower() == 'ms':
                    gender = 'female'
                elif p['name'][:2].lower() == 'mr':
                    gender = 'male'
            elif is_pronoun:
                if p['name'].lower() == 'he':
                    gender = 'male'
                elif p['name'].lower() == 'she':
                    gender = 'female'
            p['gender'] = gender
            p['probability'] = 1
            p['country_id'] = story['place_iso_code'].upper()
            needs_gender = gender is None
            logger.info(json.dumps(p))
    elif starts_with_a_or_the:
        # don't run things like "a source" through genderize
        people_with_gender = []
    else:
        needs_gender = True
    if SAVE_TO_DB:  # write all the quotes to the DB
        data_to_save = {'people': people}
        if not needs_gender:
            data_to_save['people_with_gender'] = people_with_gender
        collection.update_one({'stories_id': story['stories_id']},
                              {'$set': data_to_save})
        logger.info('{} - Saved results to DB '.format(story['stories_id']))
    else:
        logger.info('{} - NOT SAVED'.format(story['stories_id']))
    # and add genderize data as needed
    if needs_gender:
        people_with_first_names = [p for p in people if p['multi_part_name']]
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
        if SAVE_TO_DB:  # write all the data to the DB
            collection.update_one({'stories_id': story['stories_id']},
                                  {'$set': {'people_with_gender': people_with_gender}})
            logger.info('{} - Saved results to DB '.format(story['stories_id']))
        else:
            logger.info('{} - NOT SAVED'.format(story['stories_id']))
