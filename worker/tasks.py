from celery.utils.log import get_task_logger

from worker import get_db_client, get_cliff_client, get_genderize_client
from worker.celery import app

logger = get_task_logger(__name__)
SAVE_TO_DB = True  # really save results to the DB?


@app.task(serializer='json', bind=True)
def parse_with_cliff(self, story):
    cliff_results = {}
    people = []
    if 'text' not in story:
        logger.error('{} - no text')
        return
    elif len(story['text']) == 0:
        logger.warning('{} - no chars in text')
        # OK to save the empty list of quotes here because we don't have any text in story
    else:
        cliff = get_cliff_client()
        cliff_results = cliff.parse_text(story['text'])
        people = cliff_results['results']['people']
    # make a local connection to DB, because this is in its own thread
    collection = get_db_client()
    if SAVE_TO_DB:  # write all the quotes to the DB
        collection.update_one({'stories_id': story['stories_id']},
                              {'$set': {'raw_cliff_results': cliff_results}})
        logger.info('{} - Saved results to DB '.format(story['stories_id']))
    else:
        logger.info('{} - NOT SAVED'.format(story['stories_id']))


@app.task(serializer='json', bind=True)
def parse_with_genderize(self, story):
    genderize = get_genderize_client()
    names = [p['name'] for p in story['people']]
    if len(names) > 0:  # don't send it to genderize if there aren't any people
        results = genderize.get(names)
    else:
        results = []
    # make a local connection to DB, because this is in its own thread
    collection = get_db_client()
    if SAVE_TO_DB:  # write all the data to the DB
        collection.update_one({'stories_id': story['stories_id']},
                              {'$set': {
                                  'raw_genderize_results': results,
                              }})
        logger.info('{} - Saved results to DB '.format(story['stories_id']))
    else:
        logger.info('{} - NOT SAVED'.format(story['stories_id']))