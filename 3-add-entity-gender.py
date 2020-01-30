import logging

from worker import get_db_client, get_genderize_client, places
import worker.tasks as tasks

logging.info("Adding gender data")

collection = get_db_client()

genderize = get_genderize_client()
db = get_db_client()

processed = db.count_documents({'raw_genderize_results': {'$exists': True}})
unprocessed = db.count_documents({'raw_genderize_results': {'$exists': False}})
logging.info("  Need to add gender to {} ({} already done)".format(unprocessed, processed))

for story in collection.find({'raw_genderize_results': {'$exists': False}}):
    matching_place = [p for p in places if p['name'] == story['place']][0]
    tasks.parse_with_genderize.delay({
        'stories_id': story['stories_id'],
        'people': story['raw_cliff_results']['results']['people'],
        'place_iso_code': matching_place['iso_code']
    })
