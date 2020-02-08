import logging
import sys

from worker import get_db_client, get_genderize_client, places
import worker.tasks as tasks

logging.info("Adding gender data")

collection = get_db_client()

genderize = get_genderize_client()
db = get_db_client()

processed = db.find({'people': {'$exists': True}})
total_people = 0
total_stories = 0
for item in processed:
    total_people += len(item['people'])
    total_stories += 1
logging.info("  Total {} people in headlines of {} stories".format(total_people, total_stories))

processed = db.count_documents({'people_with_gender': {'$exists': True}})
unprocessed = db.count_documents({'people_with_gender': {'$exists': False}})
logging.info("  Need to add gender to {} ({} already done)".format(unprocessed, processed))

queued = 0
for story in collection.find({'people_with_gender': {'$exists': False}}):
    matching_place = [p for p in places if p['name'] == story['place']][0]
    tasks.parse_with_genderize.delay({
        'stories_id': story['stories_id'],
        'people': story['people'],
        'place_iso_code': matching_place['iso_code']
    })
    queued += 1
logging.info("Queued {} stories".format(queued))
