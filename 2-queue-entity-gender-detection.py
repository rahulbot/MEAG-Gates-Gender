import logging

from worker import get_db_client, get_genderize_client, places
import worker.tasks as tasks

logging.info("Adding gender data")

genderize = get_genderize_client()
db = get_db_client()

processed = db.count_documents({'people_with_gender': {'$exists': True}})
unprocessed = db.count_documents({'people_with_gender': {'$exists': False}})
logging.info("  Need to add gender to {} ({} already done)".format(unprocessed, processed))

queued = 0
for story in db.find({'people_with_gender': {'$exists': False}}):
#for story in db.find({'stories_id': 1160837786}):
    matching_place = [p for p in places if p['name'] == story['place']][0]
    tasks.parse_with_genderize.delay({
        'stories_id': story['stories_id'],
        'quote': story['quote'],
        'place_iso_code': matching_place['iso_code']
    })
    queued += 1
logging.info("Queued {} stories".format(queued))
