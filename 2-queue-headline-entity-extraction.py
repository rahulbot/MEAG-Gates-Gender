import logging
import sys

from worker import get_db_client, get_cliff_client
import worker.tasks as tasks

logging.info("Parsing headlines with CLIFF")

collection = get_db_client()

cliff = get_cliff_client()
db = get_db_client()

processed = db.count_documents({'raw_cliff_results': {'$exists': True}})
unprocessed = db.count_documents({'raw_cliff_results': {'$exists': False}})
logging.info("  Need to process {} ({} already done)".format(unprocessed, processed))
sys.exit()
queued = 0

for story in collection.find({'raw_cliff_results': {'$exists': False}}):
    if 'title' not in story:
        logging.error("    No title in story {}".format(story['stories_id']))
    else:
        tasks.parse_with_cliff.delay({'stories_id': story['stories_id'], 'text': story['title']})
        queued += 1

logging.info("Queued {} stories".format(queued))
