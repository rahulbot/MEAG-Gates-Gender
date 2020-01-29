import logging
import sys

from worker import get_db_client
from worker.tasks import parse_quotes_to_db

logger = logging.getLogger(__name__)

BATCH_SIZE = 1000
MAX_CHAR_LEN = 90000

collection = get_db_client()


# how many left to do?
total = collection.count_documents({'text': {'$exists': True}})
#unprocessed = collection.count_documents({'quotes': {'$exists': False}, 'text': {'$exists': True}})
unprocessed = collection.count_documents({'annotatedWithQuotes': {'$exists': False}, 'text': {'$exists': True}})
logger.info("Stats:")
logger.info("  {} total".format(total))
logger.info("  {} have quotes".format(total - unprocessed))
logger.info("  {} need quotes".format(unprocessed))
#sys.exit()

# get ​stories with text without quotes from DB
logger.info("Fetching...")
queued = 0
chunk_count = 0
# for story in collection.find({'quotes': {'$exists': False}, 'text': {'$exists': True}}).limit(BATCH_SIZE):
for story in collection.find({'annotatedWithQuotes': {'$exists': False}, 'text': {'$exists': True}}).limit(BATCH_SIZE):
    if len(story['text']) > MAX_CHAR_LEN:
        chunks = [story['text'][i:i + MAX_CHAR_LEN] for i in range(0, len(story['text']), MAX_CHAR_LEN)]
    else:
        chunks = [story['text']]
    for c in chunks:
        parse_quotes_to_db.delay({'stories_id': story['stories_id'], 'text': c})
    # logger.info("  queueing up {} ({} chunks)".format(story['stories_id'], len(chunks)))
    chunk_count += len(chunks)
    queued += 1
logger.info("Queued {} stories (in {} < {} char chunks)".format(queued, chunk_count, MAX_CHAR_LEN))
