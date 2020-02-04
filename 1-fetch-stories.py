import logging

from worker.cache import cache
from worker import get_db_client, get_mc_client, places

SAMPLE_SIZE = 100

logging.info("Fetching stories from Media Cloud to save in DB")

collection = get_db_client()

db = get_db_client()


@cache.cache_on_arguments()
def fetch_stories(q: str, fq: str):
    mc = get_mc_client()
    return mc.storyList(q, fq, sort=mc.SORT_RANDOM, rows=SAMPLE_SIZE)


total_stories = 0
for p in places:
    logging.info("  Working on {} ({} sources)".format(p['name'], len(p['sources'])))
    query = p['query']
    stories = fetch_stories(p['query'], p['date_query'])
    for s in stories:
        s['place'] = p['name']
    db.insert_many(stories)
    total_stories += len(stories)

logging.info("Fetch {} stories".format(total_stories))
