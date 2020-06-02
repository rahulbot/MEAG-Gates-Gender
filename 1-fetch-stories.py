import logging
from multiprocessing import Pool

from worker.cache import cache
from worker import get_db_client, get_mc_client, places, data

logger = logging.getLogger(__name__)

logger.info("Fetching stories from Media Cloud to save in DB")

REALLY_INSERT = True
PAGE_SIZE = 300
POOL_SIZE = 5


@cache.cache_on_arguments()
def cached_story_page1(q, page_size):
    mc = get_mc_client()
    story_page = mc.storyList(q, rows=page_size)
    return story_page


def story_fetch_worker(job):
    q = job['query']
    stories = cached_story_page1(q, PAGE_SIZE)
    logger.info("  got a chunk of data back {}".format(len(stories)))
    for s in stories:
        matching = [e for e in job['stories'] if int(e['stories_id']) == int(s['stories_id'])][0]
        s['place'] = matching['place']
        s['quote'] = matching
    logger.info("  done with place addition")
    if REALLY_INSERT:
        db = get_db_client()
        db.insert_many(stories)
    return len(stories)


worker_pool = Pool(POOL_SIZE)

chunks = [data[i * PAGE_SIZE:(i + 1) * PAGE_SIZE] for i in range((len(data) + PAGE_SIZE - 1) // PAGE_SIZE)]
logger.info("  will fetch in {:n} chunks".format(len(chunks)))

jobs = []
for story_list in chunks:
    story_ids = [s['stories_id'] for s in story_list]
    jobs.append({
        'query': "stories_id:({})".format(' '.join(story_ids)),
        'stories': story_list,
    })
logger.info("  queued up {:n} jobs".format(len(chunks)))
results = worker_pool.map(story_fetch_worker, jobs)

logger.info("Done with {:n} stories".format(sum(results)))
