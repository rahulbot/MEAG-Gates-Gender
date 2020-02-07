import logging
from deco import concurrent, synchronized
import math

from worker.cache import cache
from worker import get_db_client, get_mc_client, places

logging.info("Fetching stories from Media Cloud to save in DB")

collection = get_db_client()

db = get_db_client()

REALLY_INSERT = True
STORIES_PER_PAGE = 50


@cache.cache_on_arguments()
def cached_story_page(q, fq, last_processed_stories_id):
    mc = get_mc_client()
    story_page = mc.storyList(q, fq, sort=mc.SORT_RANDOM, rows=STORIES_PER_PAGE, last_processed_stories_id=last_processed_stories_id)
    return story_page



def month_sample(q, month, sample_size):



def sample_all_months(q, total_sample_size):
    stories_by_month = {}
    for month in range(1,12):
        stories_by_month[month] = month_sample(q, 200)




def sample_for_source(q, fq, sample_size):
    last_id = 0
    story_list = []
    story_id_list = []
    while len(story_list) < sample_size:
        page = cached_story_page(q, fq, last_id)
        logging.info("    page")
        for s in page:
            if s['stories_id'] not in story_id_list:
                story_id_list.append(s['stories_id'])
                story_list.append(s)
        last_id = page[-1]['processed_stories_id']
    return story_list


def fetch_stories(q, fq, sample_size):
    return sample_for_source(q, fq, sample_size)


stories_needed = 0
stories_fetched = 0
for p in places:
    logging.info("  Working on {} ({} sources)".format(p['name'], len(p['sources'])))
    for source in p['sources']:
        logging.info("    Media Id {} - {} stories needed".format(source['media_id'], source['sample_size']))
        query = '* AND media_id:{} AND language:en'.format(source['media_id'])
        stories_needed += int(source['sample_size'])
        if REALLY_INSERT:
            stories = fetch_stories(query, p['date_query'], 5)[:5]
            for s in stories:
                s['place'] = p['name']
            db.insert_many(stories)
            stories_fetched += len(stories)

logging.info("Need {} stories, fetched {} stories".format(stories_needed, stories_fetched))
