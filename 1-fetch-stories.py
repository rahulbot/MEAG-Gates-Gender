import logging
from deco import concurrent, synchronized
import math
from mediacloud.api import MediaCloud
import datetime
import calendar
import random
from multiprocessing import Pool

from worker.cache import cache
from worker import get_db_client, get_mc_client, places

logging.info("Fetching stories from Media Cloud to save in DB")

collection = get_db_client()

db = get_db_client()

REALLY_INSERT = True
USE_POOL = False


@cache.cache_on_arguments()
def cached_story_page(q, fq, page_size):
    mc = get_mc_client()
    story_page = mc.storyList(q, fq, sort=mc.SORT_RANDOM, rows=page_size)
    return story_page


def month_sample_worker(job):
    q = job['q']
    month = job['month']
    page_size = job['page_size']
    start_date = datetime.date(2019, month, 1)
    days_in_month = calendar.monthrange(start_date.year, start_date.month)[1]
    end_date = start_date + datetime.timedelta(days=days_in_month-1)
    fq = MediaCloud.publish_date_query(start_date, end_date)
    stories = cached_story_page(q, fq, page_size)
    return stories

worker_pool = Pool(12)


def sample_all_months(q, total_sample_size):
    jobs = [{'q': q, 'month': month, 'page_size': 200} for month in range(1, 12)]
    if USE_POOL:
        stories_by_month = worker_pool.map(month_sample_worker, jobs)
    else:
        stories_by_month = []
        for month in range(1, 12):
            job = {'q': q, 'month': month, 'page_size': 200}
            logging.info("      month {} started".format(month))
            stories_by_month.append(month_sample_worker(job))
    all_stories = []
    for m in stories_by_month:
        all_stories += m
    random.shuffle(all_stories)
    return all_stories[:total_sample_size]


def fetch_stories(q, sample_size):
    return sample_all_months(q, sample_size)


stories_needed = 0
stories_fetched = 0
for p in places:
    logging.info("  Working on {} ({} sources)".format(p['name'], len(p['sources'])))
    for source in p['sources']:
        logging.info("    Media Id {} - {} stories needed".format(source['media_id'], source['sample_size']))
        query = '* AND media_id:{} AND language:en'.format(source['media_id'])
        stories_needed += int(source['sample_size'])
        if REALLY_INSERT:
            stories = fetch_stories(query, int(source['sample_size']))
            for s in stories:
                s['place'] = p['name']
            db.insert_many(stories)
            stories_fetched += len(stories)

logging.info("Need {} stories, fetched {} stories".format(stories_needed, stories_fetched))
