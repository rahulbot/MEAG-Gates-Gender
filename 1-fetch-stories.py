import logging
from multiprocessing import Pool
import random
import math
import datetime
import mediacloud.api

from worker.cache import cache
from worker import get_db_client, get_mc_client, places

logging.info("Fetching stories from Media Cloud to save in DB")

collection = get_db_client()

db = get_db_client()

REALLY_INSERT = True
USE_POOL = False  # for bigger queries, it is helped to parallelize the story fetching
MAX_STORIES_PER_PAGE = 1000  # storyList calls don't support more than this

worker_pool = Pool(10)


@cache.cache_on_arguments()
def cached_story_page(q, fq, page_size):
    mc = get_mc_client()
    story_page = mc.storyList(q, fq, sort=mc.SORT_RANDOM, rows=page_size)
    return story_page


def time_period_sample_worker(job):
    fq = mediacloud.api.MediaCloud.publish_date_query(job['start_date'], job['end_date'])
    stories = cached_story_page(job['q'], fq, min(job['sample_size'], 1000))
    return stories


def sample_time_periods(q, sample_size, start_date, end_date, days_per_time_period):
    time_periods_needed = math.floor((end_date - start_date).days / days_per_time_period)
    # juice this a little so we make sure to get more than we need in case one time period
    # has fewer stories
    stories_per_time_period = round(1.3 * math.ceil(sample_size / time_periods_needed))
    jobs = [{
        'q': q,
        'start_date': start_date + datetime.timedelta(days=t*days_per_time_period),
        'end_date': start_date + datetime.timedelta(days=(t+1)*days_per_time_period),
        'sample_size': min(stories_per_time_period, MAX_STORIES_PER_PAGE),
    } for t in range(0, time_periods_needed)]
    # poke the dates to make sure they line up with start and end requested
    jobs[0]['start_date'] = start_date
    jobs[-1]['end_date'] = end_date
    # fetch stories in each of those time periods so we have a random sample
    if USE_POOL:
        stories_by_time_period = worker_pool.map(time_period_sample_worker, jobs)
    else:
        stories_by_time_period = []
        for j in jobs:
            stories = time_period_sample_worker(j)
            stories_by_time_period.append(stories)
            logging.info("      fetched {} stories between {} and {}".format(len(stories), j['start_date'], j['end_date']))
    all_stories = []
    for m in stories_by_time_period:
        all_stories += m
    # with all the rounding we probably over-sampled, so trim it down to the desired amount
    return random.sample(all_stories, sample_size)


def fetch_stories(q, sample_size, start_date, end_date):
    """
    We can't page through random samples, so we have to sample by a time period here to
    ensure representation from the entire corpus.
    """
    days_per_time_period = 7
    return sample_time_periods(q, sample_size, start_date, end_date, days_per_time_period)


total_stories_needed = 0
total_stories_fetched = 0
for p in places:
    logging.info("  Working on {}".format(p['name']))
    logging.info("    {} stories needed".format(p['sample_size']))
    place_query = p['q']
    place_stories_needed = int(p['sample_size'])
    total_stories_needed += place_stories_needed
    # sampling by week, kind of arbitrarily
    place_stories = fetch_stories(place_query, place_stories_needed, p['start_date'], p['end_date'])
    for s in place_stories:
        s['place'] = p['name']
    if REALLY_INSERT:
        db.insert_many(place_stories)
    total_stories_fetched += len(place_stories)

logging.info("Need {} stories, fetched {} stories".format(total_stories_needed, total_stories_fetched))
