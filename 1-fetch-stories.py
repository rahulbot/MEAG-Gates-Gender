import logging
import datetime

from worker import get_db_client, get_mc_client, places

SAMPLE_SIZE = 100

logging.info("Fetching stories from Media Cloud to save in DB")

collection = get_db_client()

mc = get_mc_client()
db = get_db_client()

total_stories = 0
for p in places:
    logging.info("  Working on {} ({} sources)".format(p['name'], len(p['sources'])))
    stories = mc.storyList("media_id:({}) AND language:en".format(" ".join([str(id) for id in p['sources']])),
                           mc.publish_date_query( datetime.date(2019,1,1), datetime.date(2019,12,31)),
                           sort=mc.SORT_RANDOM, rows=SAMPLE_SIZE)
    for s in stories:
        s['place'] = p['name']
    db.insert_many(stories)
    total_stories += len(stories)

logging.info("Fetch {} stories".format(total_stories))
