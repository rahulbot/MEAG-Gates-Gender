import logging
import os
import csv
import copy
import sys

from worker import get_db_client

SINGLE_NAME_GENDERS_CSV = os.path.join('data', 'manually coded single names.csv')

logging.info("Adding single name gender data (from {})".format(SINGLE_NAME_GENDERS_CSV))

db = get_db_client()

processed = db.count_documents({'people_one_name_genders': {'$exists': True}})
logging.info("  {} stories already processed".format(processed))
to_do = db.count_documents({'people_one_name_genders': {'$exists': False}})
logging.info("  {} more to check".format(to_do))
sys.exit()
# load custom gender lookup
accepted_genders = ['male', 'female']  # most ironic variable name ever?
custom_gender_data = csv.DictReader(open(SINGLE_NAME_GENDERS_CSV, 'r', encoding='utf-8-sig'))
gender_lookup = {r['name']: r['gender'].lower() for r in custom_gender_data if r['gender'].lower() in accepted_genders}
logging.info("  {} total manually coded genders (male or female)".format(len(gender_lookup)))

added = 0
for story in db.find({'people_one_name_genders': {'$exists': False}, 'people': {'$exists': True}}):
    singletons = [p for p in story['people'] if p['only_one_name']]
    with_gender = []
    for item in singletons:
        if item['name'] in gender_lookup:  # is  it a single name we've manually coded?
            new_item = copy.deepcopy(item)
            # add in the same things genderize would have otherwise
            new_item['gender'] = gender_lookup[item['name']]
            new_item["probability"] = 1  # we're VERY sure about the manually coded data ;-)
            new_item['country_id'] = story['place']
            with_gender.append(new_item)
            added += 1
    all_people = story['people_with_gender'] + with_gender
    db.update_one({'stories_id': story['stories_id']},
                  {'$set': {
                      'people_one_name_genders': with_gender,
                      'all_gendered_people': all_people,
                  }})

logging.info("Done! Added {} people".format(added))
