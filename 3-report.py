import logging
import csv

from worker import get_mc_client, get_db_client, get_genderize_client, places, themes_tag_ids
from worker.cache import cache

VERSION = 1

logging.info("Writing reports")

collection = get_db_client()

genderize = get_genderize_client()
db = get_db_client()

total_stories = 0
stories_no_people = 0
total_people = 0
people_one_name = 0
people_with_multipart_names = 0
people_with_gender = 0
male_people = 0
female_people = 0
unknown_people = 0

people_results = []
name_freq = {}
representation = {}
for p in places:
    representation[p['name']] = {}
    for t in p['themes']:
        representation[p['name']][t['tags_id']] = {'male': 0, 'female': 0, 'unknown': 0, 'tag': t, 'stories': 0}

for s in collection.find({'people_with_gender': {'$exists': True}}):
    if len(s['people_with_gender']) == 0:
        continue
    total_stories += 1
    story_male = 0
    story_female = 0
    story_unknown = 0
    if len(s['people']) == 0:
        stories_no_people += 1
    else:
        total_people += len(s['people'])
    for p in s['people']:
        if p['only_one_name']:
            people_one_name += 1
            if p['name'] not in name_freq:
                name_freq[p['name']] = 0
            name_freq[p['name']] += 1
        else:
            people_with_multipart_names += 1
    for p in s['people_with_gender']:
        if p['gender'] is not  None:
            people_with_gender += 1
            if p['gender'] == 'male':
                male_people += 1
                story_male += 1
            elif p['gender'] == 'female':
                female_people += 1
                story_female += 1
            summary = {
                'stories_id': s['stories_id'],
                'name': p['name'],
                'first_name': p['first_name'],
                'gender_guess': p['gender'],
                'gender_prob': p['probability'],
            }
            people_results.append(summary)
        else:
            unknown_people += 1
            story_unknown += 1
    # and track theme stuff
    for t in s['story_tags']:
        if t['tags_id'] in themes_tag_ids:  # if it is a theme of interest
            representation[s['place']][t['tags_id']]['stories'] += 1
            representation[s['place']][t['tags_id']]['male'] += story_male
            representation[s['place']][t['tags_id']]['female'] += story_female
            representation[s['place']][t['tags_id']]['unknown'] += story_unknown


print("Total Stories: {}".format(total_stories))
print("Total People: {}".format(total_people))
print("  Only one name: {}".format(people_one_name))
print("  Multi-part names: {}".format(people_with_multipart_names))
print("    No gender guess: {}".format(unknown_people))
print("    Have a gender guess: {}".format(people_with_gender))
print("      Guessed Male: {}".format(male_people))
print("      Guessed Female: {}".format(female_people))


# most freq one-part names
one_part_names = []
for k, v in name_freq.items():
    one_part_names.append({'name': k, 'frequency': v})
one_part_names = sorted(one_part_names, key=lambda i: i['frequency'], reverse=True)
with open('data-v{}/one-part-names-complete-v{}.csv'.format(VERSION, VERSION), 'w') as f:
    headers = ['name', 'frequency']
    writer = csv.DictWriter(f, headers)
    writer.writeheader()
    for item in one_part_names:
        writer.writerow(item)

# write list of people gender results for review
with open('data-v{}/quote-gender-complete-v{}.csv'.format(VERSION, VERSION), 'w') as f:
    headers = ['stories_id', 'name', 'first_name', 'gender_guess', 'gender_prob']
    writer = csv.DictWriter(f, headers)
    writer.writeheader()
    for item in people_results:
        writer.writerow(item)

# joinable test
story_writer = csv.DictWriter(open('data-v{}/stories-complete-v{}.csv'.format(VERSION, VERSION), 'w'),
                              fieldnames=['stories_id', 'publish_date', 'has_any?', 'male_count', 'has_male?',
                                          'female_count', 'has_female?', 'unknown_count', 'has_unknown?',
                                          'media_id', 'media_name', 'url', 'title', 'place'],
                              extrasaction='ignore')
story_writer.writeheader()
people_writer = csv.DictWriter(open('data-v{}/people-complete-v{}.csv'.format(VERSION, VERSION), 'w'),
                               fieldnames=['stories_id', 'name', 'gender', 'probability'],
                               extrasaction='ignore')
people_writer.writeheader()
themes_writer = csv.DictWriter(open('data-v{}/themes-complete-v{}.csv'.format(VERSION, VERSION), 'w'),
                               fieldnames=['stories_id', 'tag', 'tags_id'],
                               extrasaction='ignore')
themes_writer.writeheader()
for s in collection.find({'all_gendered_people': {'$exists': True}}):
    s['has_any?'] = 1 if len(s['people_with_gender']) > 0 else 0
    s['male_count'] = len([p for p in s['people_with_gender'] if p['gender'] == 'male'])
    s['has_male?'] = 1 if len([p for p in s['people_with_gender'] if p['gender'] == 'male']) > 0 else 0
    s['female_count'] = len([p for p in s['people_with_gender'] if p['gender'] == 'female'])
    s['has_female?'] = 1 if len([p for p in s['people_with_gender'] if p['gender'] == 'female']) > 0 else 0
    s['unknown_count'] = len([p for p in s['people_with_gender'] if p['gender'] is None])
    s['has_unknown?'] = 1 if len([p for p in s['people_with_gender'] if p['gender'] is None]) > 0 else 0
    story_writer.writerow(s)
    for p in s['all_gendered_people']:
        people_writer.writerow({**p, **{'stories_id': s['stories_id']}})
    for t in s['story_tags']:
        if t['tags_id'] in themes_tag_ids:
            themes_writer.writerow({**t, **{'stories_id': s['stories_id']}})
