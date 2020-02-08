import logging
import csv

from worker import get_mc_client, get_db_client, get_genderize_client, places, themes_tag_ids
from worker.cache import cache

logging.info("Writing reports")

collection = get_db_client()

genderize = get_genderize_client()
db = get_db_client()

# fetch the top themes across all the stories for each source
'''
@cache.cache_on_arguments()
def top_theme_tags(q: str, fq: str):
    mc = get_mc_client()
    return mc.storyTagCount(q, fq, tag_sets_id=TAG_SET_NYT_THEMES)
top_themes = {}
mc = get_mc_client()
TAG_SET_NYT_THEMES = 1963  # the tag set the top 600 labels from our NYT-corpus-trained model
place2themes = {}
for p in places:
    place_top_themes = top_theme_tags(p['query'], p['date_query'])[:20]
    place_top_theme_ids = [int(t['tags_id']) for t in place_top_themes]
    p['top_themes'] = place_top_themes
    place2themes[p['name']] = place_top_theme_ids
'''

total_stories = 0
stories_no_people = 0
total_people = 0
people_one_name = 0
people_with_multipart_names = 0
people_with_gender = 0
people_missing_gender = 0
male_people = 0
female_people = 0

people_results = []
name_freq = {}
representation = {}
for p in places:
    representation[p['name']] = {}
    for t in p['themes']:
        representation[p['name']][t['tags_id']] = {'male': 0, 'female': 0, 'tag': t, 'stories': 0}

for s in collection.find({'people_with_gender': {'$exists': True}}):
    total_stories += 1
    story_male = 0
    story_female = 0
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
        if p['gender']:
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
                'headline': s['title'].strip(),
            }
            people_results.append(summary)
        else:
            people_missing_gender += 1
    # and track theme stuff
    for t in s['story_tags']:
        if t['tags_id'] in themes_tag_ids:  # if it is a theme of interest
            representation[s['place']][t['tags_id']]['stories'] += 1
            representation[s['place']][t['tags_id']]['male'] += story_male
            representation[s['place']][t['tags_id']]['female'] += story_female


print("Total Stories: {}".format(total_stories))
print("  At least one person in headline: {}".format(total_stories - stories_no_people))
print("  No people in headline: {}".format(stories_no_people))
print("Total People: {}".format(total_people))
print("  Only one name: {}".format(people_one_name))
print("  Multi-part names: {}".format(people_with_multipart_names))
print("    Have a gender guess: {}".format(people_with_gender))
print("      Guessed Male: {}".format(male_people))
print("      Guessed Female: {}".format(female_people))


# most freq one-part names
one_part_names = []
for k, v in name_freq.items():
    one_part_names.append({'name': k, 'frequency': v})
one_part_names = sorted(one_part_names, key=lambda i: i['frequency'], reverse=True)
with open('one-part-names-complete-v5.csv', 'w') as f:
    headers = ['name', 'frequency']
    writer = csv.DictWriter(f, headers)
    writer.writeheader()
    for item in one_part_names:
        writer.writerow(item)

# write list of people gender results for review
with open('headline-gender-complete-v5.csv', 'w') as f:
    headers = ['stories_id', 'name', 'first_name', 'gender_guess', 'gender_prob', 'headline']
    writer = csv.DictWriter(f, headers)
    writer.writeheader()
    for item in people_results:
        writer.writerow(item)

# joinable test
story_writer = csv.DictWriter(open('stories-complete-v5.csv', 'w'),
                              fieldnames=['stories_id', 'publish_date', 'male_count', 'female_count', 'media_id', 'media_name', 'url', 'title', 'place'],
                              extrasaction='ignore')
story_writer.writeheader()
people_writer = csv.DictWriter(open('people-complete-v5.csv', 'w'),
                               fieldnames=['stories_id', 'name', 'gender', 'probability'],
                               extrasaction='ignore')
people_writer.writeheader()
themes_writer = csv.DictWriter(open('themes-complete-v5.csv', 'w'),
                               fieldnames=['stories_id', 'tag', 'tags_id'],
                               extrasaction='ignore')
themes_writer.writeheader()
for s in collection.find({'people_with_gender': {'$exists': True}}):
    s['male_count'] = len([p for p in s['people_with_gender'] if p['gender'] == 'male'])
    s['female_count'] = len([p for p in s['people_with_gender'] if p['gender'] == 'female'])
    story_writer.writerow(s)
    for p in s['people_with_gender']:
        people_writer.writerow({**p, **{'stories_id': s['stories_id']}})
    for t in s['story_tags']:
        if t['tags_id'] in themes_tag_ids:
            themes_writer.writerow({**t, **{'stories_id': s['stories_id']}})
