import logging
import csv

from worker import get_mc_client, get_db_client, get_genderize_client, places
from worker.cache import cache

logging.info("Writing reports")

collection = get_db_client()

genderize = get_genderize_client()
db = get_db_client()


@cache.cache_on_arguments()
def top_theme_tags(q: str, fq: str):
    mc = get_mc_client()
    return mc.storyTagCount(q, fq, tag_sets_id=TAG_SET_NYT_THEMES)


# fetch the top themes across all the stories for each source
top_themes = {}
mc = get_mc_client()
TAG_SET_NYT_THEMES = 1963  # the tag set the top 600 labels from our NYT-corpus-trained model
place2themes = {}
for p in places:
    place_top_themes = top_theme_tags(p['query'], p['date_query'])[:20]
    place_top_theme_ids = [int(t['tags_id']) for t in place_top_themes]
    p['top_themes'] = place_top_themes
    place2themes[p['name']] = place_top_theme_ids

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
    for t in p['top_themes']:
        representation[p['name']][t['tags_id']] = {'male': 0, 'female': 0, 'tag': t, 'stories': 0}

for s in collection.find():
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
        if t['tags_id'] in place2themes[s['place']]:  # if it is a top theme
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
with open('one-part-names-v2.csv', 'w') as f:
    headers = ['name', 'frequency']
    writer = csv.DictWriter(f, headers)
    writer.writeheader()
    for item in one_part_names:
        writer.writerow(item)

# write list of people gender results for review
with open('headline-gender-test-v2.csv', 'w') as f:
    headers = ['stories_id', 'name', 'first_name', 'gender_guess', 'gender_prob', 'headline']
    writer = csv.DictWriter(f, headers)
    writer.writeheader()
    for item in people_results:
        writer.writerow(item)

# top themes in sources + a list of 10 that they want to make sure are included
rows = []
for place_name in representation.keys():
    for tag_info in representation[place_name].values():
        row = {
            'place': place_name,
            'theme_tags_id': tag_info['tag']['tags_id'],
            'theme': tag_info['tag']['tag'],
            'stories_with_tag': tag_info['stories'],
            'female_headline_mentions': tag_info['female'],
            'male_headline_mentions': tag_info['male'],
        }
        rows.append(row)
with open('headline-gender-themes-v2.csv', 'w') as f:
    headers = ['place', 'theme_tags_id', 'theme', 'stories_with_tag', 'female_headline_mentions', 'male_headline_mentions']
    writer = csv.DictWriter(f, headers)
    writer.writeheader()
    for item in rows:
        writer.writerow(item)

