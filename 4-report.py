import logging
import csv

from worker import get_db_client, get_genderize_client

logging.info("Adding gender data")

collection = get_db_client()

genderize = get_genderize_client()
db = get_db_client()

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

for s in collection.find():
    total_stories += 1
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
            elif p['gender'] == 'female':
                female_people += 1
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
