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
people_with_gender = 0
male_people = 0
female_people = 0

data = []
K = 'raw_first_name_genderize_results'
name_freq = {}

for s in collection.find({K: {'$exists': True}}):
    total_stories += 1
    if len(s['raw_cliff_results']['results']['people']) == 0:
        stories_no_people += 1
    else:
        total_people += len(s['raw_cliff_results']['results']['people'])
    for p in s[K]:
        if p['name'] not in name_freq:
            name_freq[p['name']] = 0
        name_freq[p['name']] += 1
        if p['gender'] is not None:
            people_with_gender += 1
            if p['gender'] == 'male':
                male_people += 1
            elif p['gender'] == 'female':
                female_people += 1
    summary = {
        'place': s['place'],
        'stories_id': s['stories_id'],
        'p1-name': s[K][0]['name'] if len(s[K]) > 0 else '',
        'p1-gender': s[K][0]['gender'] if len(s[K]) > 0 else '',
        'p1-prob': s[K][0]['probability'] if len(s[K]) > 0 else '',
        'p2-name': s[K][1]['name'] if len(s[K]) > 1 else '',
        'p2-gender': s[K][1]['gender'] if len(s[K]) > 1 else '',
        'p2-prob': s[K][1]['probability'] if len(s[K]) > 1 else '',
        'p3-name': s[K][2]['name'] if len(s[K]) > 2 else '',
        'p3-gender': s[K][2]['gender'] if len(s[K]) > 2 else '',
        'p3-prob': s[K][2]['probability'] if len(s[K]) > 2 else '',
        'p4-name': s[K][3]['name'] if len(s[K]) > 3 else '',
        'p4-gender': s[K][3]['gender'] if len(s[K]) > 3 else '',
        'p4-prob': s[K][3]['probability'] if len(s[K]) > 3 else '',
        'headline': s['title']
    }
    data.append(summary)

print("Total Stories: {}".format(total_stories))
print("  At least one person in headling: {}".format(total_stories - stories_no_people))
print("  No people in headline: {}".format(stories_no_people))
print("Total People: {}".format(total_people))
print("  No gender guess: {}".format(total_people - people_with_gender))
print("  Have a gender guess: {}".format(people_with_gender))
print("    Guessed Male: {}".format(male_people))
print("    Guessed Female: {}".format(female_people))

with open('headline-gender-test1.csv', 'w') as f:
    headers = ['place', 'stories_id',
               'p1-name', 'p1-gender', 'p1-prob',
               'p2-name', 'p2-gender', 'p2-prob',
               'p3-name', 'p3-gender', 'p3-prob',
               'p4-name', 'p4-gender', 'p4-prob',
               'headline',
               ]
    writer = csv.DictWriter(f, headers)
    writer.writeheader()
    for item in data:
        writer.writerow(item)

freq_list = []
for k, v in name_freq.items():
    freq_list.append({'name': k, 'freq': v})
freq_list = sorted(freq_list, key = lambda i: i['freq'], reverse=True)
print(freq_list[:10])
