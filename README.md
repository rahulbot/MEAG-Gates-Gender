Media Cloud Headline Gender Mentions Pipeline
=============================================

A workflow that will fetch stories, add entity mentions, check gender on them. All info saved to a MongoDB.

Dev Installation
----------------

`pip install -r requirements.txt` - to install the dependencies

Copy the ``.env.template` to `.env` and then edit it.


Running the Pipeline:
---------------------

### 1 - Fetching Content

Run `python 1-fetch-stories.py` to fill the DB with stories from Media Cloud.

### 2 - Adding Gender

This uses the [Genderize.io API](https://genderize.io) to identity the gender of people.

Open up one terminal window and start the workers waiting: `celery worker -A worker -l info`. Watch the log to see
if processing stories.

Open up another window and run `python 3-add-entity-gender.py` to fill that queue with tasks.


Notes
-----

* To empty out your queue of jobs, run `redis-cli FLUSHALL`.
