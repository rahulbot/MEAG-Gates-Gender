Media Cloud Headline Gender Mentions Pipeline
=============================================

A workflow to count people mentioned in headlines and guess their gender (which of course is fraught with complications
and ethical issues). This:
1. starts with a list of story ids from Media Cloud (a sample you generate somehow)
2. fetches the story headlines and other metadata and saves it into a MongoDB
3. runs those headlines through CLIFF-CLAVIN to find any person entities
4. runs those people's names through Genderize to guess their gender (with a country of origin hint)
5. outputs a whole bunch of spreadsheets breaking this down by gender, themes, country, and more for you to analyze

Dev Installation
----------------

`pip install -r requirements.txt` - to install the dependencies

Copy the ``.env.template` to `.env` and then edit it.


Running the Pipeline:
---------------------

### 1 - Fetching Content

Run `python 1-fetch-stories.py` to fill the DB with stories from Media Cloud.

### 2 - Adding Entities

This uses our [Cliff-Clavin server](http://cliff.mediacloud.org) to identity any mentions of people in the headline.

Open up one terminal window and start the workers waiting: `celery worker -A worker -l info`. Watch the log to see
if processing stories.

Open up another window and run `python 2-add-headline-entities.py` to fill that queue with tasks.

### 3 - Adding Entities

This uses the [Genderize.io API](https://genderize.io) to identity the gender of people.

Open up one terminal window and start the workers waiting: `celery worker -A worker -l info`. Watch the log to see
if processing stories.

Open up another window and run `python 3-add-entity-gender.py` to fill that queue with tasks.


Notes
-----

* To empty out your queue of jobs, run `redis-cli FLUSHALL`.
