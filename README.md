Media Cloud Quote Gender Workflow 
=================================

A workflow to help count people mentioned in quotes and guess their gender (which of course is fraught with
complications and ethical issues).

This assumes you **already have a CSV** file with quote speaker attribution that has been manually reviewed. We 
build this CSV through a different workflow of:

1. pulling stories from [Media Cloud](https://mediacloud.org)
2. running the text through Stanford CoreNLP to detect and attribute quotes (with our [Quote-Annotator](https://github.com/mitmedialab/Quote-Annotator) script)
3. dumping that information to a CSV for manual review and cleanup

Then you bring that CSV into this script to identify gender of speakers [via Genderize.io](https://genderize.io).

Dev Installation
----------------

`pip install -r requirements.txt` - to install the dependencies

Copy the `.env.template` to `.env` and then edit it.


Running the Pipeline:
---------------------

### 1 - Fetching Content

Put your CSV in `data` and edit `worker.DATA_FILE_NAME` with its name.
Run `python 1-fetch-stories.py` to fill the DB with stories from Media Cloud.

### 2 - Adding Gender

Open up one terminal window and start the workers waiting: `celery worker -A worker -l info`. Watch the log to see
if processing stories.

Open up another window and run `python 3-add-entity-gender.py` to fill that queue with tasks.

### 3 - Run a Report

We write these all out to a set of CSVs suitable for analysis in Tableau. Open up a terminal and run 
`python 3-report.py` to generate these CSVs. They will be in a folder named based on the the colleciton 
and version you specified in the environment variables.


Notes
-----

* To empty out your queue of jobs, run `redis-cli FLUSHALL`.
