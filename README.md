
# IMPORTANT NOTICE
 1. Please, replace all connection strings in create.py, etl.py,  etl.ipynb, and test.ipynb with your connection strings.

 2. The name of my default PostgreSQL DB is default_db. You can supply your default DB in the docker-compose file.

3. Replace the usernames with your Postgresql username, especially in the test.ipynb connection string.

# Business Problem
Sparkify is looking for an efficient way to understand its users' listening habits from JSON logs of user activity.

# Business Objective
Design and create a Postgres DB using JSON logs to optimize song analysis queries for business users.

# Data Description
The base dataset is the [Million Song Dataset](http://millionsongdataset.com), a freely-available collection of a million contemporary popular music tracks. The core of the dataset is the **audio feature** (does not include audio) analysis and **metadata** for a million songs.

The *song data folder* in the **data** folder is a subset of the real dataset from [Million Song Dataset](http://millionsongdataset.com). The song data file contains metadata about a song and the artist of the song in a JSON format. The files are partitioned by the first three letters of each song's track ID. For example, here are file paths to two files in this dataset.

> song_data/A/B/C/TRABCEI128F424C983.json
>
> song_data/A/A/B/TRAABJL12903CDCF1A.json

Below is an example of what a single song file, TRAABJL12903CDCF1A.json, looks like.

> {"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}


The song data was used as input into [Eventsim](https://github.com/Interana/eventsim), a program designed to replicate page requests for a fake music website. The simulator generated log files of user activity in JSON format based on fake user(s) page requests for a fake music streaming app, which serves the songs in the song datasets.

The log files are partitioned by year and month. For example, here are filepaths to two files in this dataset.

> log_data/2018/11/2018-11-12-events.json
>
> log_data/2018/11/2018-11-13-events.json

[Pandas](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html) library can be used to  view the JSON log file as follows:

```python
import pandas as pd
filepath = 'data/log_data/2018/11/2018-11-01-events.json'
df = pd.read_json(filepath, lines=True)
```

# Other Files
In addition to the data files, the project workspace includes six files:

1. **sql_queries.py:** contains all your SQL queries and is imported into the following three files below.

2. **create_tables.py**: drops and creates your tables. You run this file to reset your tables each time you run your ETL scripts.

3. **etl.ipynb:** Reads and processes a single file from song_data and log_data and loads the data into the DB tables. This notebook contains detailed instructions on the ETL process for each table.

4. **etl.py:** reads and processes files from song_data and log_data and loads them into your tables. You can fill this out based on your work in the ETL notebook.

5. **test.ipynb**: displays the first few rows of each table to check the database created in step (2)

6. **README.md:** provides discussion on your project.

# How to buid the ETL Pipeline, Processes, and Tests

## ETL Processses
 - The ETL processes for each table is defined in etl.ipynb notebook
 - Run the test.ipynb after running the etl.ipynb notebbok to confirm that records were succefully inserted into ech table. Rerun create_tables.py to reset your tables before you run the test.ipynb notebook.

 ## ETL Pipeline
 - The etl.py file contains the ETL process for the entire dataset. Remember to run create_tables.py before running etl.py to reset your tables. Run test.ipynb to confirm your records were successfully inserted into each table.

 ## Run Sanity Test
 When you are satisfied with your work, run the cell under the Sanity Tests section in the test.ipynb notebook. The cells contain some basic tests that will evaluate your work and catch any silly mistakes. The notebook tests for column data types, primary key constraints and not-null constraints as well look for on-conflict clauses wherever required. If any of the test cases catches a problem, you will see a warning message printed in Orange that looks like this:

 > [WARNING] The songplays table does not have a primary key!
