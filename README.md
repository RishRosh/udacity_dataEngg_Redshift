# Purpose of sparkify database

  The analytics team of a fictitious startup <b>Sparkify</b> wants to analyze the data they've been collecting on songs and user activity on their new music streaming app, in order to understand what songs users are listening to. Their data currently resides in JSON logs for user activity and songs in AWS S3 buckets. This data is to be loaded to Redshift database for querying purposes.<br>
1. Song data: `s3://udacity-dend/song_data`<br>
2. Log data: `s3://udacity-dend/log_data`

### Song Dataset
  The first dataset is a subset of real data from the [MillionSongDataset](https://labrosa.ee.columbia.edu/millionsong/). Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to two files in this dataset.
>`song_data/A/B/C/TRABCEI128F424C983.json` <br>
>`song_data/A/A/B/TRAABJL12903CDCF1A.json`

### Log Dataset
  The second dataset consists of log files in JSON format generated by this [event simulator](https://github.com/Interana/eventsim) based on the songs in the dataset above. These simulate activity logs from a music streaming app based on specified configurations.

  The log files in the dataset are partitioned by year and month. For example, here are filepaths to two files in this dataset.

  >`log_data/2018/11/2018-11-12-events.json`<br>
  >`log_data/2018/11/2018-11-13-events.json`

## Schema for Song Play Analysis
  Using the song and log datasets, a staging and star schema optimized for queries on song play analysis has been created. This includes the following tables.

<b> Staging Tables</b>
1. staging_events - loads the log file data from S3 bucket to Redshift
*artist, auth, firstname, gender, iteminsession, lastname, length, level, location, method, page, registration, sessionid, song, status, ts, useragent, userid*<br>
2. staging_songs - loads the songs data from S3 bucket to Redshift
*num_songs int, artist_id, artist_latitude, artist_longitude, artist_location, artist_name, song_id, title, duration, year*

<b>Fact Table</b>
  1. songplays - records in log data associated with song plays i.e. records with page NextSong <br>
  *songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent*

<b>Dimension Tables</b>
  1. users - users in the app<br>
  *user_id, first_name, last_name, gender, level*
  2. songs - songs in music database<br>
  *song_id, title, artist_id, year, duration*
  3. artists - artists in music database<br>
  *artist_id, name, location, latitude, longitude*
  4. time - timestamps of records in songplays broken down into specific units<br>
  *start_time, hour, day, week, month, year, weekday*

## ETL Pipeline

  The ETL pipeline consists of 2 functions:

  1. `load_staging_tables(cur, conn)`:
    *Function to process song and log file data to staging database. Takes cursor and connection as inputs.*
  2. `insert_tables(cur, conn)`:
    *Function to insert data in to fact and dim tables from staging tables. Takes cursor and connection as inputs.*

  The all the SQL statements for creating and loading tables are in `sql_queries.py`.
  
<b> How to run the project pipeline</b>

    1. Run `create_tables.py` to create the database and tables of star and staging schema. 
    2. Run `etl.py` to load the star schema with log files
    
    