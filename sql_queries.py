import configparser


# Read config file

config = configparser.ConfigParser()
config.read('dwh.cfg')

# Drop tables if they exist

staging_events_table_drop = "drop table if exists staging_events"
staging_songs_table_drop = "drop table if exists staging_songs"
songplay_table_drop = "drop table if exists songplays cascade"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists songs cascade"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table if exists time"

# Create tables

staging_events_table_create= (""" CREATE TABLE IF NOT EXISTS
    staging_events(
        artist varchar(200),
        auth varchar(50),
        firstname varchar(100),
        gender varchar(1),
        iteminsession smallint,
        lastname varchar(100),
        length numeric(10,5),
        level varchar(10),
        location varchar(100),
        method varchar(10),
        page varchar(20),
        registration bigint,
        sessionid smallint,
        song varchar(200),
        status smallint,
        ts bigint,
        useragent varchar(300),
        userid int
    )
""")

staging_songs_table_create = (""" CREATE TABLE IF NOT EXISTS
    staging_songs(
        num_songs int,
        artist_id varchar,
        artist_latitude varchar,
        artist_longitude varchar,
        artist_location varchar,
        artist_name varchar,
        song_id varchar,
        title varchar,
        duration numeric(10,5),
        year smallint
        )
""")

songplay_table_create = (""" CREATE TABLE IF NOT EXISTS
    songplays (
        songplay_id int IDENTITY(0,1) PRIMARY KEY,
        start_time timestamp NOT NULL,
        user_id int NOT NULL REFERENCES users (user_id),
        level varchar,
        song_id varchar NOT NULL REFERENCES songs (song_id),
        artist_id varchar NOT NULL REFERENCES artists (artist_id),
        session_id int,
        location varchar,
        user_agent text
        );
""")

user_table_create = (""" CREATE TABLE IF NOT EXISTS
    users (
        user_id int PRIMARY KEY,
        first_name varchar,
        last_name varchar,
        gender varchar,
        level varchar
        );
""")

song_table_create = (""" CREATE TABLE IF NOT EXISTS
    songs (
        song_id varchar PRIMARY KEY,
        title varchar,
        artist_id varchar NOT NULL REFERENCES artists (artist_id),
        year int,
        duration numeric(10,5)
        );
""")

artist_table_create = (""" CREATE TABLE IF NOT EXISTS
    artists (
        artist_id varchar PRIMARY KEY,
        name varchar,
        location varchar,
        latitude numeric(8,5),
        longitude numeric(8,5)
        );
""")

time_table_create = (""" CREATE TABLE IF NOT EXISTS
    time (
        start_time timestamp PRIMARY KEY,
        hour smallint,
        day smallint,
        week smallint,
        month smallint,
        year smallint,
        weekday smallint
        );
""")

# Create staging tables

staging_events_copy = ("""
    copy staging_events from 's3://udacity-dend/log_data/'
    credentials 'aws_iam_role={}'
    format as json 'auto ignorecase';
""").format(*config['IAM_ROLE'].values())

staging_songs_copy = ("""
    copy staging_songs from 's3://udacity-dend/song_data/'
    credentials 'aws_iam_role={}'
    format as json 'auto';
""").format(*config['IAM_ROLE'].values())


# Load facts and dim tables

# Fetch only 'NextSong' records in fact table
# If song or artist is not found, insert 'None'
songplay_table_insert = (""" 
INSERT INTO songplays
(start_time,  user_id, level, song_id, artist_id,
    session_id, location, user_agent)
SELECT 
  timestamp 'epoch' + ts/1000 * interval '1 second' AS start_time,
  ISNULL(e.userid, 0) as user_id,
  e.level,
  ISNULL(s.song_id, 'None') as song_id,
  ISNULL(s.artist_id, 'None') as artist_id,
  e.sessionid,
  e.location,
  e.useragent
FROM staging_events e
LEFT JOIN staging_songs s
    ON e.artist = s.artist_name
        AND e.song = s.title
        AND e.length = s.duration
        AND e.page = 'NextSong'
""")

# Take latest user details, based on timestamp of records
user_table_insert = (""" 
INSERT INTO users
    (user_id, first_name, last_name, gender, level)
SELECT DISTINCT s.userid, firstname, lastname, gender, level
FROM staging_events s JOIN 
    (   select userid, max(ts) as ts 
        FROM staging_events 
        GROUP BY userid
    )a
    on s.userid = a.userid and s.ts = a.ts
""")

# Fetch song details
song_table_insert = (""" 
INSERT INTO songs
    (song_id, title , artist_id , year , duration)
SELECT distinct song_id, title, artist_id, year, duration
FROM staging_songs
""")

# Take latest change to artist based on num_songs
artist_table_insert = (""" 
INSERT INTO artists
    (artist_id, name, location, latitude, longitude)
SELECT DISTINCT s.artist_id, artist_name, artist_location,
    artist_latitude, artist_longitude
FROM staging_songs s
JOIN (
  SELECT artist_id, max(num_songs) as num_songs
  FROM staging_songs
  GROUP BY artist_id
) a
on s.artist_id = a.artist_id and s.num_songs = a.num_songs
""")

# Fetch timestamp of 'NextSong' records
time_table_insert = (""" 
    INSERT INTO time(start_time, 
        hour, day, week, month, year, weekday)
    SELECT start_time, 
        date_part(hour, start_time) as hour,
        date_part(day, start_time) as day,
        date_part(week, start_time) as week,
        date_part(month, start_time) as month,
        date_part(year, start_time) as year,
        date_part(dw, start_time) as weekday
    FROM (
      SELECT DISTINCT timestamp 'epoch' + ts/1000 * interval '1 second' AS start_time 
      FROM staging_events
      WHERE page = 'NextSong'
    )
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, artist_table_insert, song_table_insert, time_table_insert, songplay_table_insert]


