import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
DWH_ROLE_ARN = 'arn:aws:iam::299535494464:role/dwhRole'
IAM = config['IAM_ROLE']['ARN']
LOG_DATA = config['S3']['LOG_DATA']
SONG_DATA = config['S3']['SONG_DATA']
LOG_JSONPATH = config['S3']['LOG_JSONPATH']


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS _user"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= (""" CREATE TABLE IF NOT EXISTS staging_events(
artist Varchar,
auth Varchar,
firstName Varchar,
gender Varchar,
itemInSession INTEGER,
lastName Varchar,
length float,
level varchar,
location Varchar,
method VARCHAR,
page VARCHAR,
registration BIGINT,
sessionId INTEGER,
song Varchar,
status INTEGER,
ts BIGINT,
userAgent Varchar,
userId INTEGER);
""")

# {"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
staging_songs_table_create = (""" CREATE TABLE IF NOT EXISTS staging_songs(
num_songs INTEGER,
artistid varchar,
latitude Varchar, 
langitude Varchar,
location Varchar,
name Varchar,
songid Varchar,
title Varchar,
duration float,
year INTEGER
);
""")


songplay_table_create = (""" CREATE TABLE IF NOT EXISTS songplay
(songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY, \
                          start_time TIMESTAMP,\
                          user_id INTEGER, \
                          level varchar, \
                          song_id varchar, \
                          artist_id varchar, \
                          session_id INTEGER, \
                          location varchar, \
                          user_agent varchar, \
                          FOREIGN KEY(user_id) References _user, \
                          FOREIGN KEY(song_id) References song, \
                          FOREIGN KEY(artist_id) References artist);""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS _user(
                        user_id INTEGER Primary Key, \
                        first_name varchar, \
                        last_name varchar, \
                        gender varchar, \
                        level varchar);""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS song(
                        song_id varchar Primary Key, \
                        title varchar, \
                        artist_id Varchar, \
                        year INTEGER, \
                        duration float);""")

artist_table_create = (""" CREATE TABLE IF NOT EXISTS artist(
                        artist_id varchar Primary Key, \
                        name varchar, \
                        location varchar, \
                        latitude varchar, \
                        langitude varchar);""")

time_table_create = (""" CREATE TABLE IF NOT EXISTS time(
                     start_time TIMESTAMP PRIMARY KEY, \
                     hour INTEGER, \
                     day INTEGER, \
                     week INTEGER, \
                     month INTEGER, \
                     year INTEGER, \
                     weekday INTEGER);""")

# STAGING TABLES

staging_events_copy = (""" copy staging_events  
                            from {}
                            iam_role {}
                            json {}
""").format(LOG_DATA,IAM, LOG_JSONPATH)

staging_songs_copy = (""" copy staging_songs 
                          from {}
                          iam_role {}
                          json 'auto'
""").format(SONG_DATA, IAM)

# FINAL TABLES
songplay_table_insert = (""" INSERT INTO songplay(start_time,user_id,level,song_id,artist_id,session_id,location,user_agent)
SELECT Distinct TIMESTAMP 'epoch' + (a.ts / 1000) * INTERVAL '1 second' as start_time, a.userid, a.level, b.songid, b.artistid, a.sessionid, b.location, a.userAgent
FROM staging_events a
JOIN staging_songs b ON(a.song = b.title and a.artist = b.name);
""")

user_table_insert = (""" INSERT INTO _user( user_id,first_name,last_name,gender,level)
SELECT userid, firstName, lastName, gender, level 
From staging_events
where userid is NOT NULL
""")

song_table_insert = (""" INSERT into song(song_id,title,artist_id, year, duration)
SELECT songid, title, artistid, year, duration
FROM staging_songs
WHERE songid is NOT NULL;
""")

artist_table_insert = ("""INSERT into artist(artist_id,name ,location ,latitude,langitude)
SELECT artistid, name, location, latitude, langitude
FROM staging_songs
WHERE artistid IS NOT NULL;
""")

time_table_insert = (""" INSERT into time(start_time, hour, day, week, month, year, weekday)
SELECT ts, EXTRACT(hour from ts), 
EXTRACT(day from ts),
EXTRACT(week from ts),
EXTRACT(month from ts),
EXTRACT(year from ts),
EXTRACT(weekday from ts) from
(SELECT TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as ts FROM staging_events) t;
""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]