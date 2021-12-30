import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

arn = config.get('IAM_ROLE', 'ARN')
log_data = config.get('S3', 'LOG_DATA')
log_json_path = config.get('S3', 'LOG_JSONPATH')
song_data = config.get('S3', 'SONG_DATA')


# DROP TABLES
staging_events_table_drop = 'DROP TABLE IF EXISTS "staging_events"'
staging_songs_table_drop = 'DROP TABLE IF EXISTS "staging_songs"'
songplay_table_drop = 'DROP TABLE IF EXISTS "songplays"'
user_table_drop = 'DROP TABLE IF EXISTS "users"'
song_table_drop = 'DROP TABLE IF EXISTS "songs"'
artist_table_drop = 'DROP TABLE IF EXISTS "artists"'
time_table_drop = 'DROP TABLE IF EXISTS "time"'



# CREATE TABLES
staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS "staging_events" (
        "artist" VARCHAR,
        "auth" VARCHAR,
        "firstName" VARCHAR,
        "gender" VARCHAR,
        "itemInSession" SMALLINT,
        "lastName" VARCHAR,
        "length" REAL,
        "level" VARCHAR,
        "location" VARCHAR,
        "method" VARCHAR,
        "page" VARCHAR,
        "registration" DOUBLE PRECISION,
        "sessionId" SMALLINT,
        "song" VARCHAR,
        "status" SMALLINT,
        "ts" BIGINT,
        "userAgent" VARCHAR,
        "userId" SMALLINT
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS "staging_songs" (
        "artist_id" VARCHAR,
        "artist_latitude" DECIMAL(18,12),
        "artist_location" VARCHAR(MAX),
        "artist_longitude" DECIMAL(18,12),
        "artist_name" VARCHAR(MAX),
        "duration" DOUBLE PRECISION, 
        "num_songs" SMALLINT,
        "song_id" VARCHAR,
        "title" VARCHAR(MAX),
        "year" SMALLINT
    )
""")

# FACT TABLE
songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS "songplays" (
        "songplay_id" INT IDENTITY(0, 1),
        "start_time" BIGINT NOT NULL SORTKEY,
        "user_id" SMALLINT NOT NULL,
        "level" VARCHAR(5) NOT NULL,
        "song_id" VARCHAR(20) NOT NULL DISTKEY,
        "artist_id" VARCHAR(20) NOT NULL,
        "session_id" SMALLINT NOT NULL,
        "location" VARCHAR NOT NULL,
        "user_agent" VARCHAR NOT NULL
    )
""")

# DIMENSION TABLES
user_table_create = ("""
    CREATE TABLE IF NOT EXISTS "users" (
        "user_id" SMALLINT NOT NULL SORTKEY,
        "first_name" VARCHAR NOT NULL,
        "last_name" VARCHAR NOT NULL,
        "gender" VARCHAR(2) NOT NULL,
        "level" VARCHAR(5) NOT NULL
    )
    DISTSTYLE ALL
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS "songs" (
        "song_id" VARCHAR(20) NOT NULL SORTKEY DISTKEY,
        "title" VARCHAR(MAX) NOT NULL,
        "artist_id" VARCHAR(20) NOT NULL,
        "year" SMALLINT NOT NULL,
        "duration" DOUBLE PRECISION NOT NULL
    )
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS "artists" (
        "artist_id" VARCHAR(20) NOT NULL SORTKEY,
        "name" VARCHAR(MAX) NOT NULL,
        "location" VARCHAR(MAX),
        "latitude" DECIMAL(18, 12),
        "longitude" DECIMAL(18, 12)
    )
    DISTSTYLE ALL
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS "time" (
        start_time BIGINT NOT NULL SORTKEY,
        hour SMALLINT NOT NULL,
        day SMALLINT NOT NULL,
        week SMALLINT NOT NULL,
        month SMALLINT NOT NULL,
        year SMALLINT NOT NULL,
        weekday SMALLINT NOT NULL
    )
    DISTSTYLE ALL
""")



# STAGING TABLES FROM JSON
# staging_events_copy = ("""
#     COPY "staging_events" FROM {}
#     CREDENTIALS 'aws_iam_role={}'
#     COMPUPDATE OFF REGION 'us-west-2'
#     JSON {}
# """).format(log_data, arn, log_json_path)

# staging_songs_copy = ("""
#     COPY "staging_songs" FROM {}
#     CREDENTIALS 'aws_iam_role={}'
#     COMPUPDATE OFF REGION 'us-west-2'
#     JSON 'auto ignorecase'
# """).format(song_data, arn)

# STAGING TABLES FROM CSV
staging_events_copy = ("""
    COPY "staging_events" (artist,auth,firstname,gender,iteminsession,lastname,length,level,location,method,page,registration,sessionid,song,status,ts,useragent,userid) 
    FROM 's3://sparkify-dwh-bucket/log-data/'
    CREDENTIALS 'aws_iam_role={}'
    COMPUPDATE OFF REGION 'us-west-2'
    GZIP DELIMITER ',' IGNOREHEADER 1
""").format(arn)

staging_songs_copy = ("""
    COPY "staging_songs" (artist_id,artist_latitude,artist_location,artist_longitude,artist_name,duration,num_songs,song_id,title,year)
    FROM 's3://sparkify-dwh-bucket/song-data/'
    CREDENTIALS 'aws_iam_role={}'
    COMPUPDATE OFF REGION 'us-west-2'
    GZIP DELIMITER ',' IGNOREHEADER 1
""").format(arn)



# FINAL TABLES
songplay_table_insert = ("""
    INSERT INTO "songplays" ("start_time", "user_id", "level", "song_id", 
                            "artist_id", "session_id", "location", "user_agent")
    SELECT e.ts AS start_time, e.userid AS user_id, e.level AS level, s.song_id AS song_id, 
        s.artist_id AS artist_id, e.sessionid AS session_id, e.location AS location, e.useragent AS user_agent
    FROM staging_events e
    JOIN staging_songs s
    ON e.song = s.title AND e.artist = s.artist_name
    WHERE e.page = 'NextSong'
""")

user_table_insert = ("""
    INSERT INTO "users" ("user_id", "first_name", "last_name", "gender", "level")
    SELECT e.userid AS user_id, e.firstname AS first_name, e.lastname AS last_name, e.gender AS gender, e.level AS level
    FROM staging_events e
    JOIN (
        SELECT userid, MAX(ts) max_ts
        FROM staging_events
        GROUP BY 1
        HAVING userid IS NOT NULL
    ) AS unique_users
    ON e.userid = unique_users.userid 
    AND e.ts = unique_users.max_ts
""")

song_table_insert = ("""
    INSERT INTO "songs" ("song_id", "title", "artist_id", "year", "duration")
    SELECT song_id, title, artist_id, year, duration
    FROM staging_songs
""")

artist_table_insert = ("""
    INSERT INTO "artists" ("artist_id", "name", "location", "latitude", "longitude")
    SELECT artist_id, name, location, latitude, longitude
    FROM (
        SELECT artist_id, artist_name AS name, artist_location AS location, artist_latitude AS latitude, 
            artist_longitude AS longitude, ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY song_id) row_number
        FROM staging_songs
    ) AS t1
    WHERE row_number = 1
""")

time_table_insert = ("""
    INSERT INTO "time" ("start_time", "hour", "day", "week", "month", "year", "weekday")
    SELECT DISTINCT ts AS start_time,
            DATE_PART('hour', TIMESTAMP 'epoch' + (ts/1000) * INTERVAL '1 second') AS hour,
            DATE_PART('day', TIMESTAMP 'epoch' + (ts/1000) * INTERVAL '1 second') AS day,
            DATE_PART('week', TIMESTAMP 'epoch' + (ts/1000) * INTERVAL '1 second') AS week,
            DATE_PART('month', TIMESTAMP 'epoch' + (ts/1000) * INTERVAL '1 second') AS month,
            DATE_PART('year', TIMESTAMP 'epoch' + (ts/1000) * INTERVAL '1 second') AS year,
            DATE_PART('weekday', TIMESTAMP 'epoch' + (ts/1000) * INTERVAL '1 second') AS weekday
    FROM staging_events
""")



# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
