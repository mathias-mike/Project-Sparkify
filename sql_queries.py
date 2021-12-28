import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

arn = config.read('IAM_ROLE', 'ARN')
log_data = config.read('S3', 'LOG_DATA')
log_json_path = config.read('S3', 'LOG_JSONPATH')
song_data = config.read('S3', 'SONG_DATA')

# DROP TABLES

staging_events_table_drop = 'DROP TABLE IF EXISTS "staging_events"'
staging_songs_table_drop = 'DROP TABLE IF EXISTS "staging_songs"'
songplay_table_drop = 'DROP TABLE IF EXISTS "songplay"'
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
        "artist_location" VARCHAR,
        "artist_longitude" DECIMAL(18,12),
        "artist_name" VARCHAR(MAX),
        "duration" DOUBLE PRECISION, 
        "num_songs" SMALLINT,
        "song_id" VARCHAR,
        "title" VARCHAR,
        "year" SMALLINT
    )
""")

songplay_table_create = ("""
""")

user_table_create = ("""
""")

song_table_create = ("""
""")

artist_table_create = ("""
""")

time_table_create = ("""
""")



# STAGING TABLES

staging_events_copy = ("""
    COPY "staging_events" FROM {}
    CREDENTIALS 'aws_iam_role={}'
    COMPUPDATE OFF REGION 'us-west-2'
    JSON {}
""").format(log_data, arn, log_json_path)

staging_songs_copy = ("""
    COPY "staging_songs" FROM {}
    CREDENTIALS 'aws_iam_role={}'
    COMPUPDATE OFF REGION 'us-west-2'
    JSON 'auto ignorecase'
""").format(song_data, arn)



# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")



# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
