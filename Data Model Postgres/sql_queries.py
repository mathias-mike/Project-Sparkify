# DROP TABLES
songplay_table_drop = 'DROP TABLE IF EXISTS "songplays"'
user_table_drop = 'DROP TABLE IF EXISTS "users"'
song_table_drop = 'DROP TABLE IF EXISTS "songs"'
artist_table_drop = 'DROP TABLE IF EXISTS "artists"'
time_table_drop = 'DROP TABLE IF EXISTS "time"'



# CREATE TABLES
songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS "songplays" (
        "songplay_id" SERIAL PRIMARY KEY,
        "start_time" TIMESTAMP REFERENCES "time" ("start_time"),
        "user_id" SMALLINT REFERENCES "users" ("user_id"),
        "level" VARCHAR(5),
        "song_id" VARCHAR(20) REFERENCES "songs" ("song_id"),
        "artist_id" VARCHAR(20) REFERENCES "artists" ("artist_id"),
        "session_id" SMALLINT,
        "location" VARCHAR,
        "user_agent" VARCHAR
    )
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS "users" (
        "user_id" SMALLINT NOT NULL PRIMARY KEY,
        "first_name" VARCHAR NOT NULL,
        "last_name" VARCHAR NOT NULL,
        "gender" VARCHAR(2) NOT NULL,
        "level" VARCHAR(5) NOT NULL
    )
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS "songs" (
        "song_id" VARCHAR(20) NOT NULL PRIMARY KEY,
        "title" TEXT NOT NULL,
        "artist_id" VARCHAR(20) NOT NULL,
        "year" SMALLINT NOT NULL,
        "duration" DOUBLE PRECISION NOT NULL
    )
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS "artists" (
        "artist_id" VARCHAR(20) NOT NULL PRIMARY KEY,
        "name" TEXT NOT NULL,
        "location" TEXT,
        "latitude" DECIMAL(18, 12),
        "longitude" DECIMAL(18, 12)
    )
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS "time" (
        start_time TIMESTAMP NOT NULL PRIMARY KEY,
        hour SMALLINT NOT NULL,
        day SMALLINT NOT NULL,
        week SMALLINT NOT NULL,
        month SMALLINT NOT NULL,
        year SMALLINT NOT NULL,
        weekday SMALLINT NOT NULL
    )
""")


# INSERT RECORDS
songplay_table_insert = ("""
    INSERT INTO "songplays" ("start_time", "user_id", "level", "song_id", 
                            "artist_id", "session_id", "location", "user_agent")
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
""")

user_table_insert = ("""
    INSERT INTO "users" ("user_id", "first_name", "last_name", "gender", "level")
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT ("user_id") DO NOTHING 
""")

song_table_insert = ("""
    INSERT INTO "songs" ("song_id", "title", "artist_id", "year", "duration")
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT ("song_id") DO NOTHING 
""")

artist_table_insert = ("""
    INSERT INTO "artists" ("artist_id", "name", "location", "latitude", "longitude")
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT ("artist_id") DO NOTHING 
""")

time_table_insert = ("""
    INSERT INTO "time" ("start_time", "hour", "day", "week", "month", "year", "weekday")
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT ("start_time") DO NOTHING 
""")


# FIND SONGS
song_select = ("""
    SELECT s.song_id song_id, a.artist_id artist_id
    FROM songs s
    JOIN artists a
    ON s.title = %s
    AND a.name = %s
    AND s.duration = %s
""")


# QUERY LISTS
# NOTE: Order of creation matters as there are a ton of reference dependency
create_table_queries = [user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]