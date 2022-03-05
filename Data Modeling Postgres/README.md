# Data Modeling with Postgres
Sparkify is a startup that want to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently they don't have an easy way to query their data, which resides in a directly of JSON logs on user activity on the app, as well as a dirctory with JSON metadata on songs on the app.

I am tasked with creating a postgres database with tables designed to optimize queries on songplay analysis.

## About the files
There are 5 files in the repository and on folder called `data`. The data folder is a folder containing just a subset of the data from sparkify. This subset is just for the purpose of building the postgres database.

For the `.ipynb` files, this are notebook files used as guide to create the models and the `.py` files are the script that actual performs the task of creating those models.

## How to run
### Requirements
To run the files you need to have this packages installed on your system.
* Python v3.0 and above
* Pandas
* Psycopg2

### Run
You can archive this task from anywhere, e.g a text editor (vscode, sublime...) but I would be describing how to run from the terminal
* Open your terminal (`command prompt` for windows)
* Navigate to the location where you have all of the files located. The files are;
    * Folder `data`
    * `create_tables.py`
    * `etl.py`
    * `sql_queries.py`
* Next enter `python create_table.py` on your terminal. This creates all the tables needed in your database.
* Then enter `etl.py` which loads the data into your tables.

## Databare Schema
There are five tables created in the database;
1. `songplays` which contains data about users activities on the app. This table does not contain user information or song information.
2. `users` which contains data about the users and this can be joined with the songplays table for analysis with the user_id column.
3. `songs` which is a table that contains information of the songs in the app. This can also be joined to the songplays table with the song_id column.
4. `artists` table is a table that contains artist information and can be joined to songplays via the artist_id column.
5. `time` table is the last table and contains breakdown of the time when user interract with the app into month, dayofyear, weekofyear, dayofweek ...

Having your data modelled in a normalised way like this efficient and optimized.