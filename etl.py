import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
import numpy as np

# delete before submission
from psycopg2.extensions import register_adapter, AsIs
from utility import  addapt_numpy_array, addapt_numpy_float64, addapt_numpy_int64, addapt_numpy_float32, addapt_numpy_int32

"""The following register lines register the numpy data types with psycopg2 to avoid the error: "TypeError: can't adapt type <numpy data type>"
"""
register_adapter(np.ndarray, addapt_numpy_array)
register_adapter(np.float64, addapt_numpy_float64)
register_adapter(np.int64, addapt_numpy_int64)
register_adapter(np.float32, addapt_numpy_float32)
register_adapter(np.int32, addapt_numpy_int32)

def process_song_file(cur, filepath):
    """A function to process the song files and insert the data into the song and artist tables

    Args:
        cur (DB Cursor): The cursor to use to execute SQL queries
        filepath (str): Path to the song data to process
    """
    # open song file
    df = pd.read_json(filepath, lines=True) # read the file into a dataframe using pandas

    # replace all NaN values with None to avoind psycopg2 errors
    df.replace(np.nan, None, inplace=True)

    # extract the columns needed for the song table from the dataframe
    song_data = df.loc[:, ['song_id', 'title', 'artist_id', 'year', 'duration']]

    # insert song into song table
    for value in song_data.values:
        cur.execute(song_table_insert, value.tolist())


    # extract the columns needed for the artist table from the dataframe
    artist_columns = ['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']
    artist_data = df.loc[:, artist_columns]

    # insert records into artist table
    for value in artist_data.values:
        cur.execute(artist_table_insert, value.tolist())

    # cur.execute(artist_table_insert, artist_data)

    print("song and artist insterted into DB tables")


def process_log_file(cur, filepath):
    # open log file
    # read the file into a dataframe using pandas
    df = pd.read_json(filepath, lines=True)

    # replace all NaN values with None to avoind psycopg2 error
    df.replace(np.nan, None, inplace=True)

    # convert the timestamp column to datetime
    df["ts_datetime"] = pd.to_datetime(df['ts'], unit='ms')

    # filter by NextSong action
    # get only the rows with page = NextSong
    df = df.loc[df.page == 'NextSong', :].copy()
    # convert timestamp column to datetime
    t = df.ts_datetime # get the timestamp column

    # insert time data records
    time_data = []
    column_labels = ['timestamp', 'hour', 'day', 'weekofyear', 'month', 'year', 'weekday']
    for data in t:
        # create a list of lists with the data needed for the time table
        time_data.append([data, data.hour, data.day, data.weekofyear, data.month, data.year, data.day_name()])

    # create a dataframe from the list of lists
    time_df = pd.DataFrame(time_data, columns=column_labels)

    for _, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df.loc[:, ["userId", "firstName", "lastName", "gender", "level"]].copy()

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():

        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts_datetime, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    # conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    # cur = conn.cursor()

    # connect to sparkify database on local computer
    # The connection here is to my local docker container running postgres

    conn = psycopg2.connect(
    dbname="sparkifydb",
    password="password",
    user="aadehamid",
    host="172.18.0.3",
    port="5432")
    conn.set_session(autocommit=True)
    cur = conn.cursor()


    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
