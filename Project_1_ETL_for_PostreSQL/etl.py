import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Function reads the file and load selected data to table songs or artists
    Parameters:
    - cur (psycopg2.cursor()): Cursor for the Sparkifydb database
    - filepath (str): Filepath parent of the logs to be extract and load
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id', 'title' ,'artist_id', 'year', 'duration']].values[0].tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id', 'artist_name' ,'artist_location', 'artist_latitude', 'artist_longitude']].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Function reads the file, transform timestamp to date units and  load to table time.
    Also it  reads the file and load selected data to table users
    And finally it combines and collect data to load to table songplays
    Parameters:
    - cur (psycopg2.cursor()): Cursor for the Sparkifydb database
    - filepath (str): Filepath parent of the logs to be extract and load
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df.loc[df['page'] == "NextSong"] 

    # convert timestamp column to datetime columns
    df.loc[:, 'start_time'] = pd.to_datetime(df['ts'], unit='ms')
    df.loc[:, 'hour'] = df['start_time'].dt.hour
    df.loc[:, 'day'] = df['start_time'].dt.day
    df.loc[:, 'week'] = df['start_time'].dt.week
    df.loc[:, 'month'] = df['start_time'].dt.month
    df.loc[:, 'year'] = df['start_time'].dt.year
    df.loc[:, 'weekday'] = df['start_time'].dt.weekday
    
    # create time_df based on df datetime columns
    time_df = df.loc[:, ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']]

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df.loc[:, ['userId', 'firstName', 'lastName', 'gender', 'level']]

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
        songplay_data = (row.start_time, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Сreate a list of datafiles through all files nested under provided filepath.
    Parameters:
    - cur (psycopg2.cursor()): Cursor for the Sparkifydb database
    - conn (psycopg2.connect(*connection details in the main()*)): Connection to the sparkifycdb database
    - filepath (str): Filepath parent of the logs to be analyzed
    - func: Defined function to process each type of data (song_data or log_data)
    Returns:
    - All names of files processed
    """
    
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
    """
    Functions to extract, transform and load data from song and log into a PostgreSQL DB
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()