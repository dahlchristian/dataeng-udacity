import os
import glob
import psycopg2
import pandas as pd

from sql_queries import *


def process_song_file(cur, filepath):
    """
    Read song files and inserts into artist table.
    
    Reads in a file in data/song_data/, extracts song and artist info, and inserts
    info into songs and artists dim tables.
    
    Args:
        cur: The cursor
        filepath: The location of the song file
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values.tolist()[0]

    cur.execute(song_table_insert, song_data)

    # insert artist record
    artist_data = list(df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0])
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Read a log file and
    insert data to time, users and songplays tables
    Args:
        cur: the cursor object. 
        filepath: log data file path. 
    """

    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df.loc[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')

    # insert time data records
    time_data = [df.ts.values, t.dt.hour.values, t.dt.day.values,
                 t.dt.weekofyear.values, t.dt.month.values, t.dt.year.values,
                 t.dt.weekday.values]
    column_labels = ['start_time', 'hour', 'day','week', 'month', 'year', 'weekday']
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName',
                  'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():

        cur.execute(song_select, (row.song, row.artist, row.length))
        result = cur.fetchone()
        if result:
            songid, artistid = result
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = [row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent]
        cur.execute(songplay_table_insert, songplay_data)

def process_data(cur, conn, filepath, func):
    # 
        """
    get all files matching extension from directory. Also uses song and log functions to the data in the json files.
    
    Applies process_song_file() and process_log_file() to each file in
    data/song_data/ and data/log_data/, respectively.
    
    Args:
        cur: the cursor object.
        conn: database connection object.
        filepath: The location for the files that are to be processed: data/song_data/ or data/log_data/ file path.
        func: the functions to be executed: process_song_file or process_log_file function.
    Returns:
        None
    """
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
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
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()

if __name__ == "__main__":
    main()