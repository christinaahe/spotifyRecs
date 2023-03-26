# from neo4j import GraphDatabase
import subprocess
import random
import os
from spotify_objects import Song
import pandas as pd
import csv
import time

# categorical columns (song characteristics) for songs stored in the spotify.csv file
categorical_col = ['track_id', 'artists', 'album_name', 'track_name', 'explicit', 'key', 'mode', 'time_signature',
                   'track_genre']

# all columns (song characteristics) for songs stored in the spotify.csv file
cols = ["track_id", "artists", "album_name", "track_name", "popularity", "duration_ms", "explicit",
        "danceability", "energy", "key", "loudness", "mode", "speechiness", "acousticness", "instrumentalness",
        "liveness", "valence", "tempo", "time_signature", "track_genre"]

# weighst associated with songs characteristics used to calculate similarity score
key_weights = [0, 0, 0, 3, 1.5, 0, 6, 5, 0, 4, 0, 4.5, 7, 5, 3, 2, 4, 0, 0]
#thresholds = [.5] * (len(cols) - 1)

# function that calculates similarity score between two songs
def write_sim_score(writer, row, row_outter, min_score):
    if row.equals(row_outter):
        return
    #song = Song(row[0], row[1:], cols)
    song = Song(row[0], row[1:], row[1:].keys())
    song_outter = Song(row_outter[0], row_outter[1:], cols)
    score = song.find_sim_score(song_outter, key_weights, thresholds, categorical_col)
    if score >= min_score:
        writer.writerow([str(song.id), str(song_outter.id), str(score)])

# helper function that applies the write_sim_score function
def apply_sim_score(writer, row_outter, df):
    df.apply(lambda row: write_sim_score(writer, row_outter, row, 0), axis=1)

# creates csv file that stored edges (relationship) data between songs (nodes); uploaded to neo4j
def make_edge_csv(node_file):
    df = pd.read_csv(node_file)
    del df[df.columns[0]]
    with open('edges.csv', 'w') as infile:
        writer = csv.writer(infile)
        header = ['to', 'from', 'sim_score']
        writer.writerow(header)
        df.apply(lambda row: apply_sim_score(writer, row, df), axis=1)

# creates edge between two songs depending on the similarity score
def other_make_edge(df, min_score, thresholds):
    #df = pd.read_csv(node_file)
    df[df.columns[0]]
    with open('edges.csv', 'w') as infile:
        writer = csv.writer(infile)
        header = ['to_node', 'from_node', 'sim_score']
        writer.writerow(header)
        for row_outter in df.itertuples():
            song_df = pd.DataFrame(columns=['to_node', 'from_node', 'sim_score'])
            for row in df.itertuples():
                if row_outter == row:
                    continue
                song = Song(row[1], row[1:], df.columns)
                song_outter = Song(row_outter[1], row_outter[1:], df.columns)
                score = song.find_sim_score(song_outter, key_weights, thresholds, categorical_col)
                if score >= min_score:
                    temp_df = pd.DataFrame([[song.id, song_outter.id, score]], columns=['to_node', 'from_node', 'sim_score'])
                    song_df = pd.concat([song_df, temp_df])
                    #writer.writerow([str(song.id), str(song_outter.id), str(score)])
            if not song_df.empty:
                #print(song_df.head(5))
                #print(song_df.columns)
                song_df.sort_values('sim_score', ascending=False, inplace=True, axis=0)
                songs = song_df.head(30).values
                writer.writerows(songs)

# applies specific threshold utilized to calculate similarity score 
def get_thresholds(node_df):
    thresholds = [node_df[col].std() if col not in categorical_col else 0 for col in node_df.columns[1:]]
    return [elem * .5 for elem in thresholds]

def egrep_exp(artist, song):
    return "(.*,.*," + artist + ",.*," + song + ",.*)"


def full_egrep_exp(artists, songs):
    return "|".join([egrep_exp(artists[i], songs[i]) for i in range(len(artists))])

# creates sample set of songs utilizing terminal commands
def extract_songs(songs, artists):
    subprocess.check_output('rm -f pre_sample.csv', shell=True)
    regex = full_egrep_exp(artists, songs)
    cmd = "tail -n +2 spotify.csv | egrep -v '" + regex + "' | sort -t, -k3,3 -k5,5 -u > pre_sample.csv"
    subprocess.check_output(cmd, shell=True)

    cmd = "egrep '" + regex + "' spotify.csv | sort -t, -k3,3 -k5,5 -u > songs.csv"
    subprocess.check_output(cmd, shell=True)

    sample_cmd = 'shuf -n 5000 pre_sample.csv > sample.csv'
    subprocess.check_output(sample_cmd, shell=True)

    header_cmd = "head -1 spotify.csv > head.csv"
    subprocess.check_output(header_cmd, shell=True)

    cat_cmd = 'cat head.csv sample.csv songs.csv > combo_sample.csv'
    subprocess.check_output(cat_cmd, shell=True)

    subprocess.check_output('rm -f pre_sample.csv', shell=True)
    subprocess.check_output('rm -f sample.csv', shell=True)
    subprocess.check_output('rm -f songs.csv', shell=True)
    subprocess.check_output('rm -f head.csv', shell=True)
    subprocess.check_output('rm -f final.csv', shell=True)

    #header_cmd = 'sed -i -e' + '1itrack_id,artists,album_name,track_name,popularity,duration_ms,explicit,danceability,energy,key,loudness,mode,speechiness,acousticness,instrumentalness,liveness,valence,tempo,time_signature,track_genre\\\\' + ' combo_sample.csv'
    #subprocess.check_output(header_cmd, shell=True)


def _count_gen(reader):
    """

    :param reader: function
        reads raw binary
    :return: generator object
        generates the binary number of each line in a file
    """
    b = reader(1024 * 1024)
    while b:
        yield b
        b = reader(1024 * 1024)


def raw_gen_count(filename):
    """

    :param filename: string
        the name of a file to count the lines in
    :return: int
        the number of lines in the input file
    """
    infile = open(filename, 'rb')
    infile_gen = _count_gen(infile.raw.read)
    return sum(buf.count(b'\n') for buf in infile_gen)


def main():
    songs = ['The Call', "Two Birds", "Samson"]
    artists = ['Regina Spektor'] * len(songs)
    extract_songs(songs, artists)
    node_df = pd.read_csv('combo_sample.csv')
    del node_df[node_df.columns[0]]



    thresholds = get_thresholds(node_df)

    #df = pd.read_csv('spotify.csv')
    #start = time.time()
    #make_edge_csv('combo_sample.csv')
    #print(time.time() - start)
    print('---------------')
    start = time.time()
    other_make_edge(node_df, 0, thresholds)
    print(time.time() - start)
    #print(df.describe(include='all'))
    # with open('password.txt') as infile:
    #     password = infile.readline().strip()
    # driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", password))
    #
    # with driver.session() as session:
    #     result = session.run("MATCH (n) RETURN n LIMIT 10")
    #     for record in result:
    #         print(record)


if __name__ == '__main__':
    main()
