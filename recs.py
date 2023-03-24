# from neo4j import GraphDatabase
import subprocess
import random
import os
from spotify_objects import Song
import pandas as pd
import csv

categorical_col = ['track_id', 'artists', 'album_name', 'track_name', 'explicit', 'key', 'mode', 'time_signature',
                   'track_genre']
cols = ["track_id", "artists", "album_name", "track_name", "popularity", "duration_ms", "explicit",
        "danceability", "energy", "key", "loudness", "mode", "speechiness", "acousticness", "instrumentalness",
        "liveness", "valence", "tempo", "time_signature", "track_genre"]

key_weights = [1] * (len(cols) - 1)
thresholds = [.5] * (len(cols) - 1)


def write_sim_score(writer, row, row_outter):
    if row.equals(row_outter):
        return
    #song = Song(row[0], row[1:], cols)
    song = Song(row[0], row[1:], row[1:].keys())
    song_outter = Song(row_outter[0], row_outter[1:], cols)
    score = song.find_sim_score(song_outter, key_weights, thresholds, categorical_col)
    writer.writerow([str(song.id), str(song_outter.id), str(score)])


def apply_sim_score(writer, row_outter, df):
    df.apply(lambda row: write_sim_score(writer, row_outter, row), axis=1)


def make_edge_csv(node_file):
    df = pd.read_csv(node_file)
    del df[df.columns[0]]
    with open('edges.csv', 'w') as infile:
        writer = csv.writer(infile)
        header = ['to', 'from', 'sim_score']
        writer.writerow(header)
        df.apply(lambda row: apply_sim_score(writer, row, df), axis=1)


def egrep_exp(artist, song):
    return "(.*,.*," + artist + ",.*," + song + ",.*)"


def full_egrep_exp(artists, songs):
    return "|".join([egrep_exp(artists[i], songs[i]) for i in range(len(artists))])


def extract_songs(songs, artists):
    subprocess.check_output('rm -f pre_sample.csv', shell=True)
    regex = full_egrep_exp(artists, songs)
    cmd = "tail -n +2 spotify.csv | egrep -v '" + regex + "' | sort -t, -k3,3 -k5,5 -u > pre_sample.csv"
    subprocess.check_output(cmd, shell=True)

    cmd = "egrep '" + regex + "' spotify.csv | sort -t, -k3,3 -k5,5 -u > songs.csv"
    subprocess.check_output(cmd, shell=True)

    sample_cmd = 'shuf -n 10 pre_sample.csv > sample.csv'
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

    #df = pd.read_csv('spotify.csv')
    make_edge_csv('combo_sample.csv')
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
