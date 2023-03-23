# from neo4j import GraphDatabase
import subprocess
import random
import os
from spotify_objects import Song
import pandas as pd


#lines = open('spotify.csv').readlines()
#random.shuffle(lines)


# open('sample.csv', 'w').writelines(lines[:10000])


def make_edge_csv(node_file)
    df = pd.read_csv(node_file)

    # dict = {'from': [list of song_ids], 'to': [list of song ids], 'sim_score': [list of song sim scores]}}



def egrep_exp(artist, song):
    return "(.*,.*," + artist + ",.*," + song + ",.*)"


def full_egrep_exp(artists, songs):
    return "|".join([egrep_exp(artists[i], songs[i]) for i in range(len(artists))])


def extract_songs(songs, artists):
    #subprocess.check_output('rm -f combo_sample.csv', shell=True)

    regex = full_egrep_exp(artists, songs)
    cmd = "tail -n +2 spotify.csv | egrep -v '" + regex + "' | sort -t, -k3,3 -k5,5 -u > pre_sample.csv"
    subprocess.check_output(cmd, shell=True)

    cmd = "egrep '" + regex + "' spotify.csv | sort -t, -k3,3 -k5,5 -u > songs.csv"
    subprocess.check_output(cmd, shell=True)

    sample_cmd = 'shuf -n 10000 pre_sample.csv > sample.csv'
    subprocess.check_output(sample_cmd, shell=True)

    cat_cmd = 'cat sample.csv songs.csv > combo_sample.csv'
    subprocess.check_output(cat_cmd, shell=True)
    subprocess.check_output('rm -f pre_sample.csv', shell=True)
    subprocess.check_output('rm -f sample.csv', shell=True)
    subprocess.check_output('rm -f songs.csv', shell=True)


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
