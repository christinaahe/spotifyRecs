#from neo4j import GraphDatabase
import subprocess
import random
import os


lines = open('spotify.csv').readlines()
random.shuffle(lines)

open('sample.csv', 'w').writelines(lines[:10000])


def extract_songs(songs, artists):
    files=[]

    cmd = "cp spotify.csv ./copy.csv"
    subprocess.check_output(cmd, shell=True)
    seps = '","'.join(["$" + str(i) for i in range(6, 21)])
    cmd = 'awk -F, "{print $3","$5","$1","$2","$4","' + seps + '"}" OFS=, spotify.csv > tmp && mv tmp spotify.csv'
    subprocess.check_output(cmd, shell=True)
    for i in range(len(songs)):

        # puts song in separate csv file labeled song(i).csv
        command = "awk -F ',' '{if ($5==" + '"' + songs[i] + '"' + " && $3==" + '"' + artists[i] + '"' + \
                  ") print}' copy.csv > song" + str(i) + ".csv"
        subprocess.check_output(command, shell=True)
        cmd = "head -n 1 song" + str(i) + ".csv"
        line = subprocess.check_output(cmd, shell=True).decode("utf-8").strip()
        print(line)

        
        # adds file name to files list
        files.append("song" + str(i) + ".csv")

        # command to remove song from original file
        #rm_cmd = "awk -F ',' '!($5 == " + '"' + songs[i] + '"' + ")' spotify.csv > spotify" + str(i) + ".csv"
        #rm_cmd = "sed -i '/.*,.*,'" + artists[i] + "',.*,'" + songs[i] + "',.*$/d' copy.csv"

        rm_cmd = "sed -i '' '/^" + artists[i] + "," + songs[i]+ "/d' data.csv"
        subprocess.check_output(rm_cmd, shell=True)

    sample_cmd = 'shuf -n 10000 copy.csv > sample.csv'
    subprocess.check_output(sample_cmd, shell=True)

    cat_cmd = 'cat ' + ' '.join(files) + ' sample.csv'
    rm_cmd = 'rm copy.csv'
    subprocess.check_output(cat_cmd, shell=True)
    subprocess.check_output(rm_cmd, shell=True)



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