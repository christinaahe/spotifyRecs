from neo4j import GraphDatabase
import subprocess
import random
lines = open('spotify.csv').readlines()
random.shuffle(lines)

open('sample.csv', 'w').writelines(lines[:10000])


def extract_songs(songs, artists):
    files=[]
    for i in range(len(songs)):
        # puts song in separate csv file labeled song(i).csv
        command = "awk -F ',' '{if ($5==" + '"' + songs[i] + '"' + " && $3==" + '"' + artists[i] + '"' + ") print}' spotify.csv > song" + str(i) + ".csv"
        line = subprocess.check_output(command, shell=True)
        line = l

        
        # adds file name to files list
        files.append("song" + str(i) + ".csv")

        # command to remove song from original file
        rm_cmd = "awk -F ',' '!($5 == " + '"' + songs[i] + '"' + ")' spotify.csv > spotify.csv"
        rm_cmd = "sed -i '' ''"
        subprocess.check_output(rm_cmd, shell=True)

    sample_cmd = 'shuf -n 10000 spotify.csv > sample.csv'
    subprocess.check_output(sample_cmd, shell=True)

    cat_cmd = 'cat ' + ' '.join(files) + ' sample.csv'
    subprocess.check_output(cat_cmd, shell=True)

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