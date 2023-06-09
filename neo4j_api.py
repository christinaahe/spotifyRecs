from neo4j import GraphDatabase
import random

with open('password.txt') as infile:
    PASS = infile.readline().strip()
NODES = 'nodes.csv'
EDGES = 'edges.csv'


# Neo4J API used to upload results from our song recommendation engine
class neo4jAPI:
    def __init__(self, user='neo4j', password=PASS, port="bolt://localhost:7687"):
        self.driver = GraphDatabase.driver(port, auth=(user, password))

    def run_cmd(self, cmd):
        """runs a neo4j command (cmd)"""
        with self.driver.session() as session:
            res = session.run(cmd)
            return res.data()

    def add_edge(self, filename, id, node_label, rel_label, rel_prop):
        """adds relationships to neo4j graph
        filename: str, path of file containing columns ['to', 'from', 'sim_score']"""
        cmd = """
        LOAD CSV WITH HEADERS FROM "file:///""" + filename + """" AS row
        MATCH (source: """ + node_label + "{" + id + """: row.to})
        MATCH (target: """ + node_label + "{" + id + """: row.from})
        CREATE (source)-[:""" + rel_label + "{" + rel_prop + ": row.sim_score}]->(target);"
        self.run_cmd(cmd)


    def add_node(self, filename, node_label, properties):
        """adds nodes to neo4j graph
        filename: str, path of file containing node information"""
        props = []
        for p in properties:
            props.append(str(p + ":row." + p))
        prop_str = ", ".join(props)
        cmd = """
        LOAD CSV WITH HEADERS FROM "file:///""" + filename + """" AS row
        CREATE (:""" + node_label + " {" + prop_str + "});"
        self.run_cmd(cmd)



    def get_recs(self, reference_songs, reference_artists, n):
        """gets n recommendations based on reference songs by reference artists"""
        all = []
        for i in range(len(reference_songs)):
            cmd = """
            MATCH (s:Song)-[SIMILAR]-(r:Song)
            WHERE s.track_name = '""" + reference_songs[i] + """'
            AND s.artists = '""" + reference_artists[i] + """'
            RETURN r.track_name"""
            all.extend(self.run_cmd(cmd))
        all_names = [song['r.track_name'] for song in all if song['r.track_name'] not in reference_songs]
        return random.sample(list(set(all_names)), n)


    def get_property(self, ref_property, ref_value, targ_property):
        """gets targ_property value of song given that it has a ref_value in ref_property"""
        cmd = "MATCH (s:Song {" + ref_property + ": '" + ref_value + """'})
        RETURN s.""" + targ_property + ";"
        return self.run_cmd(cmd)


def main():
    driver = neo4jAPI()
    # clears neo4j database
    driver.run_cmd('match (n) detach delete n')
    # adds songs and similarity scores
    driver.add_node(NODES, 'Song', ['track_id', 'track_name', 'artists'])
    driver.add_edge(EDGES, 'track_id', 'Song', 'SIMILAR', 'score')
    # prints 5 recs
    print(driver.get_recs(['The Call', 'Two Birds', 'Sampson'], ['Regina Spektor', 'Regina Spektor', 'Regina Spektor'], 5))


if __name__ == '__main__':
    main()


