import os
import time
import pandas as pd
from dotenv import load_dotenv
from neo4j import GraphDatabase
import neo4j
from datetime import datetime


class ActiveCaseGeneration:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    # function to format the DateTime object
    def format_date(self, dt):
        if dt is not None:
            iso_str = dt.iso_format()
            formatted_str = iso_str.replace("T", "").replace("Z", "+00:00")
            return formatted_str
        return None

    def create_prefixes(self):
        start = time.time()
        prefixes = pd.DataFrame()
        max_len = self.driver.execute_query("MATCH (n:Event) RETURN max(n.activity_id) AS max_activity_id")[0][0][0]
        # Concatenating the empty row to the existing DataFrame
        for k in range(1, max_len):
            result = self.driver.execute_query(f"MATCH (e:Event) WHERE e.activity_id <= {k} "
                                               f"WITH collect(e) AS p_nodes, e.track_id as track_id "
                                               f"WHERE size(p_nodes)={k} "
                                               f"UNWIND p_nodes AS node1 "
                                               f"UNWIND p_nodes AS node2 "
                                               f"OPTIONAL MATCH (node1)-[r]-(node2) "
                                               f"RETURN DISTINCT p_nodes, collect(DISTINCT r) as p_rels, track_id",
                                               database_="neo4j",
                                               result_transformer_=neo4j.Result.to_df)

            for i in range(0, len(result)):
                prefixes = prefixes.reindex(range(len(prefixes) + 1))
                prefixes = pd.concat([prefixes, pd.DataFrame({'e_v': ['XP']})], ignore_index=True)
                for j in list(result.columns):
                    if j != "track_id":
                        flattened_data = result[j][i]
                        data = [self.extract_properties(el) for el in flattened_data]
                        if j == 'p_nodes':
                            for node in data:
                                node['node1'] = str(int(node.pop('activity_id')))
                                node['e_v'] = "v"
                            prefixes = pd.concat([prefixes, pd.DataFrame(data)], ignore_index=True)
                        if j == 'p_rels' and data != []:
                            for rel in data:
                                # for node 1 and node 2
                                connnected_nodes = rel['connection'].split(':')[0].split('_')
                                node1 = str(int(connnected_nodes[0]))
                                node2 = str(int(connnected_nodes[1]))
                                new_key1 = 'node1'
                                new_key2 = 'node2'
                                rel[new_key1] = str(int(node1))
                                rel[new_key2] = str(int(node2))
                                del rel['connection']
                                # for track id
                                rel['track_id'] = result['track_id'][i]
                                rel['event_name'] = (
                                        str(prefixes[(prefixes['node1'] == node1)
                                                     & (prefixes['track_id'] == result['track_id'][i])]
                                            ['event_name']).split()[1]
                                        + "__" +
                                        str(prefixes[(prefixes['node1'] == node2)
                                                     & (prefixes['track_id'] == result['track_id'][i])]
                                            ['event_name']).split()[1])
                                rel['e_v'] = "e"
                            prefixes = pd.concat([prefixes, pd.DataFrame(data)], ignore_index=True)
        prefixes['start_time'] = pd.to_datetime(prefixes['start_time'].
                                                apply(lambda x: x.to_native() if pd.notna(x) else None))
        prefixes['finish_time'] = pd.to_datetime(prefixes['finish_time'].
                                                 apply(lambda x: x.to_native() if pd.notna(x) else None))
        prefixes.to_csv('data/prefixes.csv', index=False)
        finish = time.time()
        print(f"Time for prefix generation: {finish - start:.6f} seconds")

    def extract_properties(self, node):
        return node._properties

    def generate_active_case(self):
        start = time.time()
        print(start)
        result = self.driver.execute_query(
            "MATCH (activity: Event) WITH DISTINCT activity.start_time AS activity_start_time "
            "MATCH p=(s:Event {event_name: 'START'})-[r*]->(e:Event) "
            "WHERE e.start_time <= activity_start_time AND e.finish_time >= activity_start_time "
            "RETURN activity_start_time, COLLECT(DISTINCT nodes(p)) as active_case, COLLECT(DISTINCT r) "
            "as active_relationship, length(p)+1 as prefix_length "
            "ORDER BY activity_start_time, prefix_length ")

        end = time.time()
        time_taken = end - start
        return result, time_taken

    def active_case_neo4j(self):
        start_time = time.time()
        result = self.driver.execute_query(
            "MATCH p = (activity: Event{event_name:'START'})-[*]->(track: Event) "
            "WITH apoc.coll.flatten(collect(nodes(p))) as nodes, activity.start_time AS start_time_prefix, activity "
            "UNWIND nodes as node "
            "RETURN DISTINCT node.track_id as track_id, node.activity_id as index, start_time_prefix, "
            "node.finish_time as finish_time_last_activity ",
            database_="neo4j",
            result_transformer_=neo4j.Result.to_df
        )

        result['start_time_prefix'] = result['start_time_prefix'].apply(self.format_date)
        result['finish_time_last_activity'] = result['finish_time_last_activity'].apply(self.format_date)

        result.to_csv('data/active_activities_Neo4j.csv', index=False)

        end = time.time()
        elapsed_time = end - start_time
        print(f"Active activities execution time with Neo4j: {elapsed_time:.6f} seconds")
        return result


def active_case_db():
    start_time = time.time()

    with open('data/graphs.g', 'r') as file:
        lines = file.readlines()

    case_data = []
    start_time_prefix = None

    for line in lines:
        if line.startswith('v'):
            parts = line.split()
            if parts[1] == '1':
                start_time_prefix = parts[3]

            case_id = parts[4]
            index = parts[1]
            finish_time_activity = parts[3]
            case_data.append((case_id, index, start_time_prefix, finish_time_activity))

    df = pd.DataFrame(case_data, columns=['track_id', 'index', 'start_time_prefix', 'finish_time_last_activity'])
    df.to_csv('data/active_activities.csv', index=False)

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Active activities execution time without Neo4j: {elapsed_time:.6f} seconds")


if __name__ == "__main__":
    # Load environment variables from .env file
    load_dotenv("config/database_conf.env")

    # Access the environment variables
    database_uri = os.getenv("DATABASE_URI")
    username = os.getenv("USERNAME_NEO4J")
    password = os.getenv("PASSWORD_NEO4J")

    # Now you can use these variables to connect to your database
    connection = ActiveCaseGeneration(database_uri, username, password)
    results = connection.active_case_neo4j()
    connection.create_prefixes()
    # print(results)
    connection.close()

    active_case_db()
