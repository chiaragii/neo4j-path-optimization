import os
import random
import time

import numpy as np
import pandas as pd
import pytz
from dotenv import load_dotenv
from neo4j import GraphDatabase
import neo4j
from datetime import datetime


class ActiveCaseGeneration:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def create_prefixes(self):
        start = time.time()
        prefixes = pd.DataFrame()
        max_len = self.driver.execute_query("MATCH (n:Event) RETURN max(n.activity_id) AS max_activity_id")[0][0][0]
        # Concatenating the empty row to the existing DataFrame
        for k in range(1, max_len - 1):
            result = self.driver.execute_query(f"MATCH (e:Event) WHERE e.activity_id <= {k} "
                                               f"WITH collect(e) AS p_nodes, e.track_id as track_id "
                                               f"WHERE size(p_nodes)={k} "
                                               f"UNWIND p_nodes AS node1 "
                                               f"UNWIND p_nodes AS node2 "
                                               f"OPTIONAL MATCH (node1)-[r]-(node2) "
                                               f"MATCH (l:Event) WHERE l.activity_id={k + 1} "
                                               f"AND l.track_id=node1.track_id "
                                               f"RETURN DISTINCT p_nodes, collect(DISTINCT r) as p_rels, "
                                               f"track_id, [l] as label",
                                               database_="neo4j",
                                               result_transformer_=neo4j.Result.to_df)

            for i in range(0, len(result)):
                prefix_id = result['track_id'][i] + "_" + str(k)
                prefixes = prefixes.reindex(range(len(prefixes) + 1))
                prefixes = pd.concat([prefixes, pd.DataFrame({'e_v': ['XP']})], ignore_index=True)
                for j in list(result.columns):
                    if j != "track_id":
                        flattened_data = result[j][i]
                        data = [self.extract_properties(el) for el in flattened_data]
                        if j == 'label':
                            data[0]['node1'] = str(int(data[0].pop('activity_id')))
                            data[0]['prefix_id'] = prefix_id
                            data[0]['e_v'] = "l"
                            prefixes = pd.concat([prefixes, pd.DataFrame(data)], ignore_index=True)
                        if j == 'p_nodes':
                            for node in data:
                                node['node1'] = str(int(node.pop('activity_id')))
                                node['prefix_id'] = prefix_id
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
                                rel['prefix_id'] = prefix_id
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

    def generate_active_case(self, s_prefix, f_prefix):
        start = time.time()
        print(start)

        # se ci sono due attività parallele finali, viene ritornato solo il prefisso di lunghezza maggiore
        result = self.driver.execute_query(
            f"MATCH (s:Event), (e:Event) "
            f"WHERE s.event_name='START' s.track_id = e.track_id AND (s.start_time <= {s_prefix} OR "
            f"s.start_time >= {s_prefix}) AND e.finish_time <= {f_prefix} AND e.track_id='173715' "
            f"WITH COLLECT(e) as nodes, s "
            f"UNWIND nodes as node1 "
            f"UNWIND nodes as node2 "
            f"OPTIONAL MATCH (node1)-[r]-(node2) "
            f"RETURN DISTINCT nodes, COLLECT(DISTINCT r), s.track_id+'_'+size(nodes) as prefix_id",
            database_="neo4j")

        end = time.time()
        time_taken = end - start
        return result, time_taken

    def active_activity_neo4j(self):
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
        result['start_time_prefix'] = pd.to_datetime(result['start_time_prefix'].
                                                     apply(lambda x: x.to_native() if pd.notna(x) else None))
        result['finish_time_last_activity'] = pd.to_datetime(result['finish_time_last_activity'].
                                                             apply(lambda x: x.to_native() if pd.notna(x) else None))
        result.to_csv('data/active_activities_Neo4j.csv', index=False)

        end = time.time()
        elapsed_time = end - start_time
        print(f"Active activities execution time with Neo4j: {elapsed_time:.6f} seconds")
        return result

    def import_data(self):
        start_time = time.time()
        result = self.driver.execute_query(
            "LOAD CSV FROM 'file:///prefix_log_2000.csv' "
            "AS row MERGE (:Event{activity_id:toInteger(row[0]), "
            "event_name:row[1], track_id:row[2], "
            "start_time:datetime(apoc.text.replace(apoc.text.replace(row[3], '\+\d{2}:\d{2}$', ''), ' ', 'T')), "
            "finish_time:datetime(apoc.text.replace(apoc.text.replace(row[4], '\+\d{2}:\d{2}$', ''), ' ', 'T')), "
            "resource:row[5]}) ",
            database_="neo4j",
        )
        end = time.time()
        elapsed_time = end - start_time
        print(f"{elapsed_time:.6f} seconds (2000)")
        return result


def active_case_and_final_activity_dbs():
    start_time = time.time()

    with open('data/graphs.g', 'r') as file:
        lines = file.readlines()

    case_data = []
    final_activities = []
    start_time_prefix = None

    for line in lines:
        if line.startswith('v'):
            parts = line.split()
            if parts[1] == '1':
                start_time_prefix = parts[3]

            v = parts[0]
            case_id = parts[4]
            index = int(parts[1])
            index2 = float('nan')
            finish_time_activity = parts[3]
            event_name = parts[2]
            case_data.append((case_id, index, start_time_prefix, finish_time_activity))
            final_activities.append((v, case_id, index, index2, event_name, finish_time_activity))

        if line.startswith('e'):
            parts = line.split()
            e = parts[0]
            case_id = float('nan')
            event_name = parts[3]
            finish_time_activity = float('nan')
            index = int(parts[1])
            index2 = int(parts[2])
            final_activities.append((e, case_id, index, index2, event_name, finish_time_activity))

    # create dataframe and make conversions
    final_df = pd.DataFrame(final_activities, columns=['e_v', 'track_id', 'node1', 'node2', 'event_name', 'finish'])
    final_df['finish'] = final_df['finish'].apply(lambda x: str(x)[:18])
    final_df['finish'] = (final_df['finish'].apply
                          (lambda x: pd.to_datetime(x, format='%Y-%m-%d%H:%M:%S', utc=True) if not pd.isna(x) else x))

    # calculates effective start times
    final_df['start'] = final_df['finish'].shift(periods=1)
    final_df['start'] = final_df.apply(lambda x: x['finish'] if x['node1'] == 1 else x['start'], axis=1)
    parallel_df = final_df[final_df['e_v'] == 'e'].groupby(['node1', 'track_id']).filter(lambda x: len(x) > 1)
    for index, row in parallel_df.iterrows():
        # si prende il vertice corrispondente al nodo di partenza delle attività parallele correnti
        mask = ((final_df[final_df['e_v'] == 'v']['node1'] == row['node1']) &
                (final_df[final_df['e_v'] == 'v']['track_id'] == row['track_id']))
        # si prende il tempo di fine di questo vertice
        finish_time = final_df[final_df['e_v'] == 'v'].loc[mask, 'finish']
        # si modifica il tempo di inizio dell'attività parallela con il tempo di fine dell'attività precedente
        row_index = final_df[(final_df['track_id'] == row['track_id']) &
                             (final_df['node1'] == row['node2']) & (final_df['e_v'] == 'v')].index
        final_df.loc[row_index, 'start'] = finish_time.iloc[0]

        # si crea una lista dei nodi di destinazione delle attività parallele correnti
        node2_name = list(final_df[(final_df['track_id'] == row['track_id']) &
                                   (final_df['node1'] == row['node2']) & (final_df['e_v'] == 'e')]['node2'])
        node2_info = final_df[(final_df['track_id'] == row['track_id']) &
                              (final_df['node1'].isin(node2_name)) & (final_df['e_v'] == 'v')]
        # si calcola il tempo di inizio minimo tra i nodi di destinazione identificati
        min_time = node2_info['start'].min()
        final_df.loc[row_index, 'finish'] = node2_info[node2_info['start'] == min_time]['start'].iloc[0]

    final_df = final_df[final_df['e_v'] != 'e']
    final_df = final_df.drop(columns=['node2', 'e_v'])
    final_df = final_df.rename(columns={"node1": "index"})
    final_df.to_csv('data/final_activities.csv', index=False)

    df = pd.DataFrame(case_data, columns=['track_id', 'index', 'start_time_prefix', 'finish_time_last_activity'])
    df['start_time_prefix'] = df['start_time_prefix'].apply(lambda x: str(x)[:18])
    df['start_time_prefix'] = pd.to_datetime(df['start_time_prefix'], format='%Y-%m-%d%H:%M:%S', utc=True)
    df['finish_time_last_activity'] = df['finish_time_last_activity'].apply(lambda x: str(x)[:18])
    df['finish_time_last_activity'] = pd.to_datetime(df['finish_time_last_activity'], format='%Y-%m-%d%H:%M:%S',
                                                     utc=True)
    df.to_csv('data/active_activities.csv', index=False)

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Active activities execution time without Neo4j: {elapsed_time:.6f} seconds")


def get_prefix_information(s_prefix, f_prefix):
    # read file DBs
    active_activities = pd.read_csv('data/active_activities.csv')
    active_activities['start_time_prefix'] = (active_activities['start_time_prefix'].
                                              apply(lambda x: str(x).replace(" ", "")[:18]))
    active_activities['start_time_prefix'] = pd.to_datetime(active_activities['start_time_prefix'],
                                                            format='%Y-%m-%d%H:%M:%S', utc=True)
    active_activities['finish_time_last_activity'] = (active_activities['finish_time_last_activity'].
                                                      apply(lambda x: str(x).replace(" ", "")[:18]))
    active_activities['finish_time_last_activity'] = pd.to_datetime(active_activities['finish_time_last_activity'],
                                                                    format='%Y-%m-%d%H:%M:%S', utc=True)

    # read file DBf
    final_activities = pd.read_csv('data/final_activities.csv')
    final_activities['start'] = (final_activities['start'].
                                 apply(lambda x: str(x).replace(" ", "")[:18]))
    final_activities['start'] = pd.to_datetime(final_activities['start'], format='%Y-%m-%d%H:%M:%S', utc=True)
    final_activities['finish'] = (final_activities['finish'].
                                  apply(lambda x: str(x).replace(" ", "")[:18]))
    final_activities['finish'] = pd.to_datetime(final_activities['finish'], format='%Y-%m-%d%H:%M:%S', utc=True)

    # read file DBp
    prefixes = pd.read_csv('data/prefixes.csv')

    active_prefixes = active_activities[((active_activities['start_time_prefix'] <= s_prefix) |
                                         (active_activities['start_time_prefix'] >= s_prefix)) &
                                        (active_activities['finish_time_last_activity'] <= f_prefix)]
    active_prefixes_keys = active_prefixes.loc[active_prefixes.groupby('track_id')['finish_time_last_activity'].idxmax()][['track_id', 'index']]
    maximal_active_prefixes = list(active_prefixes_keys['track_id'].astype(str) + '_' + active_prefixes_keys['index'].astype(str))
    complete_prefixes = prefixes[prefixes['prefix_id'].isin(maximal_active_prefixes)]


if __name__ == "__main__":
    # Load environment variables from .env file
    load_dotenv("config/database_conf.env")

    # Access the environment variables
    database_uri = os.getenv("DATABASE_URI")
    username = os.getenv("USERNAME_NEO4J")
    password = os.getenv("PASSWORD_NEO4J")

    # Connect to graph database
    connection = ActiveCaseGeneration(database_uri, username, password)

    # save sequential database: neo4j way
    # results = connection.active_case_neo4j()

    results = connection.import_data()

    # connection.get_active_cases()

    # save the prefixes in prefixes.csv file
    # connection.create_prefixes()

    connection.close()

    # save sequential database + final activities: python way
    # active_case_and_final_activity_dbs()

    '''date1 = datetime(2011, 10, 1, 8, 11, 7, tzinfo=pytz.utc)
    date2 = datetime(2011, 10, 1, 8, 11, 7,
                     tzinfo=pytz.timezone('America/New_York'))
    get_prefix_information(date1, date2)'''

    # get_some_prefixes()
