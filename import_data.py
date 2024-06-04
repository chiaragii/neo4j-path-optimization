import os
import re
import time

import neo4j
import psutil
import threading

import pandas as pd
from dotenv import load_dotenv
from neo4j import GraphDatabase

if os.path.exists('data\output_files\memory_cpu.txt'):
    os.remove('data\output_files\memory_cpu.txt')

pid = 22188
print(pid)

class ActiveCaseGeneration:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def import_data2(self,stop_event):
        start_time = time.time()
        result = self.driver.execute_query(
            "LOAD CSV FROM 'file:///filtered_prefix_log_10percent.csv' "
            "AS row MERGE (:Event{activity_id:toInteger(row[0]), "
            "event_name:row[1], track_id:row[2], "
            "start_time:datetime(apoc.text.replace(apoc.text.replace(row[3], '\+\d{2}:\d{2}$', ''), ' ', 'T')), "
            "finish_time:datetime(apoc.text.replace(apoc.text.replace(row[4], '\+\d{2}:\d{2}$', ''), ' ', 'T')), "
            "resource:row[5]}) ",
            database_="neo4j",
        )
        end = time.time()
        stop_event.set()
        elapsed_time = end - start_time
        print(f"{elapsed_time:.6f} seconds (2000)")
        return result

    def import_data(self, stop_event):
        start_time = time.time()
        output_dir = 'data/output_files/500'

        files = sorted(
            [f for f in os.listdir(output_dir) if f.startswith('filtered_prefix_log_') and f.endswith('.csv')],
            key=lambda x: int(re.findall(r'\d+', x)[0])
        )
        first_files = files[:2]

        for file in first_files:
            file_num = re.findall(r'\d+', file)[0]
            file_path = f"file:///filtered_prefix_log_{file_num}_500.csv"

            start_time_file = time.time()
            result = self.driver.execute_query(
                f"LOAD CSV FROM '{file_path}' "
                "AS row MERGE (:Event{activity_id:toInteger(row[0]), "
                "event_name:row[1], track_id:row[2], "
                "start_time:datetime(apoc.text.replace(apoc.text.replace(row[3], '\+\d{2}:\d{2}$', ''), ' ', 'T')), "
                "finish_time:datetime(apoc.text.replace(apoc.text.replace(row[4], '\+\d{2}:\d{2}$', ''), ' ', 'T')), "
                "resource:row[5]}) ",
                database_="neo4j",
            )
            end_time_file = time.time()
            stop_event.set()
            elapsed_time = end_time_file - start_time_file
            print(f"{elapsed_time:.6f} seconds")

        end_time = time.time()
        elapsed_time = end_time - start_time
        print(f"{elapsed_time:.6f} seconds")

        return result

    def create_prefixes(self, stop_event):
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

        finish = time.time()
        stop_event.set()
        print(f"Time for prefix generation: {finish - start:.6f} seconds")
        return result


def get_some_prefixes():
    df = pd.read_csv('data/prefixes/prefix_log.csv', header=None)
    df.columns = ['Index', 'Event', 'Case_ID', 'Start', 'Finish', 'Resource']

    output_dir = 'data/output_files/1000'
    os.makedirs(output_dir, exist_ok=True)

    unique_case_ids = df['Case_ID'].unique()

    for i in range(0, len(unique_case_ids), 1000):
        case_ids_group = unique_case_ids[i:i + 1000]
        filtered_df = df[df['Case_ID'].isin(case_ids_group)]

        output_file = os.path.join(output_dir, f'filtered_prefix_log_{i // 1000 + 1}_1000.csv')
        filtered_df.to_csv(output_file, index=False, header=False)

        print(f'Saved {output_file}')


def get_some_prefixes_percentual():
    df = pd.read_csv('data/prefixes/prefix_log.csv', header=None)
    df.columns = ['Index', 'Event', 'Case_ID', 'Start', 'Finish', 'Resource']

    output_dir = 'data/output_files'
    os.makedirs(output_dir, exist_ok=True)

    unique_case_ids = df['Case_ID'].unique()

    total_case_ids = len(unique_case_ids)

    log_file_path = os.path.join(output_dir, 'summary_log.txt')
    with open(log_file_path, 'w') as log_file:
        log_file.write(f'Filtered files info\n\n')

    for i in range(1, 11):
        percentage = i * 0.1  # Percentage of case_ids to include, from 10% to 100%
        end_index = int(total_case_ids * percentage)

        case_ids_group = unique_case_ids[:end_index]

        filtered_df = df[df['Case_ID'].isin(case_ids_group)]

        num_nodes = len(filtered_df)
        avg_nodes_for_case = num_nodes / end_index

        output_file = os.path.join(output_dir, f'filtered_prefix_log_{i * 10}percent.csv')
        filtered_df.to_csv(output_file, index=False, header=False)

        # Monitoring RAM and CPU usage
        process = psutil.Process(os.getpid())
        memory_usage = process.memory_info().rss / 1024 / 1024  # Conversione in MB
        cpu_usage = psutil.cpu_percent(interval=1)

        with open(log_file_path, 'a') as log_file:
            log_file.write(f'File: filtered_prefix_log_{i * 10}percent.csv\n'
                           f'Number of graphs: {end_index}\n'
                           f'Number of nodes: {num_nodes}\n'
                           f'Average number of nodes: {avg_nodes_for_case:.2f}\n'
                           f'Memory usage: {memory_usage:.2f} MB, CPU usage: {cpu_usage:.2f}%\n\n')


def get_first_n_prefixes():
    df = pd.read_csv('data/prefixes/prefix_log.csv', header=None)
    df.columns = ['Index', 'Event', 'Case_ID', 'Start', 'Finish', 'Resource']
    first_case_ids = df['Case_ID'].unique()[:2000]
    filtered_df = df[df['Case_ID'].isin(first_case_ids)]
    filtered_df.to_csv('data/prefixes/prefix_log_2000.csv', sep=',', header=False, index=False)


# Define a function to monitor CPU and memory usage
def monitor_resources(stop_event, interval=1):
    process = psutil.Process(pid)
    cpu_usage = []
    memory_usage = []

    while not stop_event.is_set():
        cpu = process.cpu_percent(interval=interval)
        memory = process.memory_info().rss / (1024 ** 2)  # Convert to MB
        cpu_usage.append(cpu)
        memory_usage.append(memory)

        file_path = 'data\output_files\prefixes.txt'
        with open(file_path, 'a') as log_file:
            log_file.write(f"CPU: {cpu}% | Memory: {memory:.2f} MB\n")
        print(f"CPU: {cpu}% | Memory: {memory:.2f} MB")
    return cpu_usage, memory_usage


if __name__ == "__main__":
    # Load environment variables from .env file
    load_dotenv("config/database_conf.env")

    # Access the environment variables
    database_uri = os.getenv("DATABASE_URI")
    username = os.getenv("USERNAME_NEO4J")
    password = os.getenv("PASSWORD_NEO4J")

    # Connect to graph database
    connection = ActiveCaseGeneration(database_uri, username, password)
    stop_event = threading.Event()

    # Start monitoring in a separate thread
    monitor_thread = threading.Thread(target=monitor_resources, args=(stop_event,))
    monitor_thread.start()

    # Run the query
    results = connection.import_data2(stop_event)
    # Wait for the monitoring thread to finish
    monitor_thread.join()

    connection.close()

    # get_some_prefixes_percentual()
    # get_first_n_prefixes()
    # get_some_prefixes()
