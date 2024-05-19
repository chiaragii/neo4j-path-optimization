import os
import time
import pandas as pd
from dotenv import load_dotenv
from neo4j import GraphDatabase
import neo4j


class ActiveCaseGeneration:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def print_greeting(self, message):
        with self.driver.session() as session:
            greeting = session.execute_write(self._create_and_return_greeting, message)
            print(greeting)

    # function to format the DateTime object
    def format_date(self, dt):
        if dt is not None:
            iso_str = dt.iso_format()
            formatted_str = iso_str.replace("T", "").replace("Z", "+00:00")
            return formatted_str
        return None

    @staticmethod
    def _create_and_return_greeting(tx, message):
        result = tx.run("MATCH (e:Event) RETURN e.event_name", message=message)
        return result.single()[0]

    def create_prefixes(self):
        results = []
        for i in range(1, 100):
            result = self.driver.execute_query(
                f"MATCH p = (s:Event) - [r *]->(e:Event) "
                f"WHERE e.activity_id = {i} AND s.event_name = 'START' "
                f"RETURN collect(nodes(p)) as nodes, collect(r) as relationship, e.track_id "
                f"ORDER BY e.track_id",
                database_="neo4j",
                result_transformer_=neo4j.Result.to_df)
            for res in result:
                results.append(res)
        return results

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
            "RETURN DISTINCT node.track_id as track_id, node.activity_id as activity_id, start_time_prefix, "
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
    username = os.getenv("USER")
    password = os.getenv("PASSWORD")

    # Now you can use these variables to connect to your database
    connection = ActiveCaseGeneration(database_uri, username, password)
    # result, time_taken = connection.generate_active_case()
    # connection.active_case_neo4j()
    # greeter.print_greeting("hello, world")
    results = connection.active_case_neo4j()
    # print(results)
    connection.close()

    active_case_db()
