import os
import time
import pandas
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

    @staticmethod
    def _create_and_return_greeting(tx, message):
        result = tx.run("MATCH (e:Event) RETURN e.event_name", message=message)
        return result.single()[0]

    def create_prefixes(self):
        results = []
        for i in range(1, 100):
            result = self.driver.execute_query(f"MATCH p = (s:Event) - [r *]->(e:Event) "
                                               f"WHERE e.activity_id = {i} AND s.event_name = 'START' "
                                               f"RETURN collect(nodes(p)), collect(r), e.track_id ORDER BY e.track_id",
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
        time_taken = end-start
        return result, time_taken


if __name__ == "__main__":
    # Load environment variables from .env file
    load_dotenv("config/database_conf.env")

    # Access the environment variables
    database_uri = os.getenv("DATABASE_URI")
    username = os.getenv("USERNAME")
    password = os.getenv("PASSWORD")

    # Now you can use these variables to connect to your database
    connection = ActiveCaseGeneration(database_uri, username, password)
    # result, time_taken = connection.generate_active_case()
    connection.create_prefixes()
    # greeter.print_greeting("hello, world")
    results = connection.create_prefixes()
    print(results)
    connection.close()
