from datetime import datetime

# Logger class to log messages into a database
class Logger:
    # Logger intialization with a database connector and optional schema name
    def __init__(self, connector, schema_name="logging"):
        self.connector = connector
        self.schema = schema_name
        self._initialize_db()

    # Private method to initialize the database schema and table for logging
    def _initialize_db(self):
        with self.connector as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {self.schema};")
                
                create_query = f"""
                CREATE TABLE IF NOT EXISTS {self.schema}.system_logs (
                    timestamp TIMESTAMP NOT NULL,
                    level TEXT NOT NULL,
                    source TEXT NOT NULL,
                    message TEXT NOT NULL
                );
                """
                cursor.execute(create_query)
            conn.commit()

    # Method to log a message with a specific level and source into the database    
    def log(self, level, source, message):
        insert_query = f"INSERT INTO {self.schema}.system_logs VALUES (%s, %s, %s, %s)"
        with self.connector as conn:
            with conn.cursor() as cursor:
                cursor.execute(insert_query, (datetime.now(), level, source, message))
            conn.commit()
