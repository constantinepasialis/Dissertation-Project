
import psycopg2

# Database connection class 
class DBConnector:

    # Initialize the DBConnector with the database URI
    def __init__(self, uri):
        self.uri = uri
        self.connection = None

    # initialize connection
    def __enter__(self):
        self.connection = psycopg2.connect(self.uri)
        return self.connection

    # Close the connection when exit
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connection.close()