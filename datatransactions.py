from cassandra.cluster import Cluster
import csv
import pandas as pd

# Show the following message if this file is run directly
if __name__ == '__main__':
    print("To execute this code, run app.py")

# file from which data will be extracted and inserted into Apache Cassandra
file_name = 'event_datafile_new.csv'


def create_cluster():
    """
    This function does the following things:
        - Creates Apache Cassandra cluster and keyspace.
        - Creates 3 tables, inserts records in them and runs the requested data analysis
        - Drops the tables after analysis and shuts down the resources created
    :return: None
    """
    cluster = Cluster(['localhost'])

    # create session to establish connection and begin executing queries
    session = cluster.connect()

    # create and set keyspace
    session.execute("CREATE KEYSPACE IF NOT EXISTS sparkifydb WITH REPLICATION = "
                    "{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }")
    session.set_keyspace('sparkifydb')

    # call the data analysis emthods
    get_records_by_session_sessionitems(session)
    get_records_by_user_session(session)
    get_records_by_song(session)

    # Drop the tables created above after the analysis output is displayed
    drop_tables(session)

    # shutdown the resources
    session.shutdown()
    cluster.shutdown()


def get_records_by_session_sessionitems(session):
    """
    Analysis requirement: Retrieve the artist, song title and song's length in the music app history that was heard in
                                sessionId = 338, and itemInSession = 4

    PRIMARY KEY LOGIC: For the query requirement, a record is uniquely identified by sessionId and itemInSession.
            Since the requirement is to filter on 2 columns, we can distribute the data across nodes by sessionId as
                partition key and uniquely identify each record by clustering on itemInSession

    This function creates a table with columns listed in the analysis requirements,
        extracts records from the consolidated csv, inserts them into music_lib table and runs  data retrieval query

    :param session: Apache Cassandra cluster session
    :return: None
    """
    query = "CREATE TABLE IF NOT EXISTS data_by_session_itemSession "
    query = query + "(sessionId text, itemInSession text, song text, artist text, length text, " \
                    "PRIMARY KEY (sessionId, itemInSession))"
    session.execute(query)

    with open(file_name, 'r', encoding='utf-8') as fh:
        reader = csv.reader(fh)
        next(reader)

        for line in reader:
            query = "INSERT INTO data_by_session_itemSession (sessionId, itemInSession, song, artist, length)"
            query = query + " VALUES (%s, %s, %s, %s, %s)"

            session.execute(query, (line[8], line[3], line[9], line[0], line[5]))

    query = "SELECT artist, song, length FROM data_by_session_itemSession WHERE sessionId = '338' and itemInSession = '4'"
    df = pd.DataFrame(list(session.execute(query)))
    print(df)


def get_records_by_user_session(session):
    """
    Analysis requirement: Retrieve name of artist, song (sorted by itemInSession) and user (first and last name) for
                                userid = 10, sessionid = 182

    PRIMARY KEY SELECTION LOGIC: For the query requirement, a record is uniquely identified by userId, sessionId and
            itemInSession. Since there is add-on requirement to sort by itemInSession, we create a clustering key on it.
                In this case, data will be distributed across nodes by composite partition key (userId, sessionId)
                    and within each node, data will be sorted by itemInSession clustering key

    This function creates a table with columns listed in the analysis requirements,
        extracts records from the consolidated csv, inserts them into music_lib table and runs  data retrieval query

    :param session: Apache Cassandra cluster session
    :return: None
    """
    query = "CREATE TABLE IF NOT EXISTS data_by_user_session "
    query = query + "(userId text, sessionId text, itemInSession text, artist text, song text, firstName text, " \
                    "lastName text, PRIMARY KEY ((userId, sessionId), itemInSession))"
    session.execute(query)

    with open(file_name, 'r', encoding='utf8') as f:
        reader = csv.reader(f)
        next(reader) # skip header
        for line in reader:
            query = "INSERT INTO data_by_user_session (userId, sessionId, itemInSession, artist, song, firstName, lastName)"
            query = query + " VALUES (%s, %s, %s, %s, %s, %s, %s)"
            session.execute(query, (line[10], line[8], line[3], line[0], line[9], line[1], line[4]))

    query = "SELECT artist, song, firstname, lastname FROM data_by_user_session WHERE userId = '10' and sessionId = '182'"
    df = pd.DataFrame(list(session.execute(query)))
    print(df)


def get_records_by_song(session):
    """
    Analysis requirement: Retrieve every user name (first and last) in my music app history who listened to the
                            song 'All Hands Against His Own'

    PRIMARY KEY LOGIC: For the query requirement, a record is uniquely identified by song and userId. Since requirement
            is to filter on 1 columns, we can distribute the data across nodes by song as partition key and uniquely
                identify each record by clustering on userId in order to retrieve user names who have listened to a song

    This function creates a table with columns listed in the analysis requirements,
        extracts records from the consolidated csv, inserts them into music_lib table and runs  data retrieval query

    :param session: Apache Cassandra cluster session
    :return: None
    """
    query = "CREATE TABLE IF NOT EXISTS data_by_user_song "
    query = query + "(song text, userId text, firstName text, lastName text, PRIMARY KEY (song, userId))"
    session.execute(query)

    with open(file_name, 'r', encoding='utf8') as f:
        reader = csv.reader(f)
        next(reader) # skip header
        for line in reader:
            query = "INSERT INTO data_by_user_song (song, userId, firstName, lastName)"
            query = query + " VALUES (%s, %s, %s, %s)"
            session.execute(query, (line[9], line[10], line[1], line[4]))

    query = "SELECT firstname, lastname FROM data_by_user_song WHERE song = 'All Hands Against His Own'"
    df = pd.DataFrame(list(session.execute(query)))
    print(df)


def drop_tables(session):
    """
    Drop the tables after the analysis is complete
    :param session: Apache Cassandra cluster session
    :return: None
    """
    query = "DROP TABLE data_by_session_itemSession"
    session.execute(query)

    query = "DROP TABLE data_by_user_session"
    session.execute(query)

    query = "DROP TABLE data_by_user_song"
    session.execute(query)




