# Data Modeling with Apache Cassandra

The goal of this project is to analyze song listening activity data of users. Currently the user activity data resides in multiple csv files. This project creates an ETL pipeline to read data from several csv files and stores it in Apache Cassandra datastore in order to run analysis queries.

## Getting Started

Cloning the repository will get you a copy of the project up and running on your local machine for development and testing purposes. 

- `git@github.com:ypatankar/CassandraETL.git`
- `https://github.com/ypatankar/CassandraETL.git`

### Prerequisites

* python 3.7
* Apache Cassandra cluster

### Contents

* `app.py` : Creates all_events.csv which is a consolidated csv file of all the user activity files and 
* `dataconnections.py` : Creates keyspaces, tables on Apache Cassandra cluster. The records are inserted in the datastore and analysis queries are run.
* `event_data` folder : Contains all the user activity csv files


### Analysis and Data Model
* Retrieve the artist, song title and song's length in the music app history that was heard in sessionId = 338 and itemInSession = 4
Structure for table `data_by_session_itemSession` : sessionId text, itemInSession text, song text, artist text, length text, PRIMARY KEY (sessionId, itemInSession)

* Retrieve name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182
Structure for table `data_by_user_session` : userId text, sessionId text, itemInSession text, artist text, song text, firstName text, lastName text, PRIMARY KEY ((userId, sessionId), itemInSession)

* Retrieve every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'
Structure for table `data_by_user_song` : song text, userId text, firstName text, lastName text, PRIMARY KEY (song, userId)

### Execution Steps
Execute `app.py` to create `event_data_new.csv` (a consolidated csv file of all the user activity files) and to create keyspaces, tables on Apache Cassandra cluster. The records are inserted in the datastore and analysis queries are executed.
