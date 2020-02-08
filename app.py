# Import Python packages
import os
import glob
import csv
from CassandraETL.datatransactions import create_cluster


def get_records_list():
    """
    This function processed the csv files in the folder, creates a list of files with their paths.
    It then iterates over the file path list to extract information from individual csv files
    and add the information to a list
    :return: list of all data rows from the csv files
    """

    # Get your current folder and subfolder event data
    current_path = os.getcwd() + "\event_data"

    file_path_list = []

    # loop to create a list of files and collect each filepath to join to root/sub-directories
    for root, dirs, files in os.walk(current_path):
        file_path_list = glob.glob(os.path.join(root, '*'))

    # initiating an empty list to populate rows from all csv files
    all_data_rows = list()

    # reading csv file
    for file in file_path_list:
        with open(file, 'r', encoding='utf8', newline='') as fh:
            reader = csv.reader(fh)
            next(reader)

            # extracting each data row one by one and appending it
            for line in reader:
                all_data_rows.append(line)

    return all_data_rows


def consolidate_csv_file(records):
    """
    This functions extracts only relevant information from the records list and
    creates a consolidated csv file in the file reporsitory which is then used to add data to Cassandra
    :param records: list of records to be inserted in the csv file
    :return: None
    """
    csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

    # write relevant data to the csv file that will later be inserted into Apache Cassandra
    with open('event_datafile_new.csv', 'w', encoding='utf8', newline='') as fh:
        writer = csv.writer(fh)
        writer.writerow(['artist', 'firstName', 'gender', 'itemInSession', 'lastName', 'length','level', 'location',
                         'sessionId', 'song', 'userId'])

        for row in records:
            # if artist is not populated, do not write that row in csv file
            if not row[0]:
                continue
            writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))


if __name__ == '__main__':
    # populate the record list
    records_list = get_records_list()

    # if record list is not empty, create a consolidated csv file and then go on to perform operations on Cassandra
    if records_list:
        consolidate_csv_file(records_list)
        create_cluster()
