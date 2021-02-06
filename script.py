# Imports
import requests
from xml.etree import ElementTree
import argparse
import psycopg2
import json
import copy


# Get deployment Info
def get_deployment_info():
    url = '{0}/cockpit/rest/deployment/{1}/resources'.format(
        engine_url, dep_id)
    try:
        obj1 = requests.get(url)
        response = obj1.json()  # arry of one json contain id - name - depId
        if response.__len__() == 1:  # Check that respone is an arry of one json
            global fileName
            global fileId
            fileName = response[0]['name']
            fileId = response[0]['id']
    except Exception as error:
        print('Error: ', error)


# Get Xml File
def get_xml_object():
    url = '{0}/cockpit/rest/deployment/{1}/resources/{2}/data'.format(
        engine_url, dep_id, fileId)
    try:
        obj2 = requests.get(url)  # get xml file
        with open(f'{fileName}', '+w') as handle:
            handle.write(obj2.text)
    except Exception as error:
        print('Error: ', error)


# Post Configrations
def post_xml():
    try:
        global depName
        url = '{0}/cockpit/rest/deployment/create'.format(target_url)
        depName = fileName[0:fileName.index('.')]
        payload = {'deployment-name': depName}
        files = [('upload', open(fileName, 'rb'))]
        headers = {}
        response = requests.post(
            url, data=payload, headers=headers, files=files)
    except Exception as error:
        print('Error: ', error)


# Configure DB
def connector(host, port, user, password, dbname):
    try:
        global connection
        connection = psycopg2.connect(
            host=host, port=port, user=user, password=password, dbname=dbname, connect_timeout=10)
    except Exception as error:
        print('Error: ', error)


def get_max_val(connection):
    try:
        if connection:
            global maxId
            connection.autocommit = True
            cursor = connection.cursor()
            query = "SELECT max(id) FROM public.config"
            cursor.execute(query)
            connection.commit()
            results = cursor.fetchall()
            maxId = results[0][0]
    except Exception as error:
        print('Error: ', error)


def get_deployment_data(connection):
    try:
        if connection:
            global result
            connection.autocommit = True
            cursor = connection.cursor()
            query = "SELECT data, path FROM public.config where path like '%processesConfig/{0}%'".format(
                depName)
            cursor.execute(query)
            connection.commit()
            result = cursor.fetchall()
    except Exception as error:
        print('Error: ', error)


def post_deployment_data(result, maxId):
    try:
        if connection:
            connection.autocommit = True
            cursor = connection.cursor()
            ids = []
            data = []
            paths = []
            for i in range(len(result)):
                if maxId == None:
                    maxId = 0
                ids.append(maxId+i+1)
                data.append(json.dumps(result[i][0]))
                paths.append(result[i][1])
            records = []
            for i in range(len(ids)):
                row = []
                rowStart = '('
                rowBody = str(ids[i]) + ',' + '\'' + str(data[i]) + \
                    '\'' + ',' + '\'' + str(paths[i]) + '\''
                rowEnd = ')'
                records.append(rowStart + rowBody + rowEnd)
            query = "INSERT INTO public.config(id, data, path) VALUES {0}".format(
                ','.join(records))
            cursor.execute(query)
            connection.commit()
    except Exception as error:
        print('Error: ', error)


if __name__ == '__main__':
    # Creating argument parser
    my_parser = argparse.ArgumentParser(allow_abbrev=False)
    my_parser.add_argument('-us', '--sourceurl', action='store',
                           type=str, required=True)
    my_parser.add_argument('-ut', '--targeturl', action='store',
                           type=str, required=True)
    my_parser.add_argument(
        '-fid', '--fileid', action='store', type=str, required=True)
    my_parser.add_argument(
        '-ds', '--sourcedb', action='store', type=str, required=True)
    my_parser.add_argument(
        '-dt', '--targetdb', action='store', type=str, required=True)
    # Getting arguments
    args = my_parser.parse_args()
    # Defining function variables
    engine_url = args.sourceurl
    target_url = args.targeturl
    dep_id = args.fileid
    source_db_conn = args.sourcedb
    source_db_conn = source_db_conn.split(',')
    source_host = source_db_conn[0]
    source_port = source_db_conn[1]
    source_user = source_db_conn[2]
    source_passwd = source_db_conn[3]
    source_db = source_db_conn[4]
    target_db_conn = args.targetdb
    target_db_conn = target_db_conn.split(',')
    target_host = target_db_conn[0]
    target_port = target_db_conn[1]
    target_user = target_db_conn[2]
    target_passwd = target_db_conn[3]
    target_db = target_db_conn[4]
    # Start Functions
    get_deployment_info()
    get_xml_object()
    post_xml()
    # Get source db info
    connector(source_host, source_port, source_user, source_passwd, source_db)
    get_deployment_data(connection)
    connection.cursor.close()
    connection.close()
    connector(target_host, target_port, target_user, target_passwd, target_db)
    get_max_val(connection)
    post_deployment_data(result, maxId)
    connection.cursor.close()
    connection.close()
