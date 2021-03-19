import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv
from cassandra.cluster import Cluster

DB_NAME = 'sparkify'
TBLS_PROP = {
    "session_activity" : {
        "cols" : {
            "sessionId" : "int", 
            "itemInSession" : "int", 
            "artist" : "text", 
            "song" : "text", 
            "length" : "float"
        },
        "pkey": ["sessionId", "itemInSession"]
    },
    "user_activity" : {
        "cols" : {
            "userId" : "int", 
            "sessionId" : "int", 
            "itemInSession" : "int", 
            "artist" : "text", 
            "song" : "text",
            "firstName" : "text",
            "lastName" : "text"

        },
        "pkey": ["userId", "sessionId", "itemInSession"]
    },
    "song_play_history" : {
        "cols" : {
            "song" : "text", 
            "userId" : "int", 
            "firstName" : "text",
            "lastName" : "text"
        },
        "pkey": ["song", "userId"]
    }
}

COL_IDX_MAPPING = {
    "artist" : 0,
    "firstName" : 1,
    "gender" : 2,
    "itemInSession" : 3,
    "lastName" : 4,
    "length" : 5,
    "level" : 6,
    "location" : 7,
    "sessionId" : 8,
    "song" : 9,
    "userId" :10
}

def get_tbl_dml(tbl_name, TBLS_PROP):
    prop =  TBLS_PROP.get(tbl_name, None)
    columns = prop.get("cols").keys()
    placeholder = ",".join(["%s" for _ in columns])
    cols = ", ".join(columns)
    dml = "INSERT INTO {0} ({1}) VALUES ({2})".format(tbl_name, cols, placeholder)
    tpl_exp = ["line[{}]".format(COL_IDX_MAPPING.get(col)) for col in columns]
    tpl_exp = [prop["cols"][col]+"({})".format(tpl_exp) if prop["cols"][col]!='text' else tpl_exp for col, tpl_exp in zip(columns, tpl_exp)]
    return dml, tpl_exp

def populate_table(session, table_name, source_file):
    print(f'Populating Table : {table_name}')
    dml, tpl_exp = get_tbl_dml(tbl_name, TBLS_PROP)
    with open(source_file, encoding = 'utf8') as f:
        csvreader = csv.reader(f)
        next(csvreader) # skip header
        for line in csvreader:
            # for some weird reason tuple([eval(item) for item in tpl_exp]) is not working hence
            row = []
            for item in tpl_exp:
                row.append(eval(item))
            #session.execute(dml, tuple([eval(item) for item in tpl_exp]))
            session.execute(dml, tuple(row))

def get_table_ddl(TBLS_PROP, table_name):
    prop =  TBLS_PROP.get(table_name, None)
    ddl = f'CREATE TABLE IF NOT EXISTS {table_name} '
    
    col_details = ", ".join(["{} {}".format(col, dtype) for col, dtype in prop.get("cols").items()]) 
    col_details = "({}".format(col_details)
    ddl += col_details

    if prop.get('pkey'):
        ddl += ", PRIMARY KEY ({}));".format(", ".join(prop["pkey"]))
    return ddl

def create_table(session, table_name):
    print(f'Creating Table : {table_name}')
    sql = get_table_ddl(TBLS_PROP, table_name)
    try:
        session.execute(sql)
    except Exception as e:
        print(e)


def set_up_db(session, db_name):
    print(f'Creating Keyspace : {db_name}')
    db_conf = {'class': 'SimpleStrategy', 'replication_factor': 1}
    sql = f'CREATE KEYSPACE IF NOT EXISTS {db_name} WITH REPLICATION = {db_conf}'
    try:
        session.execute(sql)
        session.set_keyspace(db_name)
    except Exception as e:
        print(e)

def set_up_cluster():
    print('Setting up the Cassandra cluster')
    cluster = Cluster()
    session = cluster.connect()
    return session

def preprocess_data():
    print("Running preprocessing steps")
    data_dir = os.path.join(os.getcwd(),'event_data')
    filenames = glob.glob(os.path.join(data_dir,'*'), recursive=True)
    data = []
    for f in filenames:
        with open(f, 'r', encoding = 'utf8', newline='') as csvfile: 
            csvreader = csv.reader(csvfile) 
            next(csvreader)
            for line in csvreader:
                data.append(line) 
    
    csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)
    with open('event_datafile_new.csv', 'w', encoding = 'utf8', newline='') as f:
        writer = csv.writer(f, dialect='myDialect')
        writer.writerow(['artist','firstName','gender','itemInSession','lastName','length',\
                'level','location','sessionId','song','userId'])
        for row in data:
            if (row[0] != ''):
                writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))

def drop_table(session, table_name):
    try:
        session.execute(f'DROP TABLE IF EXISTS {table_name}')
        print(f'{table_name} dropped!')
    except Exception as e:
        print(f'{table_name} not dropped!')
        print(e)

if __name__=='__main__':
    preprocess_data()
    session = set_up_cluster()
    set_up_db(session, 'sparkify')
    for tbl_name in TBLS_PROP.keys():
        populate_table(session, tbl_name, 'event_datafile_new.csv')
    
    for tbl_name in TBLS_PROP.keys():
        populate_table(session, tbl_name, 'event_datafile_new.csv')
    
    query = "select firstName, lastName from song_play_history WHERE song = 'All Hands Against His Own'"
    try:
        rows = session.execute(query)
    except Exception as e:
        print(e)

    res = pd.DataFrame(rows._current_rows)
    d = {"firstname": ["Jacqueline", "Tegan", "Sara"], "lastname": ["Lynch", "Levine", "Johnson"]}
    expected = pd.DataFrame(d)
    assert res.equals(expected)

    for tbl_name in TBLS_PROP.keys():
        drop_table(session, tbl_name)
    
    query = "DROP KEYSPACE IF EXISTS sparkify"
    try:
        session.execute(query)
    except Exception as e:
        print(e)