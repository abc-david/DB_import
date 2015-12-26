'''
Created on 23 july. 2014

@author: david
'''
#!/usr/bin/env python
# -*- coding: utf-8 -*-

import psycopg2
import pandas as pd
from functions_log import *

def DB_direct_connection(db_package):
    #direct_DB_conn = psycopg2.connect("dbname='postgres' user='postgres' host='192.168.0.52' password='postgres'")
    connect_token = "dbname='" + db_package[0] + "' user='" + db_package[1] + \
                    "' host='" + db_package[2] + "' password='" + db_package[3] + "'"
    try:
        global direct_DB_conn
        direct_DB_conn = psycopg2.connect(connect_token)
        global direct_DB_cursor
        direct_DB_cursor = direct_DB_conn.cursor()
        return [True, direct_DB_conn, direct_DB_cursor]
    except:
        return [False]

def create_index(db_schema = ""):
    try:
        if db_schema:
            direct_DB_cursor.execute("CREATE INDEX " + db_schema + "_mail5 ON " + db_schema + \
                                     """.base USING btree ("left"(mail::text, 5) COLLATE pg_catalog."default" );""")
        else:
            direct_DB_cursor.execute("""CREATE INDEX mail5 ON base USING btree ("left"(mail::text, 5) COLLATE pg_catalog."default" );""")
        message = "OK : Index created."
        print_to_log(log_file, 2, message)
        return True
    except:
        message = "Failed to create index."
        print_to_log(log_file, 2, message)
        return False
    
def drop_index(db_schema = ""):
    try:
        if db_schema:
            direct_DB_cursor.execute("DROP INDEX " + db_schema + "_mail5;")
        else:
            direct_DB_cursor.execute("""DROP INDEX mail5;""")
        message = "OK : Index dropped."
        print_to_log(log_file, 2, message)
    except:
        direct_DB_conn.rollback()
        message = "Failed to drop index."
        print_to_log(log_file, 2, message)
        e = sys.exc_info()
        for item in e:
            message = str(item)
            print_to_log(log_file, 3, message)

def modify_isolation_level(level):
    old_isolation_level = direct_DB_conn.isolation_level
    try:
        direct_DB_conn.set_isolation_level(level)
        #message = "OK : Isolation level set to " + str(level) + "."
        #print_to_log(log_file, 2, message)
        return old_isolation_level
    except:
        message = "Failed to set isolation level to " + str(level) + "."
        print_to_log(log_file, 2, message)

def revert_isolation_level(old_level):
    try:
        direct_DB_conn.set_isolation_level(old_level)
        #message = "OK : Isolation level reverted to previous level (" + str(old_level) + ")."
        #print_to_log(log_file, 2, message)
    except:
        message = "Failed to revert to old isolation level (" + str(old_level) + ")."
        print_to_log(log_file, 2, message)

def prepare_lookup_query(table, return_field, known_field, db_schema = ""):
    if db_schema:
        name_table = db_schema + "_" + table
        table = db_schema + "." + table
    else:
        name_table = table
    query_name = "search_" + return_field + "_in_" + name_table
    prepared_query = "PREPARE " + query_name + " AS SELECT " + return_field + " FROM " + \
        table + " WHERE " + known_field + " = $1;"
    try:
        old_level = modify_isolation_level(0)
        direct_DB_conn.cursor().execute(prepared_query)
        message = "OK : Lookup query prepared : " + query_name
        print_to_log(log_file, 2, message)
        revert_isolation_level(old_level)
        need_to_prepare_lookup_query = False
        lookup_query_name = query_name
        return [True, query_name]
    except:
        message = "Failed to prepare lookup query. Check arguments passed to the function."
        print_to_log(log_file, 2, message)
        e = sys.exc_info()
        for item in e:
            message = str(item)
            print_to_log(log_file, 3, message)
        return [False]

def close_DB_connection():
    direct_DB_cursor.close()
    direct_DB_conn.close()
    need_to_prepare_lookup_query = True
    lookup_query_name = ""
    need_to_prepare_update_query = True
    prepare_query_name = ""

def test_DB_connection(conn = "", db_package = ""):
    if conn:
        DB_cursor = conn.cursor()
    else:
        if 'direct_DB_cursor' in globals():
            DB_cursor = direct_DB_cursor
        elif 'direct_DB_conn' in globals():
            DB_cursor = direct_DB_conn.cursor()
        else:
            message = "Pb. No DB connection defined in globals(). Will open one with db_package."
            print_to_log(log_file, 2, message)
            try:
                return DB_direct_connection(db_package)
            except:
                message = "Pb. Failed to open connection with DB."
                print_to_log(log_file, 2, message)
                return [False]
    try:
        DB_cursor.execute("""SELECT 1;""")
        return [True]
    except:
        message = "Pb. Connection closed. Will open one with db_package."
        print_to_log(log_file, 2, message)
        try:
            return DB_direct_connection(db_package)
        except:
            message = "Pb. Failed to open connection with DB."
            print_to_log(log_file, 2, message)
            return [False]

def unify_id_table(conn):
    from import_functions_OVH_DB import *
    query = "SELECT " + \
            "i.mail_id," + \
            "array_to_string(array_agg(distinct i.prenom),',') AS prenom," + \
            "array_to_string(array_agg(distinct i.nom),',') AS nom," + \
            "array_to_string(array_agg(distinct i.civilite),',') AS civilite," + \
            "array_to_string(array_agg(distinct i.birth),',') AS birth," + \
            "array_to_string(array_agg(distinct i.cp),',') AS cp," + \
            "array_to_string(array_agg(distinct i.ville),',') AS ville " + \
            "FROM id as i GROUP BY i.mail_id LIMIT 1000;"
    df = pd.read_sql(query, conn, coerce_float=False)
    show_df(df)
    file_name = create_file_name("PYT", 'id_unik', len(df.index), header = list(df.columns), comment = "")
    csv_file = write_to_csv(df, "/home/david/csv_files", "id_unik", file_name, header = False)
    print write_csv_to_DB("id_unik", csv_file, list(df.columns), db_schema = "")

    return df


db_name = "prod" #"postgres"
db_user = "postgres"
db_host = "localhost"
db_pass = "penny9690"
global db_package
global direct_DB_conn
db_package = [db_name, db_user, db_host, db_pass]
db_connect = DB_direct_connection(db_package)
if db_connect[0]:
    direct_DB_conn = db_connect[1]
    direct_DB_cur = db_connect[2]
    df = unify_id_table(direct_DB_conn)