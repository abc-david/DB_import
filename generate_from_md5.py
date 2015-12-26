'''
Created on 27 apr. 2014

@author: david
'''
#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import time
import codecs
import pandas as pd
from pandas.io import sql
import psycopg2
from import_functions import convert_dataframe_scalar, remove_floating_part
from export_regie_functions import fix_year_problem_df

clock_0 = time.clock()
clock_1 = time.clock()
clock_2 = time.clock()
clock_3 = time.clock()
clock_4 = time.clock()
clock_5 = time.clock()
clock_6 = time.clock()
clock_container_dict = {0 : clock_0, 1 : clock_1, 2 : clock_2, 3 : clock_3, 4 : clock_4, \
                        5 : clock_5, 6 : clock_6}
for clock_item in clock_container_dict.itervalues():
    clock_item = time.clock()
log_path = "/media/freebox/Fichiers/ImportDB/Pandas/Test/log/"
log_file = codecs.open(log_path + "log_test_import_functions.txt", 'a', encoding='utf-8')

def new_format_seconds(seconds):
    #ms_str = str(seconds - int(seconds))
    ms = seconds - int(seconds)
    ms_prep = int(ms * 1000)
    s = int(seconds)
    if s == 0:
        return '%03ims' % (ms_prep)
    else:
        if seconds > 60:
            m, s = divmod(seconds, 60)
            if m > 60:
                h, m = divmod(m, 60)
                return '%02ih%02im%02is' % (h, m, s)
            else:
                return '%02im%02is' % (m, s)
        else:
            return '%02is%03i' % (s, ms_prep)

def print_to_log(log_file, indent, message, silent = False):
    if silent:
        return
    indent_size = 4
    space = " " * indent_size
    now = time.clock()
    elapsed = now - clock_container_dict[indent]
    clock_container_dict[indent] = now
    elapsed_str = new_format_seconds(elapsed)
    if indent in [0, 1, 2]:
        output = (space * indent) + "(" + elapsed_str + ") " + message
        #log_file.write(output + '\n')
    elif indent == 3:
        output = (space * indent) + message
        #log_file.write(output + '\n')
    elif indent >= 4:
        output = (space * (indent - 1)) + "(" + elapsed_str + ") " + message
        #log_file.write(output + '\n')
    print output

def show_df(df, n_line = 5):
    #pd.set_option('display.max_columns', 20)
    print df.head(n_line)
    print str(len(df.index)) + " lines."
    
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
        message = "Failed to connect to DB"
        print_to_log(log_file, 2, message)
        e = sys.exc_info()
        for item in e:
            message = str(item)
            print_to_log(log_file, 3, message)
        return [False]

def modify_isolation_level(level, direct_DB_conn):
    old_isolation_level = direct_DB_conn.isolation_level
    try:
        direct_DB_conn.set_isolation_level(level)
        #message = "OK : Isolation level set to " + str(level) + "."
        #print_to_log(log_file, 2, message)
        return old_isolation_level
    except:
        message = "Failed to set isolation level to " + str(level) + "."
        print_to_log(log_file, 2, message)

def revert_isolation_level(old_level, direct_DB_conn):
    try:
        direct_DB_conn.set_isolation_level(old_level)
        #message = "OK : Isolation level reverted to previous level (" + str(old_level) + ")."
        #print_to_log(log_file, 2, message)
    except:
        message = "Failed to revert to old isolation level (" + str(old_level) + ")."
        print_to_log(log_file, 2, message)

def prepare_lookup_query(table, return_field, known_field, direct_DB_conn):
    query_name = "search_" + return_field + "_in_" + table
    prepared_query = "PREPARE " + query_name + " AS SELECT " + return_field + " FROM " + \
        table + " WHERE " + known_field + " = $1;"
    try:
        old_level = modify_isolation_level(0, direct_DB_conn)
        direct_DB_conn.cursor().execute(prepared_query)
        message = "OK : Lookup query prepared : " + query_name
        print_to_log(log_file, 2, message)
        revert_isolation_level(old_level, direct_DB_conn)
        return [True, query_name]
    except:
        message = "Failed to prepare lookup query. Check arguments passed to the function."
        print message
        e = sys.exc_info()
        for item in e:
            message = str(item)
            print_to_log(log_file, 3, message)
        return [False]
            
def prepare_custom_lookup_query(custom_query, direct_DB_conn, query_name = "custom_query"):
    prepared_query = "PREPARE " + query_name + " AS " + custom_query + " = $1;"
    try:
        old_level = modify_isolation_level(0, direct_DB_conn)
        direct_DB_conn.cursor().execute(prepared_query)
        message = "OK : Lookup query prepared : " + query_name
        print_to_log(log_file, 2, message)
        revert_isolation_level(old_level, direct_DB_conn)
        return [True, query_name]
    except:
        message = "Failed to prepare lookup query. Check arguments passed to the function."
        print message
        e = sys.exc_info()
        for item in e:
            message = str(item)
            print_to_log(log_file, 3, message)
        return [False]
            
def lookup_record_safe(query_name, where_value, direct_DB_cursor, convert_res_to_int = True):
    mail_lookup_query = "EXECUTE " + query_name + " (%s);"
    try:
        direct_DB_cursor.execute(mail_lookup_query, (where_value, ))
        records = direct_DB_cursor.fetchall()
        #print records
        if records == []:
            return [False]
        else:
            extract_result = str(records[0]).strip('()').strip(',')
            #print extract_result
            if convert_res_to_int:
                extract_result = int(extract_result)
            else:
                extract_result = extract_result.strip("'")
            #print extract_result
            return [True, extract_result]
    except:
        return [False, where_value]
        message = "Failed to execute " + query_name + " with this input : " + where_value
        print_to_log(log_file, 2, message)
        
def custom_lookup_record_safe(query_name, where_value, direct_DB_cursor, convert_to_list = True):
    custom_lookup_query = "EXECUTE " + query_name + " (%s);"
    try:
        direct_DB_cursor.execute(custom_lookup_query, (where_value, ))
        records = direct_DB_cursor.fetchall()
        print records
        if records == []:
            return [False]
        else:
            if convert_to_list:
                if len(records) == 1:
                    extract_result = list(records[0])
                elif len(records) > 1:
                    extract_result = []
                    for record in records:
                        extract_result.append(list(record))
            else:
                extract_result = str(records[0]).strip('()').strip(',')
                print extract_result
                extract_result = extract_result.strip("'")
            print extract_result
            return [True, extract_result]
    except:
        return [False, where_value]
        message = "Failed to execute " + query_name + " with this input : " + where_value
        print_to_log(log_file, 2, message)
        
def lookup_single_columnwide(dataframe, query_name, return_field, known_field, \
                             direct_DB_cursor, convert_res_to_int = True, index_slice = "", \
                             add_return_field_column = True):
    milestones = {1 : 1000, 2 : 2000, 3 : 5000, 4 : 10000, 5 : 20000, 6 : 50000, \
                  7 : 100000, 8 : 200000, 9 : 500000, 10 : 1000000, 11 : 2000000, 12: 5000000}
    cpt_milestones = 1
    if add_return_field_column == True:
        dataframe[return_field] = ""
    if not index_slice:
        index_slice = dataframe.index
    number_lookups = len(index_slice)
    if number_lookups != 0:
        cpt_lookup = 0
        clock_4 = time.clock()
        for cpt in range(len(index_slice)):
            this_field = dataframe.at[index_slice[cpt], known_field]
            if query_name == "custom_query":
                lookup_result = custom_lookup_record_safe(query_name, this_field, direct_DB_cursor)
            else:
                lookup_result = lookup_record_safe(query_name, this_field, direct_DB_cursor, convert_res_to_int)
            #print str(cpt_lookup), mail, str(lookup_result[1])
            cpt_lookup += 1
            if cpt_lookup == milestones[cpt_milestones]:
                message = "OK. First " + str(milestones[cpt_milestones]) + " mails looked up."
                print_to_log(log_file, 4, message)
                cpt_milestones += 1
            if lookup_result[0]:
                #dataframe.at[index_slice[cpt], 'exist'] = True
                dataframe.at[index_slice[cpt], return_field] = lookup_result[1]
            else:
                if len(lookup_result) == 2:
                    pass
                    #dataframe.at[index_slice[cpt], 'fail'] = True
        message = "OK : Mails associated with existing records in table 'base'."
        print_to_log(log_file, 2, message)
        #print "-- DEBUG -- lookup_mail_columnwide : dataframe --"
        #print dataframe
        #print "--"
        return [True, dataframe]
    else:
        return "No records to lookup : DataFrame (or subset) contains ZERO record."
        print_to_log(log_file, 2, message)
        return [False]

def old_populate_dataframe(file_path, file_name, sep = "", attempt_number = "", \
                       header = False, skiprows = "", index_col = "", silent = False):
    if not attempt_number:
        this_attempt = 1
    else:
        this_attempt = attempt_number
    csv_file = file_path + "/" + file_name
    encoding_dict = {1:'latin-1', 2:'utf-8'}
    if sep == "":
        sep = ";"
    try:
        if index_col:
            dataframe = pd.read_csv(csv_file, encoding = encoding_dict[attempt_number], \
                                    header = None, sep = sep, dtype = object, skiprows = skiprows, \
                                    index_col = index_col)
        elif header == True:
            dataframe = pd.read_csv(csv_file, encoding = encoding_dict[attempt_number], \
                                    sep = sep, dtype = object, skiprows = skiprows)
        else:
            dataframe = pd.read_csv(csv_file, encoding = encoding_dict[attempt_number], \
                                    header = None, sep = sep, dtype = object, skiprows = skiprows)
    except:
        message = "Failed to open " + csv_file
        print_to_log(log_file, 2, message, silent)
        e = sys.exc_info()
        for item in e:
            message = str(item)
            print_to_log(log_file, 3, message, silent)
        if this_attempt < len(encoding_dict):
            this_attempt += 1
            message = "Other attempt to open file with '" + str(encoding_dict[this_attempt]) + \
                        "' encoding instead of '" + str(encoding_dict[this_attempt-1]) +"'."
            print_to_log(log_file, 2, message, silent)
            return old_populate_dataframe(file_path, file_name, sep = sep, attempt_number = this_attempt, \
                                      header = header, skiprows = skiprows, index_col = index_col, silent = silent)
        else:
            return [False]
    if "dataframe" in locals():
        if not dataframe.empty:
            if len(dataframe.columns) == 1:
                py_dataframe_test = convert_dataframe_scalar(dataframe.iat[0,0])[1]
                if py_dataframe_test.count(",") > 1:
                    return old_populate_dataframe(file_path, file_name, sep = ",", attempt_number = this_attempt, \
                                              header = header, skiprows = skiprows, index_col = index_col, silent = silent)
    message = "OK : DataFrame populated from file : " + file_name
    print_to_log(log_file, 2, message, silent)
    #print dataframe
    #dataframe = dataframe.str.strip()
    return [True, dataframe]

def write_csv_to_DB(write_table, csv_file, csv_columns, direct_DB_conn, direct_DB_cur, db_schema = ""):
    try:
        csv_reader = codecs.open(csv_file, 'r', encoding='utf-8')
    except:
        return [False]
    try:
        print csv_columns
        direct_DB_cursor.copy_from(file = csv_reader, table = write_table, 
                                   sep = ";", null = "", columns = csv_columns)
        direct_DB_conn.commit()
        message = "OK : csv imported to DB in table : " + write_table
        print_to_log(log_file, 2, message)
        return [True]
    except:
        message = "Failed to import csv in table : " + write_table
        print_to_log(log_file, 2, message)
        e = sys.exc_info()
        for item in e:
            message = str(item)
            print_to_log(log_file, 3, message)
        

db_name = "test_base"
db_user = "postgres"
db_host = "192.168.0.52"
db_pass = "postgres"
db_package = [db_name, db_user, db_host, db_pass]
db_connect = DB_direct_connection(db_package)
if db_connect[0]:
    conn = db_connect[1]
    cur = db_connect[2]

"""
prep_query_name = prepare_lookup_query('md5', 'mail_id', 'md5', conn)
if prep_query_name[0]:
    query_id = prep_query_name[1]

prep_query_name = prepare_lookup_query('base', 'mail', 'id', conn)
if prep_query_name[0]:
    query_mail = prep_query_name[1]
    
query_all_fields = "SELECT base.mail, id.prenom, id.nom, id.civilite, id.birth, id.cp, id.ville, lead.ip, lead.provenance, lead.date FROM base " + \
"LEFT JOIN id ON base.id = id.mail_id " + \
"LEFT JOIN lead ON lead.mail_id = id.mail_id WHERE base.id"
prep_query_name = prepare_custom_lookup_query(query_all_fields, direct_DB_conn)
if prep_query_name[0]:
    query_fields = prep_query_name[1]

file_path = "/media/freebox/Fichiers/Export Regies/Raffles"
file_name = "md5-unknown-raffles_15-juil-14.csv"
df = pd.read_csv(file_path + "/" + file_name)
show_df(df)

lookup_id_res = lookup_single_columnwide(df, query_id, 'mail_id', 'md5', cur)
if lookup_id_res[0]:
    df = lookup_id_res[1]
    show_df(df)
    
lookup_mail_res = lookup_single_columnwide(df, query_mail, 'mail', 'mail_id', cur, convert_res_to_int = False)
if lookup_mail_res[0]:
    df = lookup_mail_res[1]
    show_df(df)
    
df_mail = df.drop(['md5', 'mail_id'], 1)
mail_id_file = "Export-Raffles_15-juil-14_[mail_id]_unknown-md5.csv"
df_mail = df.drop(['md5'], 1)
show_df(df_mail)
df_mail.to_csv(file_path + "/" + mail_id_file, index = False)


file_path = "/media/freebox/Fichiers/Export Regies/Raffles"
mail_id_file = "Export-Raffles_15-juil-14_[mail_id]_unknown-md5.csv"
write_csv_to_DB('temp_query', file_path + "/" + mail_id_file, ['mail_id'], conn, cur)
"""

query_all_fields_in_temp_query = "SELECT base.mail, id.prenom, id.nom, id.civilite, id.birth, id.cp, id.ville, lead.ip, lead.provenance, lead.date FROM base " + \
"LEFT JOIN id ON base.id = id.mail_id " + \
"LEFT JOIN lead ON lead.mail_id = id.mail_id " + \
"INNER JOIN temp_query ON base.id = temp_query.mail_id;"
df = sql.read_sql(query_all_fields_in_temp_query, conn)
show_df(df)
df['provenance'] = df['provenance'].str.strip()
try:
    col = "civilite"
    df[col] = df[col].apply(str)
    df[col] = df[col].apply(lambda x: remove_floating_part(x))
except:
    pass
df = fix_year_problem_df(df, 'birth')

file_path = "/media/freebox/Fichiers/Export Regies/Raffles"
res_file_name = "Export-Raffles_15-juil-14_[all_fields_clean]_unknown-md5.csv"
df.to_csv(file_path + "/" + res_file_name, sep = ";", index = False)


""" Script that looks up fields in the DB
lookup_mail_res = lookup_single_columnwide(df, query_fields, 'fields', 'mail_id', cur, convert_res_to_int = False)
if lookup_mail_res[0]:
    df = lookup_mail_res[1]
    show_df(df)

fields_list = list(df['fields'])
new_fields_list = []
for field in fields_list:
    if len(field) == 1:
        new_fields_list.append(field)
    elif len(field) > 1:
        for subfield in field:
            new_fields_list.append(subfield)
            
df_field = pd.DataFrame(new_fields_list, columns = ['mail', 'prenom', 'nom', 'civilite', 'birth', 'cp', 'ville', 'ip', 'provenance', 'date'])
show_df(df_field)
df_field['mail'] = df_field['mail'].apply(str)
df_field['mail'] = df_field['mail'].apply(lambda x: x.strip("'"))
show_df(df_field)
df_field.to_csv(file_path + "/Export-Raffles_15-juil-14_[mail,prenom,nom,civilite,birth,cp,ville,ip,provenance,date]_unknown-md5.csv", index = False)
"""    
