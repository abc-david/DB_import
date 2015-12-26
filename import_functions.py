'''
Created on 15 feb. 2014

@author: david
'''
#!/usr/bin/env python
# -*- coding: utf-8 -*-

#from DB_mapping import *
import time
import numpy as np
import pandas as pd
from pandas.io import sql
import psycopg2
import codecs
import operator
import random
import collections
import datetime
import dateutil.parser as dparser
from dateutil.relativedelta import relativedelta
from unicode_fix import *
from rfc_mail_validation import *
import hashlib
import sys
from threading import Thread

class Thread_Return(Thread):
    def __init__(self, group=None, target=None, name=None,
                 args=(), kwargs={}, Verbose=None):
        Thread.__init__(self, group, target, name, args, kwargs, Verbose)
        self._return = None
    def run(self):
        if self._Thread__target is not None:
            self._return = self._Thread__target(*self._Thread__args,
                                                **self._Thread__kwargs)
    def join(self):
        Thread.join(self)
        return self._return

global need_to_prepare_lookup_query
need_to_prepare_lookup_query = True
global lookup_query_name
lookup_query_name = ""
global need_to_prepare_update_query
need_to_prepare_update_query = True
global update_query_name
update_query_name = ""

log_path = "/media/freebox/Fichiers/ImportDB/Pandas/Test/log/"
log_file = codecs.open(log_path + "log_test_import_functions.txt", 'a', encoding='utf-8')

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

def show_df(df):
    print df.head(5)
    print str(len(df.index)) + " lines."

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
        log_file.write(output + '\n')
    elif indent == 3:
        output = (space * indent) + message
        log_file.write(output + '\n')
    elif indent >= 4:
        output = (space * (indent - 1)) + "(" + elapsed_str + ") " + message
    #log_file.write(output + '\n')
    print output

# --- file_name argument extraction ---

def extract_content(string, open_char, close_char):
    if open_char:
        if open_char in string:
            after_open_char = string.partition(open_char)[-1]
            if close_char:
                if close_char in after_open_char:
                    content = after_open_char.partition(close_char)[0]
                    rest_of_string = after_open_char.partition(close_char)[-1]
                    return[content, rest_of_string]
            return[after_open_char, ""]
        return [string]
    else:
        if close_char:
            if close_char in string:
                content = string.partition(close_char)[0]
                rest_of_string = string.partition(close_char)[-1]
                return[content, rest_of_string]
        return [string]

def extract_enclosed_arguments(file_name):
    separator_list = [',']
    argument_list = []
    search_flag = True
    string = file_name
    while search_flag:
        extract_result = extract_content(string, '[', ']')
        if len(extract_result) == 2:
            for separator in separator_list:
                if separator in extract_result[0]:
                    argument_list.append(list(extract_result[0].split(separator)))
                    break
                else:
                    single_item = []
                    single_item.append(extract_result[0])
                    argument_list.append(single_item)
            string = extract_result[1]
        else:
            search_flag = False
    return argument_list

def extract_front_arguments(file_name):
    argument_list = []
    continue_extract_flag = True
    string = file_name
    while continue_extract_flag:
        extract_result = extract_content(string, '', '_')
        if len(extract_result) == 1:
            return argument_list
        else:
            content = extract_result[0]
            string = extract_result[1]
            if '[' in content:
                content = content[:content.find('[')]
                if content:
                    argument_list.append(content)
                return argument_list
            else:
                argument_list.append(content)
            #continue_extract_flag = False

def extract_arguments(file_name, silent = False):
    status_list = []
    update_dict = {}
    
    file_name = file_name.strip("")
    
    file_arguments = extract_front_arguments(file_name)
    if file_arguments:
        if len(file_arguments) == 3:
            file_date = file_arguments[2]
            try:
                parsed_date = dparser.parse(file_date, fuzzy = True, dayfirst = False)
                file_date = parsed_date.strftime('%d/%m/%y')
                file_arguments[2] = file_date
            except:
                file_date = ""
                file_arguments = file_arguments[:-1]
        elif len(file_arguments) == 2:
            file_date = ""
    else:
        message = "FATAL ERROR. Unable to read file arguments."
        print_to_log(log_file, 1, message)
        return
    
    argument_list_bracket = extract_enclosed_arguments(file_name)
    if argument_list_bracket:
        header = argument_list_bracket[0]

        if len(argument_list_bracket) > 1:
            status_list = argument_list_bracket[1]
            for cpt in range(len(status_list)):
                formatted_date = ""
                if "-" in status_list[cpt]:
                    date_string = status_list[cpt].split("-")[1]
                    try:
                        parsed_date = dparser.parse(date_string, fuzzy = True, dayfirst = False)
                        formatted_date = parsed_date.strftime('%d/%m/%y')
                    except:
                        if file_date:
                            formatted_date = file_date
                    if formatted_date:
                        status_list[cpt] = [status_list[cpt].split("-")[0], formatted_date]
                else:
                    if file_date:
                        formatted_date = file_date
                        status_list[cpt] = [status_list[cpt], formatted_date]
                    
            if len(argument_list_bracket) > 2:
                update_info = argument_list_bracket[2]
                update_dict = {}
                for item in update_info:
                    update_dict[item] = True

            else:
                update_dict = {}
        else:
            status_list = []        
    else:
        header = ['mail']
        
    insert_prov = True
    file_name_parts = file_name.split("_")
    for part in file_name_parts:
        if part.lower() == "false":
            insert_prov = False
    file_name_tail = file_name.rpartition(']')[-1]
    if "false" in file_name_tail.lower():
        insert_prov = False
    
    if "ok" in file_arguments[0].lower():
        print "Special configuration file : 'OK' file (i.e. non-NPAI import)."
        header = ['mail']
        status_list_flag = False
        if len(file_arguments) == 3:
            if not "file_date" in locals():
                file_date = file_arguments[2]
            if file_date:
                status_list = [['ok', file_date]]
                status_list_flag = True
        if not status_list_flag:
            today = datetime.datetime.now()
            formatted_date = today.strftime('%d/%m/%y')
            status_list = [['ok', formatted_date]]
        insert_prov = False
    
    if not silent:
        message = "OK : Arguments extracted from file name."
        print_to_log(log_file, 2, message)
        message = "file = %s | header = %s | status = %s | update = %s | insert_prov = %s" \
                    % (str(file_arguments), str(header), str(status_list), str(update_dict), str(insert_prov))
        print_to_log(log_file, 3, message)
    return [file_arguments, header, status_list, update_dict, insert_prov]

# --- DB-related ---

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
    
# --- 'fichier_list' table ---

def update_provenance_record(file_argument, header = "", status_list = "", db_schema = ""):
    fichier_num = file_argument[0]
    if len(file_argument) > 1:
        if file_argument[1]:
            if db_schema:
                direct_DB_cursor.execute("UPDATE " + db_schema + ".fichier_list SET fichier_nom = %s WHERE fichier_num = %s", \
                                         (str(file_argument[1]), str(fichier_num)))
            else:
                direct_DB_cursor.execute("UPDATE fichier_list SET fichier_nom = %s WHERE fichier_num = %s", \
                                         (str(file_argument[1]), str(fichier_num)))
        if len(file_argument) > 2:
            if file_argument[2]:
                date_argument = file_argument[2]
                parsed_date = dparser.parse(date_argument, fuzzy = True, dayfirst = False)
                formatted_date = parsed_date.strftime('%d/%m/%y')
                if db_schema:
                    direct_DB_cursor.execute("UPDATE " + db_schema + ".fichier_list SET fichier_date = %s WHERE fichier_num = %s", \
                                             (str(formatted_date), str(fichier_num)))
                else:
                    direct_DB_cursor.execute("UPDATE fichier_list SET fichier_date = %s WHERE fichier_num = %s", \
                                             (str(formatted_date), str(fichier_num)))
    if header:
        champs = ""
        for item in header:
            champs = champs + str(item) + ","
        champs = champs[:-1]
        if db_schema:
            direct_DB_cursor.execute("UPDATE " + db_schema + ".fichier_list SET champs = %s WHERE fichier_num = %s", \
                                     (str(champs), str(fichier_num)))
        else:   
            direct_DB_cursor.execute("UPDATE fichier_list SET champs = %s WHERE fichier_num = %s", \
                                     (str(champs), str(fichier_num)))
    if status_list:
        status = ""
        for item in status_list:
            status = status + str(item) + ","
        status = status[:-1]
        if db_schema:
            direct_DB_cursor.execute("UPDATE " + db_schema + ".fichier_list SET statuts = %s WHERE fichier_num = %s", \
                                     (str(status), str(fichier_num)))
        else:
            direct_DB_cursor.execute("UPDATE fichier_list SET statuts = %s WHERE fichier_num = %s", \
                                     (str(status), str(fichier_num))) 
    direct_DB_conn.commit()
    
def insert_provenance(fichier_num, db_schema = ""):
    try:
        if db_schema:
            direct_DB_cursor.execute("INSERT INTO " + db_schema + ".fichier_list (fichier_num) VALUES (%s)", \
                                     (str(fichier_num),))
        else:
            direct_DB_cursor.execute("INSERT INTO fichier_list (fichier_num) VALUES (%s)", \
                                     (str(fichier_num),))
        direct_DB_conn.commit()
        message = "OK : New record in table 'fichier_list' : " + str(fichier_num)
        print_to_log(log_file, 2, message)
        try:
            if db_schema:
                direct_DB_cursor.execute("SELECT fichier_id FROM " + db_schema + ".fichier_list WHERE fichier_num = %s", \
                                         (str(fichier_num),))
            else:
                direct_DB_cursor.execute("SELECT fichier_id FROM fichier_list WHERE fichier_num = %s", \
                                         (str(fichier_num),))   
        except:
            message = "Failed to execute SELECT query from table 'fichier_list' with fichier_num = " + str(fichier_num)
            print_to_log(log_file, 2, message)
            return [False]
        records = direct_DB_cursor.fetchall()
        if records == []:
            message = "Failed to retrieve existing record in table 'fichier_list' with fichier_num = " + str(fichier_num)
            print_to_log(log_file, 2, message)
            return [False]
        else:
            prov_fichier_id = str(records[0]).strip('()').strip(',')
            return [True, fichier_num, prov_fichier_id] 
    except:
        message = "Failed to insert new record in table 'fichier_list'."
        print_to_log(log_file, 2, message)
        return [False]

def extract_provenance(file_argument, header = "", status_list = "", db_schema = ""):
    #fichier_num = file_name[:file_name.find("_")]
    fichier_num = file_argument[0]
    try:
        if db_schema:
            direct_DB_cursor.execute("SELECT fichier_id FROM " + db_schema + ".fichier_list WHERE fichier_num = %s", \
                                     (str(fichier_num),))
        else:
            direct_DB_cursor.execute("SELECT fichier_id FROM fichier_list WHERE fichier_num = %s", \
                                     (str(fichier_num),))
        #direct_DB_cursor.execute("SELECT fichier_id FROM fichier_list WHERE fichier_num = E'" + str(fichier_num) + "';")
        pass
    except:
        message = "Failed to execute SELECT query from table 'fichier_list' with fichier_num = " + str(fichier_num)
        print_to_log(log_file, 2, message)
        insert_attempt = insert_provenance(fichier_num, db_schema = db_schema)
        if insert_attempt[0]:
            update_provenance_record(file_argument, header, status_list, db_schema = db_schema)
            return insert_attempt
        else:
            return [False]
    records = direct_DB_cursor.fetchall()
    if records == []:
        message = "Failed to retrieve existing record in table 'fichier_list' with fichier_num = " + str(fichier_num)
        print_to_log(log_file, 2, message)
        insert_attempt = insert_provenance(fichier_num, db_schema = db_schema)
        if insert_attempt[0]:
            update_provenance_record(file_argument, header, status_list, db_schema = db_schema)
            message = "OK : Provenance name '" + str(insert_attempt[1]) + "' identified in DB as record " + str(insert_attempt[2]) + "."
            print_to_log(log_file, 2, message)
            return insert_attempt
        else:
            return [False]
    else:
        prov_fichier_id = str(records[0]).strip('()').strip(',')
        update_provenance_record(file_argument, header, status_list, db_schema = db_schema)
        message = "OK : Provenance name '" + str(fichier_num) + "' identified in DB as record " + str(prov_fichier_id) + "."
        print_to_log(log_file, 2, message)
        return [True, fichier_num, prov_fichier_id]

# --- inspect text file ---

def guess_encoding(text_file):
    import magic
    with open(text_file, 'r') as f:
        content = f.read()
        m = magic.Magic(mime_encoding=True)
        encoding = m.from_buffer(content)
    return encoding

def number_of_lines(text_file):
    with open(text_file, 'r') as f:
        for i, l in enumerate(f):
            pass
    return i + 1

def convert_file_encoding(text_file, new_text_file = "", native_encoding = "", \
                          new_encoding = "utf-8", block_size = 1048576):
    if not native_encoding:
        native_encoding = guess_encoding(text_file)
    if not new_encoding:
        new_encoding = "utf-8"
    if not block_size:
        block_size = 1048576 # or some other, desired size in bytes
    if not new_text_file:
        new_text_file = text_file
        with codecs.open(text_file, "r", native_encoding) as sourceFile:
            contents = sourceFile.read()
        with codecs.open(new_text_file, "w", new_encoding) as targetFile:
            targetFile.write(contents)
    else:
        with codecs.open(text_file, "r", native_encoding) as sourceFile:
            with codecs.open(new_text_file, "w", new_encoding) as targetFile:
                while True:
                    contents = sourceFile.read(block_size)
                    if not contents:
                        break
                    targetFile.write(contents)

def fix_text_file_bad_unicode(text_file, encoding):
    fixed_content = []
    with codecs.open(text_file, mode = 'r', encoding = encoding, errors='ignore') as f:
        data = f.readlines()
        for cpt_line in range(len(data)):
            this_line = data[cpt_line]
            unicode_data = unicode(this_line)
            fixed_line = fix_bad_unicode(unicode_data)
            fixed_content.append(fixed_line)
    with codecs.open(text_file, mode = 'w', encoding = 'utf-8', errors='ignore') as f:
        f.writelines(fixed_content)

def fix_html_encoding(string):
    html_dict = collections.OrderedDict()
    html_dict = {'&ampamp;' : '&', '&amp;amp;' : '&', '&amp;' : '&', \
                 '&eacute;' : 'e', '&egrave;' : 'e', '&agrave;' : 'a', \
                 '&eacute' : 'e', '&egrave' : 'e', '&agrave' : 'a'}
    for key, value in html_dict.iteritems():
        if key in string:
            mod_string = string.replace(key,value)
            string = mod_string
    if '&' in string:
        string = string.replace('&', '')
        string = string.replace(';', '')
    return string

def inspect_text_file(text_file, desired_encoding = 'utf-8'):
    try:
        encoding_guess = guess_encoding(text_file)
        message = "OK : encoding detected : '%s'" % str(encoding_guess)
        print_to_log(log_file, 3, message)
        encoding_list = [encoding_guess, 'utf-8', 'latin-1']
    except:
        encoding_list = ['utf-8', 'latin-1']
    keep_trying = True
    encoding_attempt = 0
    while keep_trying:
        try:
            try_this_encoding = encoding_list[encoding_attempt]
            f = codecs.open(text_file, 'r', encoding = try_this_encoding)
            message = "OK : file opens correctly with the following encoding : '%s'" % str(try_this_encoding)
            print_to_log(log_file, 3, message)
            keep_trying = False
        except:
            message = "File failed to open with the following encoding : '%s'" % str(try_this_encoding)
            print_to_log(log_file, 3, message)
            e = sys.exc_info()
            for item in e:
                message = str(item)
                print_to_log(log_file, 3, message)
            encoding_attempt += 1
            if encoding_attempt > len(encoding_list):
                message = "FATAL PROBLEM : Unable to open file '%s'" % str(text_file)
                print_to_log(log_file, 2, message)
                return [False]
    
    # find separator character
    sep_list = [",", ";"]
    try:
        contents = f.read()
        sep_dict = {}
        for sep in sep_list:
            sep_dict[sep] = contents.count(sep)
        f.close()
        sorted_sep_dict = sorted(sep_dict.iteritems(), key=operator.itemgetter(1), reverse = True)
        result_sep = sorted_sep_dict[0][0]
        message = "OK : separator character detected : '%s'" % str(result_sep)
        print_to_log(log_file, 3, message)
        
        # check if all lines have an equal number of legitimate separator
        line_number = number_of_lines(text_file)
        message = "OK : found %s occurences of '%s' separator character for %s lines" \
                    % (str(sep_dict[result_sep]), str(result_sep), str(line_number))
        print_to_log(log_file, 3, message)
        
        # fix problem if needed
        col_num, nb_pb = divmod(sep_dict[result_sep], line_number)
        if nb_pb > (line_number * 0.9):
            col_num += 1
        if try_this_encoding != desired_encoding:
            need_rewrite = True
        else:
            need_rewrite = False
        milestones = {1 : 10000, 2 : 20000, 3 : 50000, 4 : 100000, 5 : 200000, \
                      6 : 500000, 7 : 1000000, 8 : 2000000, 9: 5000000}
        cpt_milestones = 1
        with codecs.open(text_file, 'r', encoding = try_this_encoding) as f:
            # read a list of lines into data
            data = f.readlines()
            for cpt_line in range(len(data)):
                if cpt_line == milestones[cpt_milestones]:
                    message = "OK. First " + str(milestones[cpt_milestones]) + " mails looked up."
                    print_to_log(log_file, 4, message)
                    cpt_milestones += 1
                line_count = data[cpt_line].count(result_sep)
                if line_count != col_num:
                    need_rewrite = True
                    fixed_line = fix_html_encoding(data[cpt_line])
                    if fixed_line != data[cpt_line]:
                        data[cpt_line] = fixed_line
                    line_count = data[cpt_line].count(result_sep)
                    if line_count == 0:
                        fixed_line = data[cpt_line].replace('""', '";"')
                        data[cpt_line] = fixed_line
                        line_count = data[cpt_line].count(result_sep)
                    if line_count > col_num:
                        message = "Found extra occurence in line %s : '%s'" % (str(cpt_line + 1), str(data[cpt_line]))
                        print_to_log(log_file, 4, message)
                        #data[cpt_line] = data[cpt_line].replace(result_sep, "", (line_count - col_num)) # removes at the beginning of the line: not good.
                        for __ in range(line_count - col_num):
                            position = data[cpt_line].rfind(result_sep)
                            data[cpt_line] = data[cpt_line][:position] + data[cpt_line][position:].replace(result_sep, "")
                        message = "Fixed. New line is : '%s'" % str(data[cpt_line])
                        print_to_log(log_file, 4, message)
                    elif line_count < col_num:
                        message = "Found missing occurence in line %s : '%s'" % (str(cpt_line + 1), str(data[cpt_line]))
                        print_to_log(log_file, 4, message)
                        for __ in range(col_num - line_count):
                            position = data[cpt_line].rfind(result_sep)
                            data[cpt_line] = data[cpt_line][:position] + str(result_sep) + data[cpt_line][position:]
                        message = "Fixed. New line is : '%s'" % str(data[cpt_line])
                        print_to_log(log_file, 4, message)
        if need_rewrite == True:
            with codecs.open(text_file, 'w', encoding = desired_encoding) as f:
                f.writelines(data)
            message = "OK : File succesfully checked and converted to '%s' encoding." % str(desired_encoding)
            print_to_log(log_file, 3, message)
        else:
            message = "OK : File succesfully checked (natively found in '%s' encoding)." % str(desired_encoding)
            print_to_log(log_file, 3, message)
        return [True, result_sep, desired_encoding]
    except:
        try:
            fix_text_file_bad_unicode(text_file, try_this_encoding)
            return [True, "", 'utf-8']
        except:
            return [True, "", try_this_encoding]

# --- load text file ---

def test_text_file(text_file, header = "", sep = "", n_rows = 5, keep_char = "@", sep_detect_done = False):
    enc = guess_encoding(text_file)
    try:
        if sep:
            part = pd.read_csv(text_file, encoding = enc, sep = sep, header = None, nrows = n_rows)
        else:
            part = pd.read_csv(text_file, encoding = enc, header = None, nrows = n_rows)
    except:
        inspection = inspect_text_file(text_file, desired_encoding = 'utf-8')
        if inspection[0]:
            enc = inspection[2]
            sep = inspection[1]
            if sep:
                part = pd.read_csv(text_file, encoding = enc, sep = sep, header = None, nrows = n_rows)
            else:
                part = pd.read_csv(text_file, encoding = enc, header = None, nrows = n_rows)
        else:
            return [False]
    n_col = len(part.columns)
    if header:
        if n_col < len(header):
            sep_list = [';', ',']
            sep_dict = {}
            for sep in sep_list:
                sep_dict[sep] = 0
            for cpt_line in range(n_rows - 1):
                content = str("".join(list(str(part.at[cpt_line, cpt_col]) for cpt_col in range(n_col))))
                for sep in sep_list:
                    sep_dict[sep] = sep_dict[sep] + content.count(sep)
            sorted_sep_dict = sorted(sep_dict.iteritems(), key=operator.itemgetter(1), reverse = True)
            result_sep = sorted_sep_dict[0][0]
            if not sep_detect_done: 
                test = test_text_file(text_file, header = header, sep = result_sep, n_rows = n_rows, keep_char = keep_char, \
                                      sep_detect_done = True)
                return test
            else:
                message = "Failed to match given header (%s) with the content of the file, which seem to have only %s column(s)." \
                            % (str(header), str(n_col))
                print_to_log(log_file, 2, message)
                return [False]    
    skip_rows = []
    for cpt_line in range(n_rows - 1):
        if keep_char not in str("".join(list(str(part.at[cpt_line, cpt_col]) for cpt_col in range(n_col)))):
            skip_rows.append(cpt_line)
    return [True, enc, sep, skip_rows, n_col]

def load_text_file(text_file, header = ""):
    test = test_text_file(text_file, header)
    if test[0]:
        if header:
            header = list(header)
            if len(header) < test[4]:
                for cpt in range(test[4] - len(header)):
                    header.append("unknown_" + str(cpt + 1))
            if len(header) > test[4]:
                for cpt in range(len(header) - test[4]):
                    header.pop()
            try:
                if test[2]:
                    df = pd.read_csv(text_file, names = header, encoding = test[1], sep = test[2], skiprows = test[3], dtype=object)
                else:
                    df = pd.read_csv(text_file, names = header, encoding = test[1], skiprows = test[3], dtype=object)
            except:
                if test[2]:
                    df = pd.read_csv(text_file, names = header, encoding = test[1], sep = test[2], skiprows = test[3], \
                                     error_bad_lines = False, dtype=object)
                else:
                    df = pd.read_csv(text_file, names = header, encoding = test[1], skiprows = test[3], \
                                     error_bad_lines = False, dtype=object)
        else:
            try:
                if test[2]:
                    df = pd.read_csv(text_file, header = None, encoding = test[1], sep = test[2], skiprows = test[3], dtype=object)
                else:
                    df = pd.read_csv(text_file, header = None, encoding = test[1], skiprows = test[3], dtype=object)
            except:
                if test[2]:
                    df = pd.read_csv(text_file, header = None, encoding = test[1], sep = test[2], skiprows = test[3], \
                                     error_bad_lines = False, dtype=object)
                else:
                    df = pd.read_csv(text_file, header = None, encoding = test[1], skiprows = test[3], \
                                     error_bad_lines = False, dtype=object)
        show_df(df)
        return [True, df]
    else:
        return [False]

# --- clean-up dataframe ---

def remove_unknown_columns(df):
    header = list(df.columns)
    for field in header:
        if field.find("unknown") != -1:
            df = df.drop(field, 1)
    return df

def add_header(dataframe, header, remove_unknown = True): # depreceted function
    len_col_df = len(dataframe.columns)
    if len_col_df == 1:
        header = ['mail']
        dataframe.columns = header
        return [True, dataframe, header]
    else:
        if header:
            if len(header) == len_col_df:
                try:
                    dataframe.columns = header
                    return [True, dataframe, header]
                except:
                    message = "Failed to add header to DataFrame : " + str(header)
                    print_to_log(log_file, 2, message)
                    return [False]
            elif len(header) < len_col_df:
                for cpt in range(len_col_df - len(header)):
                    header.append("unknown_" + str(cpt))
                try:
                    dataframe.columns = header
                    if remove_unknown == True:
                        dataframe = remove_unknown_columns(dataframe)
                        header = list(dataframe.columns)
                    return [True, dataframe, header]
                except:
                    message = "Failed to add header to DataFrame : " + str(header)
                    print_to_log(log_file, 2, message)
                    return [False]
            elif len(header) > len_col_df:
                header = header[:len_col_df]
                try:
                    dataframe.columns = header
                    return [True, dataframe, header]
                except:
                    message = "Failed to add header to DataFrame : " + str(header)
                    print_to_log(log_file, 2, message)
                    return [False]
            else:
                message = "Failed to add header to DataFrame : " + str(header)
                print_to_log(log_file, 2, message)
                return [False]
        else:
            message = "Failed to add header to DataFrame : " + str(header)
            print_to_log(log_file, 2, message)
            message = "Header not defined."
            print_to_log(log_file, 3, message)
            return [False]

def drop_null_or_missing_values(dataframe, field):
    if not isinstance(field, list):
        field = [field]
    dataframe = dataframe.dropna(subset = field)
    message = "OK : DataFrame cleaned from null or missing values ('NaN') on the " + str(field) +" field."
    print_to_log(log_file, 2, message)
    return dataframe

def sort_unique(dataframe, uniq_cols, sort_col):
    try:
        unik_dataframe = dataframe.drop_duplicates(uniq_cols).sort(sort_col)
        #df = df.groupby('mail')
        #new_index = [ind[0] for ind in sdf.groups.values()]
        #df = df.reindex(new_index)
        message = "OK : DataFrame unifyed by " + str(uniq_cols) + ", and sorted by " + str(sort_col) + "."
        print_to_log(log_file, 2, message)
        return [True, unik_dataframe]
    except:
        message = "Failed to sort & unify the DataFrame."
        print_to_log(log_file, 2, message)
        return [False]

def add_status_columns(dataframe, status_header = ""):
    if status_header == "":
        status_header = ['syntax', 'fail', 'exist', 'insert', 'update']
    dataframe['syntax'] = True
    for status in status_header[1:]:
        dataframe[status] = False
    #message = "OK : Status columns inserted in DataFrame : " + str(status_header)
    #print_to_log(log_file, 2, message)
    return dataframe

# --- cleanup_mail_syntax ---

def modifications_on_mail_syntax(dataframe):
    #mail_index = dataframe.index
    dataframe['mail'] = dataframe['mail'].apply(str)
    dataframe['mail'] = dataframe['mail'].apply(lambda x: x.strip())
    dataframe['mail'] = dataframe['mail'].apply(lambda x: x.lower())
    dataframe['mail'] = dataframe['mail'].apply(lambda x: x.replace('"', ""))
    dataframe['mail'] = dataframe['mail'].apply(lambda x: x.replace('@@', '@'))
    #dataframe['mail'] = dataframe['mail'].str.replace("'", "\\'") # this to be performed in write_to_csv()
    message = "OK. modifications_on_mail_syntax() done : convert to string, strip spaces, lower-case, get rid of double ' and @."
    print_to_log(log_file, 4, message)
    return dataframe

def boolean_checks_on_mail(dataframe, filtre_in, filtre_out):
    mail_index = dataframe.index
    boolean_checks = []
    for must_be in filtre_in:
        try:
            boolean_checks.append(dataframe['mail'].str.contains(must_be))
        except:
            message = "Problem with checking if 'mail' field contains " + str(must_be) + ". Will be ignored."
            print_to_log(log_file, 5, message)
            e = sys.exc_info()
            for item in e:
                message = str(item)
                print_to_log(log_file, 6, message)
    for killer in filtre_out:
        try:
            boolean_checks.append(~(dataframe['mail'].str.contains(killer)))
        except:
            message = "Problem with checking if 'mail' field contains " + str(must_be) + ". Will be ignored."
            print_to_log(log_file, 5, message)
            e = sys.exc_info()
            for item in e:
                message = str(item)
                print_to_log(log_file, 6, message)
    boolean_checks.append(dataframe['mail'].map(lambda x: len(str(x)) <= 100))
    for cpt in range(len(mail_index)):
        for boolean_series in boolean_checks:
            if boolean_series.at[mail_index[cpt]] == False:
                dataframe.at[mail_index[cpt], 'syntax'] = False
                break
    message = "OK. boolean_checks_on_mail() done : filtre_in, filtre_out, length, update syntax field."
    print_to_log(log_file, 4, message)
    return dataframe

def repair_bad_unicode(dataframe, field, filter_by = ""):
    if not filter_by:
        df_index = dataframe.index
        df_unicode = pd.Series(index = df_index)
        #df_unicode[field] = dataframe[field].apply(lambda x: unicode(x))
        df_unicode = dataframe[field]
    else:
        df_syntax = dataframe[dataframe[filter_by] == True]
        df_index = df_syntax.index
        df_unicode = pd.Series(index = df_index)
        #df_unicode[field] = df_syntax[field].apply(lambda x: unicode(x))
        df_unicode = df_syntax[field]
    boolean_checks = []
    ascii_list = [unichr(195), unichr(194), unichr(191), unichr(171)]
    for ascii_char in ascii_list:
        boolean_checks.append(df_unicode.str.contains(ascii_char))
    for cpt in range(len(df_index)):
        for boolean_series in boolean_checks:
            if boolean_series.at[df_index[cpt]] == True:
                broken_mail = df_unicode.at[df_index[cpt]]
                unicode_mail = unicode(broken_mail)
                clean_mail = fix_bad_unicode(unicode_mail)
                dataframe.at[df_index[cpt], field] = clean_mail
                break
    message = "OK. repair_bad_unicode() done : if ascii_list, then fix_bad_unicode()."
    print_to_log(log_file, 4, message)
    return dataframe

def validate_rfc_syntax(dataframe, line_by_line = False, check_mx=False, verify=False, debug=False):
    if line_by_line == False:
        try:
            boolean_check_rfc = dataframe['mail'].apply(lambda x: validate_email (x, check_mx, verify, debug))
            dataframe['rfc_syntax'] = boolean_check_rfc
        except:
            message = "Failed to use RFC_validator at the Series level. Will perform a line_by_line check instead."
            print_to_log(log_file, 5, message)
            dataframe = validate_rfc_syntax(dataframe, True, check_mx, verify, debug)
    else:
        dataframe['rfc_syntax'] = False
        df_index = dataframe.index
        for cpt in range(len(df_index)):
            mail = dataframe.at[df_index[cpt], 'mail']
            try:
                result = validate_email(mail, check_mx, verify, debug)
            except: 
                message = "index : " + str(df_index[cpt]) + ", mail = " + str(mail)
                print_to_log(log_file, 5, message)
                result = False
            if result == True:
                dataframe.at[df_index[cpt], 'rfc_syntax'] = True
    message = "OK. Mails checked with RFC_validator. 'rcf_syntax' field updated."
    print_to_log(log_file, 4, message)
    #print dataframe
    return dataframe
    
def fill_and_check_domains(dataframe):
    mail_index = dataframe.index
    domain_splits = dataframe['mail'].str.split('@')
    dataframe['domain'] = domain_splits.str[1]
    domain_check = dataframe['domain'].map(lambda x: (len(str(x)) >= 5) and (len(str(x)) <= 40))
    for cpt in range(len(mail_index)):
        if domain_check.at[mail_index[cpt]] == False:
            dataframe.at[mail_index[cpt], 'syntax'] = False
    message = "OK. fill_and_check_domains() done : split, domain length, update syntax field."
    print_to_log(log_file, 4, message)
    return dataframe
    
def cleanup_mail_syntax_columnwide(dataframe, mail_cleanup = True, filtre_in =  "", filtre_out = ""):
    if not filtre_in:
        filtre_in = ['@', '.']
    if not filtre_out:
        filtre_out = [',', ';']    
    clock_4 = time.clock()
    dataframe = modifications_on_mail_syntax(dataframe)
    if mail_cleanup:
        dataframe = boolean_checks_on_mail(dataframe, filtre_in, filtre_out)
        #dataframe = repair_bad_unicode(dataframe, 'mail', 'syntax')
    #dataframe = validate_rfc_syntax(dataframe, True, True, True, True)
    if mail_cleanup:
        dataframe = validate_rfc_syntax(dataframe)
    dataframe = fill_and_check_domains(dataframe)
    df_syntax = dataframe[dataframe.syntax == True]
    message = "OK : 'mail' data verified and cleaned up. %s valid mails." % str(len(df_syntax.index))
    print_to_log(log_file, 2, message)
    return df_syntax

# --- md5 ---

def hash_mail_to_md5(string):
    string = string.lower().encode()
    hash_object = hashlib.md5(string)
    return hash_object.hexdigest()

def append_md5_field(dataframe):
    try:
        dataframe['md5'] = dataframe['mail'].apply(lambda x: hash_mail_to_md5(x))
    except:
        message = "Failed to hash mails to md5."
        print_to_log(log_file, 2, message)
        e = sys.exc_info()
        for item in e:
            message = str(item)
            print_to_log(log_file, 3, message)
        message = "Attempt to hash to md5 line-by-line."
        print_to_log(log_file, 2, message)
        dataframe['md5'] = 'NaN'
        for cpt in range(len(dataframe.index)):
            mail = dataframe.at[dataframe.index[cpt], 'mail']
            try:
                mail_to_md5 = hash_mail_to_md5(mail)
                dataframe.at[dataframe.index[cpt], 'md5'] = mail_to_md5
            except:
                message = "Failed md5 : line = " + str(dataframe.index[cpt] + 1) + " , mail = " + str(mail)
        #return [dataframe, header]
    message = "OK : Mails hashed to md5. Field 'md5' appended to dataframe and 'header'."
    print_to_log(log_file, 2, message)
    return dataframe

# --- map records (md5) ---

def sync_md5_table(DB_conn, csv_path = ""):
    from SQL_query_builder import query_builder
    if not csv_path:
        csv_path = "/media/freebox/Fichiers/ImportDB/Pandas/Test"
    select_dict = collections.OrderedDict()
    select_dict['base'] = ['id', 'mail']
    where_dict = collections.OrderedDict()
    where_dict['md5.mail_id'] = ['left', 'null']
    query_dict = {'select' : select_dict, \
                  'where' : where_dict, \
                  'limit' : 0}
    query = query_builder(query_dict, silent = True)
    missing_md5 = sql.read_sql(query, DB_conn, coerce_float=False)
    #show_df(missing_md5)
    missing_md5_length = len(missing_md5.index)
    if missing_md5_length > 0:
        missing_md5 = append_md5_field(missing_md5)
        missing_md5.rename(columns = {'id' : 'mail_id'}, inplace = True)
        missing_md5 = missing_md5.drop('mail', 1)
        #show_df(missing_md5)
        missing_md5_header = list(missing_md5.columns)
        missing_md5_file_name = create_file_name("MISSING-md5", 'md5', missing_md5_length, missing_md5_header)
        csv_file = write_to_csv(missing_md5, csv_path, 'md5', missing_md5_file_name)
        if csv_file:
            write_csv_to_DB('md5', csv_file, missing_md5_header)
            message = "OK : 'md5' table synced with 'base' table : %s records added." % str(missing_md5_length)
            print_to_log(log_file, 2, message)
    else:
        message = "OK : 'md5' table already in sync with 'base' table."
        print_to_log(log_file, 2, message)

def test_md5_table_sync(DB_conn):
    DB_cursor = DB_conn.cursor()
    DB_cursor.execute("SELECT COUNT(id) FROM base;")
    records = DB_cursor.fetchall()
    if records:
        raw_record = str(records[0]).strip('()').strip(',')
        try:
            nb_mail = int(raw_record)
        except:
            nb_mail = int(raw_record[:-1])
    DB_cursor.execute("SELECT COUNT(mail_id) FROM md5;")
    records = DB_cursor.fetchall()
    if records:
        raw_record = str(records[0]).strip('()').strip(',')
        try:
            nb_md5 = int(raw_record)
        except:
            nb_md5 = int(raw_record[:-1])
    if nb_mail > nb_md5:
        sync_md5_table(DB_conn, csv_path = "")

def load_md5_table(DB_conn, limit = "", silent = False):
    if not DB_conn:
        message = "Pb. with the function md5_table(). No connection to the DB : missing parameter 'BD_conn'. "
        print_to_log(log_file, 3, message)
        return [False]
    test_md5_table_sync(DB_conn)
    query = "select %s, %s from %s;" % ('mail_id', 'md5', 'md5')
    if limit:
        if limit > 0:
            query = "select %s, %s from %s limit %s;" % ('mail_id', 'md5', 'md5', str(limit))   
    #query = "select %s, %s from %s limit 1000;" % ('mail_id', 'md5', 'md5')
    try:
        md5 = pd.read_sql(query, DB_conn)
    except:
        message = "Pb. with pd.read_sql() function on md5 table."
        print_to_log(log_file, 3, message)
        e = sys.exc_info()
        for item in e:
            message = str(item)
            print_to_log(log_file, 6, message)
            return [False]
    if not silent:
        show_df(md5)
        message = "OK : md5 table extracted from DB: " + str(len(md5.index)) + " records found."
        print_to_log(log_file, 2, message)
    return [True, md5]

def map_existing_rows(df, ref_df, join_key = 'md5', output = 'df', exist_key = 'exist', new_key = 'new'):
    header = list(df.columns)
    df[exist_key] = False
    df[new_key] = False
    nb_records_dict = {}
    nb_records_dict['initial'] = len(df.index)
    ref_header = list(ref_df.columns)
    common_fields = [field for field in header if field in ref_header]
    if len(common_fields) == 0:
        message = "Pb. with map_existing_rows(). There are no fields common to the databases. Will return all records as new records."
        print_to_log(log_file, 3, message)
        if output in ['row', 'rows', 'index']:
            return [True, df.index, ""]
        else:
            return [True, df, nb_records_dict]
    else:
        if isinstance(join_key, list):
            new_join_key = []
            for item in join_key:
                if item in common_fields:
                    new_join_key.append(item)
            if new_join_key != join_key:
                if new_join_key:
                    message = "Info : map_existing_rows(). Specified join_key '%s' has fields not common to the databases. " % str(join_key) + \
                            "Will use '%s' instead as join_key (this field(s) is(are) common to both tables)." % str(new_join_key)
                    print_to_log(log_file, 3, message)
                    join_key = new_join_key
                else:
                    message = "Info : map_existing_rows(). Specified join_key '%s' does not exist in both databases. " % str(join_key) + \
                            "Will use '%s' instead as join_key (ie. all field(s) common to both tables)." % str(common_fields)
                    print_to_log(log_file, 3, message)
                    join_key = common_fields
        else:
            if not join_key in common_fields:
                message = "Info : map_existing_rows(). Specified join_key '%s' does not exist in dataframes. " % str(join_key) + \
                            "Will use '%s' instead as join_key (this field is common to both tables)." % str(common_fields[0])
                print_to_log(log_file, 3, message)
                join_key = common_fields[0]
        ref_df['flag'] = True
        try:
            new_df = pd.merge(df, ref_df, on=join_key, how='left')
            message = "OK : Records matched using the fields %s." % str(join_key)
            print_to_log(log_file, 2, message)
        except:
            message = "Pb. with pd.merge() : Unable to merge dataframes."
            print_to_log(log_file, 3, message)
            e = sys.exc_info()
            for item in e:
                message = str(item)
                print_to_log(log_file, 6, message)
            return [False]
        show_df(new_df)
        common_rows_with_ref_df = new_df[new_df['flag'] == True].index
        new_rows_unknown_to_ref_df = new_df[new_df['flag'] != True].index
        new_df = new_df.drop('flag',1)
        if output in ['row', 'rows', 'index']:
            return [True, common_rows_with_ref_df, new_rows_unknown_to_ref_df]
        else:
            new_df.loc[common_rows_with_ref_df, exist_key] = True
            new_df.loc[new_rows_unknown_to_ref_df, new_key] = True
            show_df(new_df)
            nb_records_dict['sorted'] = len(new_df.index)
            nb_records_dict[exist_key] = len(common_rows_with_ref_df)
            nb_records_dict[new_key] = len(new_rows_unknown_to_ref_df)
            return [True, new_df, nb_records_dict]

def lookup_md5(df, md5_df, get_md5 = False, DB_conn = "", limit = ""):
    if get_md5 == False:
        get_md5 = md5_df.empty
    if get_md5 == True:
        md5_query_result = load_md5_table(DB_conn, limit = limit)
        if md5_query_result[0]:
            md5_df = md5_query_result[1]
        else:
            return [False]
    md5_df.rename(columns={'mail_id' : 'pg_id'}, inplace=True)
    map_rows = map_existing_rows(df, md5_df, join_key = 'md5', output = "df", \
                                 exist_key = 'exist', new_key = 'insert')
    if map_rows[0]:
        df = map_rows[1]
        #show_df(df)
        #show_df(df[(df['exist'] == True)])
        #show_df(df[(df['insert'] == True)])
        result = map_rows[2]
        message = "OK : 'exist' and 'insert' fields updated (using matching with 'md5' table)."
        print_to_log(log_file, 2, message)
        kv = []
        for k, v in result.iteritems():
            kv.append(str(k) + " : " + str(v))
        message = str(" | ".join(kv))
        print_to_log(log_file, 3, message)
        return [True, df]
    else:
        message = "Pb. with md5 matching. Will revert to looping through all records in 'base' table."
        print_to_log(log_file, 2, message)
        return [False]

# --- map records (query) ---       

def lookup_record_safe(query_name, mail):
    mail_lookup_query = "EXECUTE " + query_name + " (%s);"
    try:
        direct_DB_cursor.execute(mail_lookup_query, (mail, ))
        records = direct_DB_cursor.fetchall()
        if records == []:
            return [False]
        else:
            mail_id = int(str(records[0]).strip('()').strip(','))
            return [True, mail_id]
    except:
        return [False, mail]
        message = "Failed to execute " + query_name + " with this input : " + mail
        print_to_log(log_file, 3, message)

def lookup_mail_columnwide(dataframe, query_name, index_slice = "", \
                           add_pg_id_column = True):
    milestones = {1 : 1000, 2 : 2000, 3 : 5000, 4 : 10000, 5 : 20000, 6 : 50000, \
                  7 : 100000, 8 : 200000, 9 : 500000, 10 : 1000000, 11 : 2000000, 12: 5000000}
    cpt_milestones = 1
    if add_pg_id_column == True:
        dataframe['pg_id'] = ""
    if not index_slice:
        index_slice = dataframe.index
    number_lookups = len(index_slice)
    if number_lookups != 0:
        cpt_lookup = 0
        clock_4 = time.clock()
        for cpt in range(len(index_slice)):
            mail = dataframe.at[index_slice[cpt], 'mail']
            lookup_result = lookup_record_safe(query_name, mail)
            #print str(cpt_lookup), mail, str(lookup_result[1])
            cpt_lookup += 1
            if cpt_lookup == milestones[cpt_milestones]:
                message = "OK. First " + str(milestones[cpt_milestones]) + " mails looked up."
                print_to_log(log_file, 4, message)
                cpt_milestones += 1
            if lookup_result[0]:
                dataframe.at[index_slice[cpt], 'exist'] = True
                dataframe.at[index_slice[cpt], 'pg_id'] = lookup_result[1]
            else:
                if len(lookup_result) == 2:
                    dataframe.at[index_slice[cpt], 'fail'] = True
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
    
def lookup_query_base(dataframe, prepare_query = True, lookup_query = "", \
                      db_schema = ""):
    if prepare_query == True:
        lookup_query_result = prepare_lookup_query('base', 'id', 'mail', db_schema = db_schema)
        if lookup_query_result[0]:
            query_name = lookup_query_result[1]
        else:
            message = "FATAL PROBLEM. Impossible to return corresponding id for existing mails."
            print_to_log(log_file, 2, message)
            return [False]
    else:
        if lookup_query:
            query_name = lookup_query
        else:
            message = "FATAL PROBLEM. Provide name ('lookup_query' argument) for the prepared query to be used in the lookup process."
            print_to_log(log_file, 2, message)
            return [False]
    
    index_created = False #create_index(db_schema)
    lookup_columnwide_result = lookup_mail_columnwide(dataframe, query_name)
    if lookup_columnwide_result[0]:
        dataframe = lookup_columnwide_result[1]
        if index_created:
            drop_index(db_schema)
        return [True, dataframe, query_name]
    else:
        message = "FATAL PROBLEM. No mails to lookup in DB."
        print_to_log(log_file, 2, message)
        return [False]

# --- populate_and_prepare_dataframe ---

# TODO : rename to prepare_dataframe()
# TODO : test new populate worklow
# TODO : insert new lookup_md5() function
# TODO : use threading to load md5 table at the beginning of the process

def cleanup_mail_column(dataframe, mail_cleanup = True, filtre_in = "", filtre_out = "", status_columns = ""):
    # clean up stuff that's not useful (empty columns, empty lines, non-valid emails)
    dataframe = remove_unknown_columns(dataframe)
    dataframe = drop_null_or_missing_values(dataframe, 'mail')
    dataframe = add_status_columns(dataframe, status_columns)
    dataframe_syntax = cleanup_mail_syntax_columnwide(dataframe, mail_cleanup, filtre_in, filtre_out)
    sort_result = sort_unique(dataframe_syntax, list(dataframe.columns), 'mail')
    if sort_result[0]:
        dataframe_syntax = sort_result[1]
    # match up new records with existing ones in the DB
    dataframe_syntax = append_md5_field(dataframe_syntax)
    return dataframe_syntax

def get_working_dataframe(file_path, file_name, header, DB_conn, md5_lookup = True, md5_table_limit = "", \
                          prepare_query = True, lookup_query = "", db_schema = "", mail_cleanup = True, \
                          filtre_in = "", filtre_out = "", status_columns = ""):
    # define threads
    md5_thread = Thread_Return(target = load_md5_table, \
                              args = (DB_conn, ), \
                              kwargs = {'limit' : 0})
    csv_thread = Thread_Return(target = load_text_file, \
                                  args = (str(file_path + "/" + file_name), ), \
                                  kwargs = {'header' : header})
    # launch threads
    if md5_lookup == True:
        md5_thread.start()
    csv_thread.start()
    # switch to thread result for load_text_file()
    #import_result = load_text_file(file_path + "/" + file_name, header)
    import_result = csv_thread.join()
    if import_result[0]:
        dataframe = import_result[1]
        dataframe_syntax = cleanup_mail_column(dataframe, mail_cleanup = mail_cleanup, \
                                               filtre_in = filtre_in, filtre_out = filtre_out, \
                                               status_columns = status_columns)
        # match up new records with existing ones in the DB
            # attempt to map with md5 matching
        if md5_lookup == True:
            # switch to thread result for load_md5_table()
            #load_md5_table_result = load_md5_table(DB_conn, limit = md5_table_limit)
            load_md5_table_result = md5_thread.join()
            if load_md5_table_result[0]:
                md5_df = load_md5_table_result[1]
                lookup_md5_result = lookup_md5(dataframe_syntax, md5_df)
                if lookup_md5_result[0]:
                    return lookup_md5_result # [True, dataframe_syntax]
                # if md5_lookup = False or md5 attempt fails, revert to query method
        lookup_query_result = lookup_query_base(dataframe_syntax, prepare_query = True, \
                                                lookup_query = "", db_schema = "")
        if lookup_query_result[0]:
            return lookup_query_result # [True, dataframe_syntax, query_name]
        else:
            return [False]
    else:
        message = "FATAL PROBLEM. Failed to populate initial DataFrame"
        print_to_log(log_file, 2, message)
        return [False]
        
# --- insert_new_records ---

def build_dataframe_for_new_records_insertion(df_syntax, drop_dupli_field_list):
    try:
        df_insert = df_syntax[df_syntax.exist == False]
        df_insert = df_insert.drop_duplicates(drop_dupli_field_list)
        #print "-- DEBUG : build_dataframe_for_new_records_insertion : df_insert --"
        #print df_insert
        #print "--"
        if len(df_insert.index) == 0:
            message = "Failed : No new records to insert in DB."
            print_to_log(log_file, 2, message)
            return [False]
        else:
            if len(df_insert.index) == 1:
                if df_insert.at[df_insert.index[0], 'mail'] in ['', 'NULL', 'NaN']:
                    message = "Failed : No new records to insert in DB."
                    print_to_log(log_file, 2, message)
                    return [False]
            insert_records = list(df_insert.index)
            message = "OK : Dataframe built with new records to be inserted in DB."
            print_to_log(log_file, 2, message)
            return [True, df_insert, insert_records]
    except:
        message = "Failed to create a subset of main dataframe with records to be inserted in DB."
        print_to_log(log_file, 2, message)
        e = sys.exc_info()
        for item in e:
            message = str(item)
            print_to_log(log_file, 3, message)
        return [False]

def build_dataframe_for_csv(dataframe, csv_dict = ""):
    df_csv = dataframe[['mail', 'domain']]
    # in case of other fields in 'base' table to be updated now, append info contained in csv_dict to the dataframe
    if csv_dict:
        for key, value in csv_dict.iteritems():
            df_csv[key] = value
    return df_csv

def create_file_name(fichier_num, table, length, header = "", comment = ""):
    file_name = str(fichier_num) + "_"
    if header != "":
        field_description = "["
        for field in header:
            field_description = field_description + str(field) + ","
        field_description = field_description[:len(field_description)-1] + "]"
        file_name = file_name + str(field_description) + "_"
    if comment != "":
        file_name = file_name + str(comment) + "_"
    k = str(int(length / 1000)) + "k"
    file_name = file_name + str(table) + "_" + k + ".csv" 
    return file_name

def remove_floating_part(value):
    num_sep = [",", "."]
    for item in num_sep:
        if item in value:
            pos = value.find(item)
            return int(value[:pos])
    return value

def write_to_csv(dataframe, path, folder, file_name, header = False):
    csv_file = path + "/" + folder + "/" + file_name
    write_cols = list(dataframe.columns)
    if len(dataframe.index) > 0:
        for col in write_cols:
            if "id" in col:
                try:
                    dataframe[col] = dataframe[col].astype(int)
                except:
                    try:
                        dataframe[col] = dataframe[col].apply(str)
                        dataframe[col] = dataframe[col].apply(lambda x: remove_floating_part(x))
                    except:
                        for cpt in range(len(dataframe.index)):
                            cpt_index = dataframe.index[cpt]
                            np_value = dataframe.at[cpt_index, col]
                            py_convert = convert_dataframe_scalar(np_value)
                            if py_convert[0]:
                                cell_content = py_convert[1]
                                cell_content = str(cell_content) 
                                if cell_content.isdigit():
                                    cell_content = int(cell_content)
                                    dataframe.at[cpt_index, col] = cell_content
                                else:
                                    dataframe.at[cpt_index, col] = remove_floating_part(cell_content)
                            else:
                                dataframe.at[cpt_index, col] = ""
            elif "md5" in col:
                pass
            elif "civilite" in col:
                dataframe[col] = dataframe[col].apply(str)
                dataframe[col] = dataframe[col].apply(lambda x: remove_floating_part(x))
            else:
                try:
                    dataframe[col] = dataframe[col].str.replace("'", "\\'")
                except:
                    try:
                        dataframe.loc[col] = dataframe[col].str.replace("'", "\\'")
                    except:
                        for cpt in range(len(dataframe.index)):
                            cpt_index = dataframe.index[cpt]
                            np_value = dataframe.at[cpt_index, col]
                            py_convert = convert_dataframe_scalar(np_value)
                            if py_convert[0]:
                                cell_content = py_convert[1]
                                cell_content = cell_content.replace("'", "\\'")
                                dataframe.at[cpt_index, col] = cell_content
                            else:
                                dataframe.at[cpt_index, col] = ""
        try:
            dataframe.to_csv(csv_file, sep = ";", cols = write_cols, \
                             header = header, index = False, \
                             na_rep = 'NULL', encoding = 'utf-8')   
            message = "OK : Dataframe written to csv : " + csv_file
            print_to_log(log_file, 2, message)
            return csv_file
        except:
            message = "Failed to write dataframe to csv. Will retry line by line with Python standard function."
            print_to_log(log_file, 2, message)
            e = sys.exc_info()
            for item in e:
                message = str(item)
                print_to_log(log_file, 3, message)
            #line_stack = []
            with codecs.open(csv_file, 'w', 'utf-8', 'ignore') as csv:
                for cpt_line in range(len(dataframe.index)):
                    cpt_index = dataframe.index[cpt_line]
                    try:
                        line_str = ";".join(str(dataframe.at[cpt_index, col]) for col in write_cols)
                        try:
                            csv.write(line_str + '\n')
                        except:
                            message = "Pb. with line %s. Will pass to next line." % str(cpt_line)
                            print_to_log(log_file, 4, message)
                            e = sys.exc_info()
                            for item in e:
                                message = str(item)
                                print_to_log(log_file, 3, message)
                        #line_stack.append(line_str)
                    except:
                        message = "Pb. with line %s. Will pass to next line." % str(cpt_line)
                        print_to_log(log_file, 4, message)
                        e = sys.exc_info()
                        for item in e:
                            message = str(item)
                            print_to_log(log_file, 3, message)
                #csv.writelines(line_stack)
            message = "OK : Dataframe written to csv : " + csv_file
            print_to_log(log_file, 2, message)
            return csv_file
    else:
        message = "Failed to write dataframe to csv. Empty dataframe passed to write_to_csv() function."
        print_to_log(log_file, 2, message)
        return ""
    
def write_csv_to_DB(write_table, csv_file, csv_columns, db_schema = ""):
    test_DB_connection(direct_DB_conn, db_package)
    try:
        csv_reader = codecs.open(csv_file, 'r', encoding='utf-8')
    except:
        return [False]
    if db_schema:
        table_argument = db_schema + "." + write_table + " (" + ', '.join(csv_columns) + ")"
        sql_copy_query = "COPY %s FROM STDIN WITH DELIMITER ';' ENCODING 'utf-8';" % (table_argument)
        csv_reader = codecs.open(csv_file, 'r', encoding='utf-8')
        try:
            direct_DB_cursor.copy_expert(sql_copy_query, csv_reader)
            direct_DB_conn.commit()
            message = "OK : csv imported to DB in table : " + write_table
            print_to_log(log_file, 2, message)
            return [True]
        except:
            message = "Failed to import csv with copy_expert() function."
            print_to_log(log_file, 3, message)
            e = sys.exc_info()
            for item in e:
                message = str(item)
                print_to_log(log_file, 3, message)
                direct_DB_conn.rollback()
            message = "Attempt to import csv with executemany() function."
            print_to_log(log_file, 3, message)
            df_executemany = pd.read_csv(csv_file, names = csv_columns, sep = ';', encoding = 'utf-8')
            insert_data_list = []
            for row in range(0, len(df_executemany.index)):
                insert_data_list.append(list([df_executemany.at[row,col] for col in df_executemany.columns]))
            sql_insert = "INSERT INTO " + db_schema + "." + write_table + " (" + ', '.join(csv_columns) + \
                            ") VALUES (" + ', '.join(list(['%s'] * len(csv_columns))) + ")"
            try:
                direct_DB_cursor.executemany(sql_insert, [insert_data for insert_data in insert_data_list])
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
                try:
                    direct_DB_conn.rollback()
                except:
                    test_DB_connection(direct_DB_conn, db_package)
                return [False]
    else:
        try:
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
            try:
                direct_DB_conn.rollback()
            except:
                test_DB_connection(direct_DB_conn, db_package)       
            return [False]

def insert_new_records(df_syntax, fichier_num, db_schema = "", update_dict = "", \
                       table = "", comment = "", path = "", folder = ""):
    insert_result = build_dataframe_for_new_records_insertion(df_syntax, 'mail')
    if insert_result[0]:
        df_insert = insert_result[1]
        inserted_records = insert_result[2]
        df_csv = build_dataframe_for_csv(df_insert, csv_dict = update_dict)
        csv_columns = list(df_csv.columns)
        if table == "":
            table = 'base'
        if comment == "":
            comment = "new-records"
        file_name = create_file_name(fichier_num, table, len(df_csv.index), \
                                     header = csv_columns, comment = comment)
        if path == "":
            path = "/media/freebox/Fichiers/ImportDB/Pandas/Test"
        if folder == "":
            folder = table
        csv_file = write_to_csv(df_csv, path, folder, file_name)
        if csv_file:
            insert_DB_result = write_csv_to_DB(table, csv_file, csv_columns, db_schema)
            if insert_DB_result[0]:
                message = "OK : " + str(len(inserted_records)) + " new records in DataFrame inserted in DB, in table '" + str(table) + "'."
                print_to_log(log_file, 2, message)
                return [True, inserted_records]
    return [False]

# --- post_insertion_update ---

def update_dataframe_status_column(dataframe, records_array, status_column, boolean_status):   
    try:
        dataframe.loc[records_array, status_column] = boolean_status
        message = "OK : Status " + str(status_column) + " updated to " + str(boolean_status) + "."
        print_to_log(log_file, 2, message)
        return [True, dataframe]
    except:
        message = "Failed to update status " + str(status_column) + " to " + str(boolean_status) + "."
        print_to_log(log_file, 2, message)
        return [False]
        
def build_id_series(dataframe, inserted_records, query_name):
    first_mail = dataframe.loc[inserted_records[0], 'mail']
    first_id = lookup_record_safe(query_name, first_mail)[1]
    last_mail = dataframe.loc[inserted_records[-1], 'mail']
    last_id = lookup_record_safe(query_name, last_mail)[1]
    if ((last_id - first_id) - (len(inserted_records) - 1)) == 0:
        id_series = range(first_id, last_id + 1, 1)
        return [True, id_series]
    else:
        message = "Problem with the insertion. Impossible to build a correct series of id."
        print_to_log(log_file, 2, message)
        return [False]
    
def apply_id_series(dataframe, inserted_records, id_series):
    dataframe.loc[inserted_records, 'pg_id'] = id_series
    return dataframe
    
def check_id_series(dataframe, inserted_records, lookup_query):
    random.seed()
    test_loop = int(len(inserted_records) * (3 /100))
    if test_loop < 3:
        if len(inserted_records) < 3:
            test_loop = len(inserted_records)
        else: 
            test_loop = 3
    for cpt in range(test_loop):
        random_position = random.randint(0, len(inserted_records) - 1)
        random_index = inserted_records[random_position]
        random_mail = dataframe.loc[random_index]['mail']
        random_pg_id = dataframe.loc[random_index]['pg_id']
        lookup_result = lookup_record_safe(lookup_query, random_mail)
        if lookup_result[0]:
            if lookup_result[1] != random_pg_id:
                message = "Failed to match theoratical 'pg_id' with real 'id' recorded in DB : mail = " \
                + str(random_mail) + "; pg_id = " + str(random_pg_id) + "; id = " + str(lookup_result[1])
                print_to_log(log_file, 2, message)
                return [False]
        else:
            return [False]
    return [True]

def post_insertion_update(dataframe, inserted_records, lookup_query):
    # this function call is useless. maybe could be useful with a new status column 'now_in_db' ??
    #status_result = update_dataframe_status_column(dataframe, inserted_records, \
    #                                               'insert', True)
    #if status_result[0]:
    #    dataframe = status_result[1]
    series_result = build_id_series(dataframe, inserted_records, lookup_query)
    if series_result[0]:
        dataframe = apply_id_series(dataframe, inserted_records, series_result[1])
        show_df(dataframe)
        #check_series_result = check_id_series(dataframe, inserted_records, lookup_query)
        try:
            check_series_result = check_id_series(dataframe, inserted_records, lookup_query)
        except:
            message = "Failed to check theoratical id series vs. real ids recorded in the DB."
            print_to_log(log_file, 2, message)
        if check_series_result[0]:
            message = "OK : New records in DataFrame updated with corresponding 'pg_id'."
            print_to_log(log_file, 2, message)
            show_df(dataframe)
            return [True, dataframe]
        else:
            need_lookup = True        
    else:
        need_lookup = True
    if need_lookup == True:
        try:
            dataframe = lookup_mail_columnwide(dataframe, lookup_query, \
                        index_slice = inserted_records, add_pg_id_column = False)
        except:
            message = "Failed to retrieve ids of newly inserted mails."
            print_to_log(log_file, 2, message)
            return [False]
        message = "OK : New records in DataFrame updated with corresponding 'pg_id'."
        print_to_log(log_file, 2, message)
        return [True, dataframe]
    
# --- insert_provenance_&_status ---

def prepare_dataframe_for_side_tables(dataframe, only_new = False):
    try:
        if only_new == False:
            map_insert = dataframe['insert'].map(lambda x: x == True)
            map_exist = dataframe['exist'].map(lambda x: x == True)
            df_temp = dataframe[map_insert | map_exist]
            message_part = "'insert' and 'exist' (ie. all valid mails in file)"
        else:
            map_insert = dataframe['insert'].map(lambda x: x == True)
            df_temp = dataframe[map_insert]
            message_part = "'insert' (ie. only new mails in file)"
        df_temp = df_temp.drop_duplicates('mail')
        df_temp = df_temp['pg_id']
        message = "OK : Dataframe built with only " + message_part + ", with no duplicates in 'mail' : " + \
        str(len(df_temp.index)) + " records."
        print_to_log(log_file, 2, message)
        return [True, df_temp]
    except:
        message = "Failed to build DataFrame to insert provenance in DB for records " + message_part + "."
        print_to_log(log_file, 2, message)
        return [False]

def prepare_dataframe_special_ok_files(dataframe):
    try:
        map_insert = dataframe['insert'].map(lambda x: x == True)
        #map_exist = dataframe['exist'].map(lambda x: x == True)
        df_temp = dataframe[map_insert]
        df_temp = df_temp.drop_duplicates('mail')
        df_temp = df_temp['pg_id']
        message = "OK : Dataframe built with only 'insert', with no duplicates in 'mail' : " + \
        str(len(df_temp.index)) + " records."
        print_to_log(log_file, 2, message)
        return [True, df_temp]
    except:
        message = "Failed to build DataFrame to insert provenance in DB for records 'exist' or 'insert'."
        print_to_log(log_file, 2, message)
        return [False]

def insert_records_provenance(dataframe, prov_fichier_id, fichier_num, ok_case = False, \
                              second_attempt = False, db_schema = "", table = "", comment = "", \
                              path = "", folder = ""):
    if len(dataframe.index) != 0:
        if ok_case == True:
            second_attempt = True
        if second_attempt == True | ok_case == True:
            dataframe_result = prepare_dataframe_for_side_tables(dataframe, only_new = True)
            #dataframe = dataframe[dataframe['exist'] == False]
        else:
            dataframe_result = prepare_dataframe_for_side_tables(dataframe)
        if dataframe_result[0]:
            df_pg_id = dataframe_result[1]
            if len(df_pg_id.index) != 0:
                df_csv = pd.DataFrame(columns = ['id', 'fichier_id'], index = df_pg_id.index)
                df_csv['id'] = df_pg_id
                df_csv['fichier_id'] = prov_fichier_id
                if not table:
                    table = "fichier_match"
                show_df(df_csv)
                df_csv = remove_duplicates_in_inserts_csv(df_csv, table, fichier_num, join_key = 'id')
                if len(df_csv.index) != 0:
                    #return [True, df_csv]
                    show_df(df_csv)
                    csv_columns = list(df_csv.columns)
                    if table == "":
                        table = 'fichier_match'
                    if comment == "":
                        comment = "new-records"
                    file_name = create_file_name(fichier_num, table, len(df_csv.index), \
                                                 header = csv_columns, comment = comment)
                    if path == "":
                        path = "/media/freebox/Fichiers/ImportDB/Pandas/Test"
                    if folder == "":
                        folder = table
                    csv_file = write_to_csv(df_csv, path, folder, file_name)
                    if csv_file:
                        insert_DB_result = write_csv_to_DB(table, csv_file, csv_columns, db_schema)
                        if insert_DB_result[0]:
                            message = "OK : " + str(len(df_csv.index)) + " provenance info inserted in '" + \
                            str(table) + "' for new records."
                            print_to_log(log_file, 2, message)
                            return [True]
                        else:
                            message = "Failed to insert provenance in '" + str(table) + "'. DB copy_to operation rolled back."
                            print_to_log(log_file, 2, message)
                            df_csv = remove_duplicates_in_inserts_csv(df_csv, table, fichier_num)
                            if len(df_csv.index) != 0:
                                file_name = create_file_name(fichier_num, table, len(df_csv.index), \
                                                         header = csv_columns, comment = comment)
                                csv_file = write_to_csv(df_csv, path, folder, file_name)
                                insert_DB_result = write_csv_to_DB(table, csv_file, csv_columns, db_schema)
                                if insert_DB_result[0]:
                                    message = "OK : " + str(len(df_csv.index)) + " provenance info inserted in '" + \
                                    str(table) + "' for new records (after deduplication with past import)."
                                    print_to_log(log_file, 2, message)
                                    return [True]
                                else:
                                    if second_attempt == False:
                                        message = "Second attempt to insert provenance with only new records."
                                        print_to_log(log_file, 2, message)
                                        return insert_records_provenance(dataframe, prov_fichier_id, fichier_num, ok_case, True, \
                                                                  db_schema, table, comment, path, folder)
                                    else:
                                        return [False]
                            else:
                                message = "OK : No new provenance info to record in this case. All records identified as duplicates."
                                print_to_log(log_file, 2, message)
                                return [False]
                    else:
                        message = "Pb. Failed to write records to the csv file : %s | %s | %s" % (str(path), str(folder), str(file_name))
                        print_to_log(log_file, 2, message)
                        return [False]
                else:
                    message = "OK : Empty dataframe for insert of fields %s in table '%s'" % (str(list(df_csv.columns)), str(table))
                    print_to_log(log_file, 2, message)
                    return [False]
            else:
                message = "Failed to insert provenance in '" + str(table) + "'. No records found after filtering on 'exist' and/or 'insert' fields."
                print_to_log(log_file, 2, message)
                return[False]
        else:
            # error message given in previous function
            return [False]   
    else:
        message = "Failed to insert provenance in '" + str(table) + "'. No records found."
        print_to_log(log_file, 2, message)
        return [False]
    
def analyse_insert_status(status_list):
    if status_list:
        status_updated = []
        for item in status_list:
            date_insert = [False]
            if isinstance(item, basestring):
                status = item
            else:
                status = item[0]
                if len(item) > 1:
                    date_insert = [True, clean_date(item[1])]  
            for field_insert, field_list in fields_list_dict.iteritems():
                if status in field_list:
                    table = table_dict[field_insert]
                    status_list = [table]
                    csv_fields = ['id']
                    if date_insert[0] and date_insert[1] != "":
                        csv_fields.append('date')
                    status_list.append(csv_fields)
                    status_updated.append(status_list)
        if status_updated:
            return [True, status_updated]
    return [False]

global fields_list_dict, table_dict, extra_fields_list_dict
fields_list_dict = {'b2b' : ['b2b', 'mimi_b2b'], 'ouvreur' : ['ouvreur', 'mimi_ouvreur'], \
                    'plainte' : ['plainte', 'mimi_plainte'], 'npai' : ['npai', 'base_mimi_npai'], \
                    'pcp' : ['pcp'], 'pcp-wl' : ['pcpwl'], 'pcp-sm' : ['pcpsm'], \
                    'ok_npai' : ['ok_npai', 'ok'], 'desabo' : ['desabo'], \
                    'emarsys' : ['emarsys'], 'emarsys_plainte' : ['emarsys_plainte']}
table_dict = {'b2b' : 'base_b2b', 'ouvreur' : 'base_mimi_ouvreur', \
              'plainte' : 'base_mimi_plainte', 'npai' : 'base_mimi_npai', \
              'pcp' : 'base_pcp', 'pcp-wl' : 'regie_pcp_wl', 'pcp-sm' : 'regie_pcp_sm', \
              'ok_npai' : 'base_emailverifier', 'desabo' : 'desabo', \
              'emarsys' : 'base_emarsys', 'emarsys_plainte' : 'emarsys_plainte'}
extra_fields_list_dict = {'id' : ['nom', 'prenom', 'civilite', 'cp', 'birth', 'ville'], \
                          'lead' : ['ip', 'provenance', 'date'], \
                          'md5' : ['md5'], \
                          'appetence_match' : ['interet_score']}

def insert_status(dataframe, status_list, fichier_num, db_schema = "", comment = "", path = "", folder = ""):
    if path == "":
        path = "/media/freebox/Fichiers/ImportDB/Pandas/Test"
    if comment == "":
        comment = "all"
    if len(dataframe.index) != 0:
        if status_list:
            status_updated = []
            dataframe_result = prepare_dataframe_for_side_tables(dataframe)
            if dataframe_result[0]:
                df_pg_id = dataframe_result[1]
                if len(df_pg_id.index) != 0:
                    for item in status_list:
                        date_insert = [False]
                        if isinstance(item, basestring):
                            status = item
                        else:
                            status = item[0]
                            if len(item) > 1:
                                date_insert = [True, clean_date(item[1])]  
                        for field_insert, field_list in fields_list_dict.iteritems():
                            if status in field_list:
                                table = table_dict[field_insert]
                                folder = table
                                try:
                                    df_csv = pd.DataFrame()
                                    df_csv['id'] = df_pg_id
                                except:
                                    message = "Failed to build DataFrame to insert records in table '" + \
                                    str(table) + "'. Field identified : " + str(field_insert) + "."
                                    print_to_log(log_file, 2, message)
                                    return [False]
                                if date_insert[0] and date_insert[1] != "":
                                    df_csv['date'] = date_insert[1]
                                df_csv = remove_duplicates_in_inserts_csv(df_csv, table, fichier_num)
                                if len(df_csv.index) != 0:
                                    csv_columns = list(df_csv.columns)
                                    file_name = create_file_name(fichier_num, table, len(df_csv.index), \
                                                                 header = csv_columns, comment = comment)    
                                    csv_file = write_to_csv(df_csv, path, folder, file_name)
                                    if csv_file:
                                        insert_DB_result = write_csv_to_DB(table, csv_file, csv_columns, db_schema)
                                        if insert_DB_result[0]:
                                            status_updated.append(field_insert)
                                            message = "OK : " + str(len(df_csv.index)) + " " + str(field_insert) + \
                                            " info inserted in '" + str(table) + "' for new records."
                                            print_to_log(log_file, 2, message)
                                else:
                                    message = "OK : No new records to insert in table '%s' for fields %s" % (str(table), str(list(df_csv.columns)))
                                    print_to_log(log_file, 2, message)
                    if len(status_updated) > 0:
                        return [True, status_updated]
                    else:
                        message = "No field recognized in status_list argument. No status update."
                        print_to_log(log_file, 2, message)
                        return [False]
                else:
                    message = "No records found after filtering on 'exist' and/or 'insert' fields."
                    print_to_log(log_file, 2, message)
                    return [False]
            else:
                return [False]
        else:
            message = "Empty status_list."
            print_to_log(log_file, 2, message)
            return [False]  
    else:
        message = "Empty DataFrame."
        print_to_log(log_file, 2, message)
        return [False]

# --- clean_data ---

def convert_dataframe_scalar(np_value):
    if np_value != "":
        try:
            py_value = np_value.encode('utf-8').strip()
        except:
            try:
                py_value = np.asscalar(np_value)
            except:
                return [False, np_value, "Failed"]
        return [True, py_value]
    else:
        return [False, np_value, "Empty"]

def check_for_bad_text(string):
    return string
    """unicode_string = unicode(string)
    num_ascii_list =[153,154,158,162,163,164,165,166,167,169,170,171,174,188,189,190,191,192,193,194,195,196,197]
    ascii_list = [unichr(i) for i in num_ascii_list]
    for ascii_char in ascii_list:
        if ascii_char in unicode_string:
            #print unicode_string
            return fix_bad_unicode(unicode_string)
    return string"""

def get_separator_list(separator, factor):
    separator_list = []
    for cpt_factor in range((factor + 1), 1, -1):
        char = str(separator * cpt_factor)
        #separator_list.append(str(" " + char + " "))
        #separator_list.append(str(" " + char))
        #separator_list.append(str(char + " "))
        separator_list.append(str(char))
    return separator_list

def get_separator_dict(sep_dict):
    separator_dict = {}
    for separator, factor in sep_dict.iteritems:
        separator_dict[separator] = get_separator_list(separator, factor)
    return separator_dict

def clean_separator(value, separator_dict, universal_separator, city_mode = False):
    city_word = ['le', 'la', 'de', 'du', 'des', 'les', 'a', 'aux', 'lez', 'en', 'dans', 'sous', 'sur']
    particule_word = ['de', 'du', 'des']
    try:
        for separator, separator_list in separator_dict.iteritems():
            for item in separator_list:
                if item in value:
                    word_list = list(value.split(item))
                    new_word_list = []
                    for word in word_list:
                        word = word.strip()
                        if not word.isdigit():
                            if city_mode and word in city_word:
                                word = word.lower()
                            elif not city_mode and word in particule_word:
                                word = [word.lower(), 'particule']
                            else:
                                try:
                                    if "d'" in word:
                                        pos = word.find("d'")
                                        word = word[:pos+1].lower() + word[pos+2].upper() + word[pos+3:].lower() 
                                    else:
                                        word = word[0].upper() + word[1:].lower()
                                except:
                                    pass
                            new_word_list.append(word)
                    if len(new_word_list) > 1:
                        if not city_mode:
                            for word in new_word_list:
                                if isinstance(word, list):
                                    if word[1] == 'particule':
                                        particule_list = []
                                        for word in new_word_list:
                                            if isinstance(word, list):
                                                particule_list.append(word[0].lower())
                                            else:
                                                particule_list.append(word[0].upper() + word[1:].lower())
                                        try:
                                            new_value  = str(" ".join(particule_list))
                                        except:
                                            new_value = value
                                        return new_value
                    try:
                        if universal_separator:
                            new_value = str(universal_separator.join(new_word_list))
                        else:
                            new_value = str(separator.join(new_word_list))
                    except:
                        new_value = value
                    return new_value
        if not value.isdigit():
            new_value = value[0].upper() + value[1:].lower()
            return new_value
        else:
            return value
    except:
        return value

def clean_name(value, separator_check = True, city_mode = False):
    separator_dict = {'-' : ['----', '---', '--', '-'], \
                      '/' : ['////', '///', '//', '/'], \
                      '_' : ['____', '___', '__', '_'], \
                      ":" : ["::::",":::", "::", ":"], \
                      "#" : ["####", "###", "##", "#"], \
                      '~' : ['~~~~', '~~~', '~~', '~'], \
                      "'" : ["''''","'''", "''"], \
                      ',' : [',,,,', ',,,', ',,'], \
                      ';' : [';;;;', ';;;', ';;']}
    if isinstance(value, basestring):
        value = ' '.join(value.split())
        value = value.replace('\\', '')
        value = value.replace('?', 'e')
        if separator_check and separator_dict:
            value = clean_separator(value, separator_dict, '-', city_mode = city_mode)
        else:
            if not value.isdigit():
                try:
                    value = value[0].upper() + value[1:].lower()
                except:
                    pass
        if len(value) > 40:
            value = value[:40]
        return value
    else:
        return ""

def clean_ville(value):
    return clean_name(value, city_mode = True)
    

def clean_cp(value):
    if value.isdigit():
        len_value = len(str(value))
        if len_value == 5:
            return str(value)
        elif len_value == 4:
            value = "0" + str(value)
            return str(value)
        elif len_value == 3:
            value = "0" + str(value) + "0"
            return str(value)
        else:
            return ""
    else:
        return ""
    
def clean_tel(value):
    if value.isdigit():
        len_value = len(str(value))
        if len_value == 10:
            return str(value)
        elif len_value == 9:
            value = "0" + str(value)
            return str(value)
        else:
            return ""
    else:
        return ""
    
def clean_civilite(value):
    if isinstance(value, basestring):
        if not value.isdigit():
            convert_civilite = {'M' : 1, 'Mr' : 1, 'M.' : 1, "Mr." : 1, "Mlle" : 2, "Mme" : 3}
            for key, return_value in convert_civilite.iteritems():
                if value.lower() == key.lower():
                    return return_value
            return 1
        else:
            return ""
    elif isinstance(value, (int, long)):
        if value in [1, 2, 3]:
            return value
        else:
            return 1
    else:
        return ""

def clean_birth(value, dayfirst = True):
    if isinstance(value, basestring):
        try:
            parsed_date = dparser.parse(value, fuzzy = True, dayfirst = dayfirst)
        except:
            return ""
        if parsed_date.year < 1900:
            parsed_date = parsed_date + relativedelta(years = 100)
        elif parsed_date.year > 2000:
            parsed_date = parsed_date - relativedelta(years = 100)
        if (parsed_date.year < 1900) or (parsed_date.year > 2000):
            return ""
        try:
            formatted_date = parsed_date.strftime('%d/%m/%y')
        except:
            try:
                print "date problem :" + str(parsed_date)
            except:
                pass
            return ""
        #print "clean_birth : " + value + " --> " + str(parsed_date) + " --> " + str(formatted_date)
        return formatted_date
    else:
        return ""

def clean_ip(value):
    if isinstance(value, basestring):
        if value.count('.') == 3:
            return value
    return ""

def clean_provenance(value):
    if isinstance(value, basestring):
        return value
    else:
        return ""

def clean_date(value):
    return clean_birth(value)

def clean_int_score(value):
    if value:
        if value != "NaN":
            try:
                new_dict = dict((int(k), int(v)) for k, v in \
                            (part.split(': ') for part in \
                             re.sub('[{}]', '', value).split(', ')))
                return new_dict
            except:
                return ""
    return ""

def clean_data(dataframe, field):
    cleaning_scripts = {'prenom' : clean_name, 'nom' : clean_name, 'cp' : clean_cp, \
						'ville' : clean_ville, 'civilite' : clean_civilite, 'birth' : clean_birth, \
                        'ip' : clean_ip, 'provenance' : clean_provenance, 'date' : clean_date, \
                        'port' : clean_tel, 'tel' : clean_tel, 'fax' : clean_tel, \
                        'interet_score' : clean_int_score}
    string_fields = ['cp', 'port', 'tel', 'fax']
    milestones = {1 : 1000, 2 : 2000, 3 : 5000, 4 : 10000, 5 : 20000, 6 : 50000, \
                  7 : 100000, 8 : 200000, 9 : 500000, 10 : 1000000, 11 : 2000000, 12: 5000000}
    cpt_milestones = 1
    if field in list(cleaning_scripts.keys()):
        if field in string_fields:
            dataframe[field] = dataframe[field].astype('string', copy = True)
            show_df(dataframe[field])
            try:
                dataframe[field] = dataframe[field].str.strip()
            except:
                pass
        cleaning_function = cleaning_scripts[field]
        index = dataframe.index
        fix_list = []
        for cpt in range(len(index)):
            if cpt == milestones[cpt_milestones]:
                message = "OK. First %s data for field '%s' looked up." % (str(milestones[cpt_milestones]), str(field))
                print_to_log(log_file, 4, message)
                cpt_milestones += 1
                print_to_log(log_file, 4, str(fix_list))
            np_value = dataframe.at[index[cpt], field]
            py_convert = convert_dataframe_scalar(np_value)
            if py_convert[0]:
                raw_value = py_convert[1]
                unicode_clean_value = check_for_bad_text(raw_value)
                if raw_value != unicode_clean_value:
                    this_fix = [raw_value, unicode_clean_value]
                    fix_list.append(this_fix)
                clean_value = cleaning_function(unicode_clean_value)
                for csv_separator in [';', ',']:
                    try:
                        clean_value = clean_value.replace(csv_separator, '')
                    except:
                        clean_value = str(clean_value)
                        clean_value = clean_value.replace(csv_separator, '')
            else:
                clean_value = ""
            dataframe.at[index[cpt], field] = clean_value
        if len(fix_list) > 0:
            message = "OK : Data fixed for bad unicode in %s occasions." % str(len(fix_list))
            print_to_log(log_file, 3, message)
            for item in fix_list:
                print " --> ".join(item) 
        return dataframe
    else:
        return dataframe

# --- insert_extra_fields ---

def get_duplicate_records(dataframe, field_list):
    dupli_map = dataframe.duplicated(field_list)
    dupli_only = dupli_map[dupli_map == True]
    dupli_records = list(dupli_only.index)
    return dupli_records

def update_id_duplicate_records(dataframe):
    duplicate_records = get_duplicate_records(dataframe, 'mail')
    if len(duplicate_records) != 0:
        index = list(dataframe.index)
        for cpt in range(len(index)):
            if index[cpt] in duplicate_records:
                ref_pg_id = dataframe.at[index[cpt - 1], 'pg_id']
                dataframe.at[index[cpt], 'pg_id'] = ref_pg_id
        message = "OK : Duplicate records on 'mail' updated with 'pg_id' values."
        print_to_log(log_file, 2, message)
    return dataframe 
    
def check_all_records_for_id(dataframe):
    pass

def analyze_extra_fields(data_columns):
    #data_columns = list(dataframe.columns)
    csv_fields = {}
    result_dict = {}
    for table, table_fields_list in extra_fields_list_dict.iteritems():
        csv_fields[table] = []
        for field in data_columns:
            if field in table_fields_list:
                csv_fields[table].append(field)
        if csv_fields[table]:
            result_dict[table] = [table, csv_fields[table]]
    if result_dict:
        result = [True]
        for field_map in result_dict.itervalues():
            result.append(field_map)
        return result
    else:
        return [False]

def threading_query_duplicates_removal(fields, table, fichier_num):
    #header = list(df_csv.columns)
    if table in ['base_b2b', 'base_mimi_npai', 'base_mimi_ouvreur', 'base_mimi_plainte', \
                 'regie_pcp_wl', 'regie_pcp_sm']:
        field_str = ", ".join((str(table) + "." + str(field)) for field in fields)
        select_part = "SELECT %s FROM %s " % (field_str, table)
        join_part = "INNER JOIN fichier_match ON fichier_match.id = %s.id " % (table)
    else:
        field_str = ", ".join(str(field) for field in fields)
        select_part = "SELECT %s FROM %s " % (field_str, table)
        join_part = "INNER JOIN fichier_match ON fichier_match.id = %s.mail_id " % (table)
    where_part = "WHERE fichier_match.fichier_id IN " + \
            "(SELECT fichier_list.fichier_id FROM fichier_list WHERE fichier_num LIKE '%s')" \
            % (str(fichier_num))
    if table == "fichier_match":
        query = select_part + where_part + ";" # impossible to join fichier_match on itself
    else:
        query = select_part + join_part + where_part + ";"
    try:
        table_df = sql.read_sql(query, direct_DB_conn, coerce_float=False)
        message = "OK : SQL Thread OK : %s records for table '%s' and fichier '%s' imported to DataFrame (stored in 'dupli_dict') for duplicates removal." \
                    % (str(len(table_df.index)), str(table), str(fichier_num))
        print_to_log(log_file, 4, message)
        show_df(table_df)
        return table_df
    except:
        e = sys.exc_info()
        for item in e:
            message = str(item)
            print_to_log(log_file, 3, message)
        return ""

def threading_get_dict_duplicates_removal(header, status_list, insert_prov, fichier_num):
    global dupli_dict
    dupli_dict = {}
    if insert_prov:
        fields = ['id', 'fichier_id']
        table = 'fichier_match'
        get_prov_table = Thread_Return(target = threading_query_duplicates_removal, \
                                               args = (fields, table, fichier_num))
        get_prov_table.start()
        dupli_dict[table] = get_prov_table.join()
    status_check = analyse_insert_status(status_list)
    if status_check[0]:
        thread_dict = {}
        for field_map in status_check[1]:
            table = field_map[0]
            fields = field_map[1]
            thread_dict[table] = Thread_Return(target = threading_query_duplicates_removal, \
                                               args = (fields, table, fichier_num))
            thread_dict[table].start()
            dupli_dict[table] = thread_dict[table].join()
    extra_fields = analyze_extra_fields(header)
    if extra_fields[0]:
        thread_dict = {}
        for field_map in extra_fields[1:]:
            table = field_map[0]
            fields = ['mail_id']
            for item in field_map[1:][0]:
                fields.append(item)
            thread_dict[table] = Thread_Return(target = threading_query_duplicates_removal, \
                                               args = (fields, table, fichier_num))
            thread_dict[table].start()
            dupli_dict[table] = thread_dict[table].join()

def drop_missing_field(dataframe, csv_fields):
    df_drop = pd.DataFrame(index = dataframe.index, columns = csv_fields)
    for field in csv_fields:
        df_drop[field] = dataframe[field]
    df_drop = df_drop.dropna(how='all')
    clean_index = df_drop.index
    df_res = dataframe.loc[clean_index]
    return df_res

def modify_csv_if_dict(df_csv, dict_field, key_field, value_field):
    milestones = {1 : 1000, 2 : 2000, 3 : 5000, 4 : 10000, 5 : 20000, 6 : 50000, \
                  7 : 100000, 8 : 200000, 9 : 500000, 10 : 1000000, 11 : 2000000, 12: 5000000}
    cpt_milestones = 1
    existing_fields = list(df_csv.columns)
    inherited_fields = filter(lambda fields: fields != dict_field, existing_fields)
    new_fields = list(inherited_fields)
    new_fields.extend([key_field, value_field])
    data = []
    cpt_lookup = 0
    for cpt in range(len(df_csv.index)):
        cpt_lookup += 1
        if cpt_lookup == milestones[cpt_milestones]:
            message = "OK. First " + str(milestones[cpt_milestones]) + " mails looked up."
            print_to_log(log_file, 4, message)
            cpt_milestones += 1
        cpt_index = df_csv.index[cpt]
        dict_arg = df_csv.at[cpt_index, dict_field]
        if dict_arg != "":
            if isinstance(dict_arg, dict):
                for key, value in dict_arg.iteritems():
                    record = []
                    for field in inherited_fields:
                        record.append(df_csv.at[cpt_index, field])
                    record.append(key)
                    record.append(value)
                data.append(record)
    new_df = pd.DataFrame(data, columns = new_fields)
    #show_df(new_df)
    return new_df

def remove_duplicates_in_inserts_csv(df_csv, table, fichier_num, join_key = ""):
    table_flag = False
    if 'dupli_dict' in globals():
        if table in dupli_dict.iterkeys():
            try:
                if not dupli_dict[table].empty:
                    table_flag = True
                    table_df = dupli_dict[table]
            except:
                pass
    if not table_flag:
        header = list(df_csv.columns)
        if table in ['base_b2b', 'base_mimi_npai', 'base_mimi_ouvreur', 'base_mimi_plainte']:
            field_str = ", ".join((str(table) + "." + str(field)) for field in header)
            select_part = "SELECT %s FROM %s " % (field_str, table)
            join_part = "INNER JOIN fichier_match ON fichier_match.id = %s.id " % (table)
        else:
            field_str = ", ".join(str(field) for field in header)
            select_part = "SELECT %s FROM %s " % (field_str, table)
            join_part = "INNER JOIN fichier_match ON fichier_match.id = %s.mail_id " % (table)
        where_part = "WHERE fichier_match.fichier_id IN " + \
                "(SELECT fichier_list.fichier_id FROM fichier_list WHERE fichier_num LIKE '%s')" \
                % (str(fichier_num))
        if table == "fichier_match":
            query = select_part + where_part + ";" # impossible to join fichier_match on itself
        else:
            query = select_part + join_part + where_part + ";"
        try:
            table_df = sql.read_sql(query, direct_DB_conn, coerce_float=False)
            message = "OK : No Thread table found : %s records for table '%s' and fichier '%s' imported to DataFrame (stored in 'dupli_dict') for duplicates removal." \
                    % (str(len(table_df.index)), str(table), str(fichier_num))
            print_to_log(log_file, 4, message)
            show_df(table_df)
        except:
            e = sys.exc_info()
            for item in e:
                message = str(item)
                print_to_log(log_file, 3, message)
            return df_csv
    if len(df_csv.index) > 0:
        #table_df['exist'] = True
        header = list(df_csv.columns)
        if not join_key:
            join_key = header
        # decision to remove dates from join_key because it creates too much of a mess
        remove_join_key = ['date', 'birth']
        for key in remove_join_key:
            if key in join_key:
                join_key.remove(key)
        try:
            # actual call to map_existing_rows()
            map_rows = map_existing_rows(df_csv, table_df, join_key = join_key, exist_key = 'exist', new_key = 'insert')
            if map_rows[0]:
                mapped_df = map_rows[1]
                #show_df(df)
                #show_df(df[(df['exist'] == True)])
                #show_df(df[(df['insert'] == True)])
                result = map_rows[2]
                # show results to log & console
                message = "OK : insert to '%s' table matching with on %s fields with SQL query (to identify possible duplicates)." \
                            % (str(table), str(header))
                print_to_log(log_file, 4, message)
                kv = []
                for k, v in result.iteritems():
                    kv.append(str(k) + " : " + str(v))
                message = str(" | ".join(kv))
                print_to_log(log_file, 4, message)
            else:
                message = "Pb. with matching on %s fields in table '%s'. Will try insert with initial dataframe." \
                            % (str(header), str(table))
                print_to_log(log_file, 4, message)
                return df_csv
            # filter only new records (ie. insert = True)
            df_csv = mapped_df[mapped_df['insert'] == True]
            #show_df(df_csv)
            # clean up columns in merged dataframe (whatever_x & whatever_y problem)
            merged_header = list(df_csv.columns)
            new_header = []
            rename_column_dict = {}
            for field in header:
                if field in merged_header:
                    new_header.append(field)
                else:
                    for merged_field in merged_header:
                        if field in merged_field:
                            new_header.append(field)
                            rename_column_dict[merged_field] = field
                            break
            if rename_column_dict:
                df_csv.rename(columns = rename_column_dict, inplace = True)
            df_csv = df_csv[new_header]
            # try to solve trailing .0 in _id colmuns (ie. get rid of float type)
            if len(df_csv.index) > 0:
                for field in new_header:
                    if "id" in field:
                        try:
                            df_csv[field] = df_csv[field].astype(int)
                        except:
                            try:
                                df_csv[field] = df_csv[field].apply(str)
                                df_csv[field] = df_csv[field].apply(lambda x: remove_floating_part(x))
                            except:
                                pass
            #show_df(df_csv)
            return df_csv
        except:
            e = sys.exc_info()
            for item in e:
                message = str(item)
                print_to_log(log_file, 3, message)
            return df_csv
    else:
        return df_csv

def build_extra_fields_dataframe(dataframe, csv_fields, table, fichier_num):
    check_for_duplicates = True
    if table in ['md5']:
        dataframe = dataframe[dataframe['insert'] == True]
        check_for_duplicates = False
    try:
        df_csv = pd.DataFrame()
        df_csv['mail_id'] = dataframe['pg_id']
    except:
        message = "Failed to build DataFrame to insert records in table '" + str(table) + \
        "'. Fields identified : " + str(csv_fields) + "."
        print_to_log(log_file, 2, message)
        e = sys.exc_info()
        for item in e:
            message = str(item)
            print_to_log(log_file, 3, message)
        return [False]
    for field in csv_fields:
        df_csv[field] = dataframe[field]
    df_csv = drop_missing_field(df_csv, csv_fields)
    for field in csv_fields:    
        df_csv = clean_data(df_csv, field)
    for field in csv_fields:
        if field in ["interet_score"]:
            df_csv = modify_csv_if_dict(df_csv, field, "appetence_id", "score")
    if check_for_duplicates:
        df_csv = remove_duplicates_in_inserts_csv(df_csv, table, fichier_num)
    if len(df_csv.index) == 0:
        message = "OK : Empty dataframe for insert of fields %s in table '%s'" % (str(csv_fields), str(table))
        print_to_log(log_file, 2, message)
        return [False]
    else:
        return [True, df_csv]

def default_csv_arguments(table, folder = "", comment = "", path = ""):
    if not folder:
        folder = table
    if not comment:
        comment = "all"
    if not path:
        path = "/media/freebox/Fichiers/ImportDB/Pandas/Test"
    return [folder, comment, path]

def insert_extra_fields(dataframe, fichier_num, db_schema = "", folder = "", comment = "", path = ""):
    dataframe_x = update_id_duplicate_records(dataframe)
    if len(dataframe_x.index) != 0:
        analyse_result = analyze_extra_fields(list(dataframe_x.columns))
        if analyse_result[0]:
            insert_flag = False
            for insert_map in analyse_result[1:]:
                table = insert_map[0]
                csv_fields = insert_map[1]
                dataframe_x_result = build_extra_fields_dataframe(dataframe_x, csv_fields, table, fichier_num)
                if dataframe_x_result[0]:
                    df_csv = dataframe_x_result[1]
                    csv_columns = list(df_csv.columns)
                    csv_arguments = default_csv_arguments(table, folder = "", comment = "", path = "")
                    file_name = create_file_name(fichier_num, table, len(df_csv.index), \
                                                 header = csv_columns, comment = csv_arguments[1])
                    csv_file = write_to_csv(df_csv, csv_arguments[2], csv_arguments[0], file_name)
                    if csv_file:
                        insert_DB_result = write_csv_to_DB(table, csv_file, csv_columns, db_schema)
                        if insert_DB_result[0]:
                            message = "OK : " + str(len(df_csv.index)) + " ID info inserted in '" + str(table) + \
                            "' for new records. Fields : " + str(csv_columns) + "."
                            print_to_log(log_file, 2, message)
                            insert_flag = True
                else:
                    return [False]
            if insert_flag:
                return [True]
            else:
                return [False]
        else:
            return [False, "fields"]
    else:
        [False, "records"]

"""
def insert_ID(dataframe, fichier_num, id_fields_list = "", \
               table = "", comment = "", path = "", folder = ""):
    csv_fields = extra_field_header(dataframe, table)
    if csv_fields:
    if id_fields_list == "":
        id_fields_list = ['nom', 'prenom', 'civilite', 'cp', 'birth', 'ville']
    if len(dataframe.index) != 0:
        data_columns = list(dataframe.columns)
        csv_fields = []
        for item in id_fields_list:
            if item in data_columns:
                csv_fields.append(item)
        if len(csv_fields) != 0:
            if table == "":
                table = 'id'
            if folder == "":
                folder = table
            try:
                df_csv = pd.DataFrame()
                df_csv['mail_id'] = dataframe['pg_id']
            except:
                e = sys.exc_info()
                for item in e:
                    message = str(item)
                    print_to_log(log_file, 3, message)
                message = "Failed to build DataFrame to insert records in table '" + str(table) + \
                "'. Fields identified : " + str(csv_fields) + "."
                print_to_log(log_file, 2, message)
                return [False]
            for field in csv_fields:
                df_csv[field] = dataframe[field]
            df_csv.dropna(thresh = len(csv_fields) - 1)
            for field in csv_fields:    
                df_csv = clean_data(df_csv, field)
            csv_columns = list(df_csv.columns)
            if comment == "":
                comment = "all"
            file_name = create_file_name(fichier_num, table, len(df_csv.index), \
                                         header = csv_columns, comment = comment)
            if path == "":
                path = "/media/freebox/Fichiers/ImportDB/Pandas/Test"
            csv_file = write_to_csv(df_csv, path, folder, file_name)
            insert_DB_result = write_csv_to_DB(table, csv_file, csv_columns)
            if insert_DB_result[0]:
                message = "OK : " + str(len(df_csv.index)) + " ID info inserted in '" + str(table) + \
                "' for new records. Fields : " + str(csv_fields) + "."
                print_to_log(log_file, 2, message)
                return [True]
        else:
            message = "No records added to table '" + str(table) + "'. DataFrame had no field in list : " + \
            str(id_fields_list) + "."
            print_to_log(log_file, 2, message)
            return [False]
    else:
        message = "No records added to table '" + str(table) + "'. Empty DataFrame."
        print_to_log(log_file, 2, message)
        return [False]

def insert_lead(dataframe, fichier_num, lead_fields_list = "", \
               table = "", comment = "", path = "", folder = ""):
    if lead_fields_list == "":
        lead_fields_list = ['ip', 'provenance', 'date']
    if len(dataframe.index) != 0:
        data_columns = list(dataframe.columns)
        csv_fields = []
        for item in lead_fields_list:
            if item in data_columns:
                csv_fields.append(item)
        if len(csv_fields) != 0:
            if table == "":
                table = 'lead'
            try:
                df_csv = pd.DataFrame()
                df_csv['mail_id'] = dataframe['pg_id']
                for field in csv_fields:
                    df_csv[field] = dataframe[field]
                    df_csv = clean_data(df_csv, field)
            except:
                message = "Failed to build DataFrame to insert records in table '" + str(table) + \
                "'. Fields identified : " + str(csv_fields) + "."
                print_to_log(log_file, 2, message)
                return [False]
            csv_columns = list(df_csv.columns)
            if comment == "":
                comment = "all"
            file_name = create_file_name(fichier_num, table, len(df_csv.index), \
                                         header = csv_columns, comment = comment)
            if path == "":
                path = "/media/freebox/Fichiers/ImportDB/Pandas/Test"
            if folder == "":
                folder = table
            csv_file = write_to_csv(df_csv, path, folder, file_name)
            insert_DB_result = write_csv_to_DB(table, csv_file, csv_columns)
            if insert_DB_result[0]:
                message = "OK : " + str(len(df_csv.index)) + " lead info inserted in '" + str(table) + \
                "' for new records. Fields : " + str(csv_fields) + "."
                print_to_log(log_file, 2, message)
                return [True]
        else:
            message = "No records added to table '" + str(table) + "'. DataFrame had no field in list : " + \
            str(lead_fields_list) + "."
            print_to_log(log_file, 2, message)
            return [False]
    else:
        message = "No records added to table '" + str(table) + "'. Empty DataFrame."
        print_to_log(log_file, 2, message)
        return [False]
"""

# --- update_existing_records ---
            
def prepare_update_query(table, update_dict, known_field, db_schema = ""):
    name_string = ""
    query_string = ""
    cpt_query = 0
    for field in update_dict:
        name_string = name_string + field + "_"
        cpt_query += 1
        query_string = query_string + field + " = $" + str(cpt_query) + " , "
    if db_schema:
        query_name_table = db_schema + "_" + table
        table = db_schema + "." + table
    else:
        query_name_table = table
    query_name = "update_" + name_string + "in_" + query_name_table
    query_string = query_string[:len(query_string)-3]
    prepared_query = "PREPARE " + query_name + " AS UPDATE " + table + " SET " + \
        query_string + " WHERE " + known_field + "= $" + str(cpt_query + 1) + ";"
    try:
        old_level = modify_isolation_level(0)
        direct_DB_conn.cursor().execute(prepared_query)
        revert_isolation_level(old_level)
        message = "OK : Update query prepared : " + query_name
        print_to_log(log_file, 2, message)
        need_to_prepare_update_query = False
        update_query_name = query_name
        return [True, query_name]
    except:
        message = "Failed to prepare update query. Check arguments passed to the function."
        print_to_log(log_file, 2, message)
        return [False]
    
def setup_partial_prepared_update_query_safe(query_name, update_dict):
    query_container = "("
    parameters_list = []
    for key, value in update_dict.iteritems():
        query_container = query_container + "%s, "
        parameters_list.append(value)
    query_container = query_container + "%s)"
    full_execute_safe = "EXECUTE " + query_name + " " + query_container + ";"
    return [full_execute_safe, parameters_list]

def update_record_safe(full_execute, parameters_list, known_field_value):
    execute_parameters_list = []
    for parameter in parameters_list:
        execute_parameters_list.append(parameter)
    execute_parameters_list.append(known_field_value)
    try:
        direct_DB_cursor.execute(full_execute, execute_parameters_list)
        direct_DB_conn.commit()
        return [True]
    except:
        message = "Failed to execute prepared update query with this input : " + str(known_field_value)
        print_to_log(log_file, 2, message)
        return [False]

def update_existing_records_in_base(dataframe, update_dict = "", db_schema = ""):
    if not update_dict:
        return [False]
    else:
        try:
            subset = dataframe[dataframe.exist == True]
        except:
            message = "Failed to create DataFrame subset with existing records."
            print_to_log(log_file, 2, message)
            return [False]
        existing_records = subset.index
        number_existing_records = len(existing_records)
        if number_existing_records != 0:
            prepare_result = prepare_update_query('base', update_dict, 'id', db_schema)
            if prepare_result[0]:
                partial_execute = setup_partial_prepared_update_query_safe(prepare_result[1], \
                                                                           update_dict)
                #partial_execute = setup_partial_prepared_update_query(prepare_result[1], \
                #                                                      update_dict)
                for record_index in existing_records:
                    #record_pg_id = dataframe.loc[record_index]['pg_id']
                    record_pg_id = dataframe.loc[record_index, 'pg_id']
                    update_result = update_record_safe(partial_execute[0], partial_execute[1], \
                                                       record_pg_id)
                    #update_result = update_record(partial_execute, record_pg_id)
                    if update_result[0]:
                        dataframe.loc[record_index, 'update'] = True
                message = "OK : Existing records updated in DB. Status updated in DataFrame."
                print_to_log(log_file, 2, message)
                return [True]
            else:
                return [False]
        else:
            message = "Failed to update existing records : no existing record identified."
            print_to_log(log_file, 2, message)
            return [False]
        
# --- wrapped-up_import_function ---

#def import_file_to_DB(file_name, file_path, header, insert_prov = "", update_dict = "", \
#                      status_list = "", export_path = ""):
def import_file_to_DB(DB_package, file_name, file_path, db_schema = "", export_path = "", \
                      md5_mapping = True, md5_query_limit = "", mail_cleanup = True):
    global db_package
    db_package = DB_package
    message = "--- Processing " + str(file_name) + " ---"
    print_to_log(log_file, 0, message)
    if DB_direct_connection(db_package)[0]:
        message = "OK : Direct connection established with DB."
        print_to_log(log_file, 1, message)
    else:
        message = "FATAL ERROR : Failed to connect to database."
        print_to_log(log_file, 1, message)
        return
    argument_list = extract_arguments(file_name)
    header = argument_list[1]
    status_list = argument_list[2]
    update_dict = argument_list[3]
    insert_prov = argument_list[4]
    extract_result = extract_provenance(argument_list[0], header, status_list, db_schema)
    fichier_num = extract_result[1]
    prov_fichier_id = extract_result[2]
    # old version before threading
    #load = load_text_file(file_path + "/" + file_name, header = "")
    
    #get_working_dataframe(file_path, file_name, header, DB_conn, md5_lookup = True, md5_table_limit = "", \
    #                      prepare_query = True, lookup_query = "", db_schema = "", \
    #                      filtre_in = "", filtre_out = "", status_columns = ""):
    
    working_df_result = get_working_dataframe(file_path, file_name, header, direct_DB_conn, \
                                            md5_lookup = md5_mapping, md5_table_limit = md5_query_limit, \
                                            mail_cleanup = mail_cleanup)
    if working_df_result[0]:
        message = "--- CLEARED STEP 1 : populate_and_prepare_dataframe() ---"
        print_to_log(log_file, 1, message)
        get_dict_dupli = Thread(target = threading_get_dict_duplicates_removal, \
                                args = (header, status_list, insert_prov, fichier_num))
        get_dict_dupli.start()
        dataframe_syntax = working_df_result[1]
        insert_result = insert_new_records(dataframe_syntax, fichier_num, db_schema, update_dict, \
                                           path = export_path)
        if insert_result[0]:
            message = "--- CLEARED STEP 2 : insert_new_records() ---"
            print_to_log(log_file, 1, message)
            inserted_records = insert_result[1]
            # make sure query_name is defined (not yet defined in case lookup with md5 worked OK)
            if len(working_df_result) > 2:
                query_name = working_df_result[2]
            else:
                lookup_query_result = prepare_lookup_query('base', 'id', 'mail', db_schema = db_schema)
                if lookup_query_result[0]:
                    query_name = lookup_query_result[1]
            post_insertion_result = post_insertion_update(dataframe_syntax, inserted_records, \
                                                          query_name)
            if post_insertion_result[0]:
                dataframe_syntax = post_insertion_result[1]
                message = "--- CLEARED STEP 3 : post_insertion_update() ---"
                print_to_log(log_file, 1, message)   
            else:
                message = "-x- NO IDs FOUND FOR NEW MAILS FOUND IN FILE -x-" + str(file_name)
                print_to_log(log_file, 1, message)
                message = "-x- FAILED STEP 3 : post_insertion_update() -x-"
                print_to_log(log_file, 1, message)
        else:
            message = "-x- NO NEW RECORDS INSERTED FOR FILE " + str(file_name) + " -x-"
            print_to_log(log_file, 1, message)
            message = "-x- FAILED STEP 2 : insert_new_records() -x-"
            print_to_log(log_file, 1, message)
            message = "-x- NO NEED FOR STEP 3 : post_insertion_update() -x-"
            print_to_log(log_file, 1, message)
        
        if insert_prov:
            if "ok" in fichier_num.lower():
                #dataframe_ok_provenance = prepare_dataframe_for_side_tables(dataframe_syntax, only_new = True)[1]
                #dataframe_ok_provenance = prepare_dataframe_special_ok_files(dataframe_syntax)
                ok_case = True
            else:
                ok_case = False
            ins_prov_result = insert_records_provenance(dataframe_syntax, prov_fichier_id, fichier_num, \
                                                        ok_case = ok_case, db_schema = db_schema, path = export_path)
            if ins_prov_result[0]:
                message = "--- CLEARED STEP 4 : insert_records_provenance() ---"
                print_to_log(log_file, 1, message)
            else:
                message = "-x- FAILED STEP 4 :  insert_records_provenance() -x-"
                print_to_log(log_file, 1, message)
        else:
            message = "-x- NO NEED FOR STEP 4 : insert_records_provenance() BECAUSE 'insert_prov' SET TO FALSE -x-"
            print_to_log(log_file, 1, message)
            
        ins_stat_result = insert_status(dataframe_syntax, status_list, fichier_num, \
                                        db_schema = db_schema, path = export_path)
        if ins_stat_result[0]:
            message = "--- CLEARED STEP 5 : insert_status() for status : " + str(ins_stat_result[1]) + " ---"
            print_to_log(log_file, 1, message)
        else:
            message = "-x- FAILED STEP 5 :  insert_status() -x-"
            print_to_log(log_file, 1, message)
        
        """
        prepare_df_side_table = prepare_dataframe_for_side_tables(dataframe_syntax)
        if prepare_df_side_table[0]:
            dataframe_side_table = prepare_df_side_table[1]
            if insert_prov:
                if "ok" in fichier_num.lower():
                    dataframe_ok_provenance = prepare_dataframe_for_side_tables(dataframe_syntax, only_new = True)[1]
                    #dataframe_ok_provenance = prepare_dataframe_special_ok_files(dataframe_syntax)
                    ins_prov_result = insert_records_provenance(dataframe_ok_provenance, prov_fichier_id, \
                                                                fichier_num, path = export_path)
                else:
                    ins_prov_result = insert_records_provenance(dataframe_side_table, prov_fichier_id, \
                                                                fichier_num, path = export_path)
                if ins_prov_result[0]:
                    message = "--- CLEARED STEP 4 : insert_records_provenance() ---"
                    print_to_log(log_file, 1, message)
                else:
                    message = "-x- FAILED STEP 4 :  insert_records_provenance() -x-"
                    print_to_log(log_file, 1, message)
            else:
                message = "-x- NO NEED FOR STEP 4 : insert_records_provenance() BECAUSE 'insert_prov' SET TO FALSE -x-"
                print_to_log(log_file, 1, message)
                
            ins_stat_result = insert_status(dataframe_side_table, status_list, fichier_num, path = export_path)
            if ins_stat_result[0]:
                message = "--- CLEARED STEP 5 : insert_status() for status : " + str(ins_stat_result[1]) + " ---"
                print_to_log(log_file, 1, message)
            else:
                message = "-x- FAILED STEP 5 :  insert_status() -x-"
                print_to_log(log_file, 1, message)
        else:
            message = "--- FAILED STEP 4 & 5 : insert_records_provenance() & insert_status() ---"
            print_to_log(log_file, 1, message)
        """
            
        insert_extra_fields_result = insert_extra_fields(dataframe_syntax, fichier_num, \
                                                         db_schema = db_schema, path = export_path)
        if insert_extra_fields_result[0]:
            message = "--- CLEARED STEP 6 : insert_extra_fields() ---"
            print_to_log(log_file, 1, message)
        else:
            if len(insert_extra_fields_result) > 1:
                if insert_extra_fields_result[1] == "records":
                    message = "-x- NO NEED FOR STEP 6 : insert_extra_fields(). NO RECORDS IN DATAFRAME. -x-"
                    print_to_log(log_file, 1, message)
                elif insert_extra_fields_result[1] == "fields":
                    message = "-x- NO NEED FOR STEP 6 : insert_extra_fields(). NO MATCHING FIELD. -x-"
                    print_to_log(log_file, 1, message)
                else:
                    message = "-x- FAILED STEP 6 : insert_extra_fields() -x-"
                    print_to_log(log_file, 1, message)
            else:
                message = "-x- FAILED STEP 6 : insert_extra_fields() -x-"
                print_to_log(log_file, 1, message)
        """
        dataframe_xtra = update_id_duplicate_records(dataframe_syntax)[1]
        ins_ID_res = insert_ID(dataframe_xtra, fichier_num, path = export_path)
        ins_lead_res = insert_lead(dataframe_xtra, fichier_num, path = export_path)
        if ins_ID_res[0] or ins_lead_res[0]:
            message = "--- CLEARED STEP 6 : insert_ID() and/or insert_lead() ---"
            print_to_log(log_file, 1, message)
        else:
            message = "-x- FAILED STEP 6 : insert_ID() and/or insert_lead() -x-"
            print_to_log(log_file, 1, message)
        """
            
        if update_dict:
            update_existing_result = update_existing_records_in_base(dataframe_syntax, update_dict, db_schema)
            if update_existing_result[0]:
                message = "--- CLEARED STEP 7 : update_existing_records_in_base() ---"
                print_to_log(log_file, 1, message)
            else:
                message = "-x- FAILED STEP 7 : update_existing_records_in_base() -x-"
                print_to_log(log_file, 1, message)
        else:
            message = "-x- NO NEED FOR STEP 7 : update_existing_records_in_base(). -x-"
            print_to_log(log_file, 1, message)
            
    else:
        message = "-x- STOP IMPORT ATTEMPT FOR FILE " + str(file_name) + " -x-"
        print_to_log(log_file, 1, message)
        message = "-x- FAILED STEP 1 : populate_and_prepare_dataframe() -x-"
        print_to_log(log_file, 1, message)
        
    close_DB_connection()
    message = "--- OK : Successfully processed : " + str(file_name) + " ---"
    print_to_log(log_file, 0, message)
    print str("-" * 200)

# --- check_mot_cle ---

def check_mot_cle(mail, df_mot):
    mail = str(mail).lower().strip()
    domain = mail[mail.rfind("@") + 1:]
    mail_dict = {}
    mail_dict['mail'] = mail
    mail_dict['domain'] = domain
    mail_dict['TDN'] = domain[domain.rfind("."):]
    index = df_mot.index
    for cpt in range(len(index)):
        cpt_mot = index[cpt]
        mot = df_mot.at[cpt_mot, 'mot']
        secteur = df_mot.at[cpt_mot, 'secteur']
        if secteur in ['mail', 'domain']:
            if mail_dict[secteur].find(mot) > -1:
                #res = [True, df_mot.at[cpt_mot, 'exclusion']]
                res = [True, cpt_mot]
                #print mot, mail_dict[secteur], res
                return res
        elif secteur == "TDN":
            if mail_dict[secteur] == mot:
                #res = [True, df_mot.at[cpt_mot, 'exclusion']]
                res = [True, cpt_mot]
                #print mot, mail_dict[secteur], res
                return res
    res = [False, ""]
    #print res
    return res
    
def check_mot_cle_columnwide(dataframe):
    milestones = {1 : 1000, 2 : 2000, 3 : 5000, 4 : 10000, 5 : 20000, 6 : 50000, \
                  7 : 100000, 8 : 200000, 9 : 500000, 10 : 1000000, 11 : 2000000, 12: 5000000}
    cpt_milestones = 1
    db_name = "test_base" #"postgres"
    db_user = "postgres"
    db_host = "192.168.0.52"
    db_pass = "postgres"
    db_package = [db_name, db_user, db_host, db_pass]
    db_connect = DB_direct_connection(db_package)
    if db_connect[0]:
        conn = db_connect[1]
        #cur = db_connect[2]
    else:
        return
    df_mot = sql.read_sql("SELECT * FROM %s" % ('mot_list'), conn, index_col='mot_list_id', coerce_float=False)
    print df_mot.head(5)
    conn.close()
    dataframe['exclusion_mot_cle'] = ""
    dataframe['raison_exclusion'] = ""
    cpt_lookup = 0
    clock_4 = time.clock()
    for cpt in range(len(dataframe.index)):
        cpt_mail = dataframe.index[cpt]
        mail = dataframe.at[cpt_mail, 'mail']
        res_check = check_mot_cle(mail, df_mot)
        dataframe.at[cpt_mail, 'exclusion_mot_cle'] = res_check[0]
        dataframe.at[cpt_mail, 'raison_exclusion'] = res_check[1]
        cpt_lookup += 1
        if cpt_lookup == milestones[cpt_milestones]:
            message = "OK. First " + str(milestones[cpt_milestones]) + " mails looked up."
            print_to_log(log_file, 4, message)
            cpt_milestones += 1
    return dataframe

def check_mot_cle_sql(fichier_num): #42
    milestones = {1 : 1000, 2 : 2000, 3 : 5000, 4 : 10000, 5 : 20000, 6 : 50000, \
                  7 : 100000, 8 : 200000, 9 : 500000, 10 : 1000000, 11 : 2000000, 12: 5000000}
    cpt_milestones = 1
    #from pandas.io import sql
    db_name = "test_base" #"postgres"
    db_user = "postgres"
    db_host = "192.168.0.52"
    db_pass = "postgres"
    db_package = [db_name, db_user, db_host, db_pass]
    db_connect = DB_direct_connection(db_package)
    if db_connect[0]:
        conn = db_connect[1]
        #cur = db_connect[2]
    else:
        return
    df_mot = sql.read_sql("SELECT * FROM %s" % ('mot_list'), conn, index_col='mot_list_id', coerce_float=False)
    show_df(df_mot)
    select_part = "SELECT base.id, base.mail FROM base "
    join_part = "INNER JOIN fichier_match ON fichier_match.id = base.id "
    where_part = "WHERE fichier_match.fichier_id IN " + \
            "(SELECT fichier_list.fichier_id FROM fichier_list WHERE fichier_num LIKE '%s')" \
            % (str(fichier_num))
    query = select_part + join_part + where_part + ";"
    
    df_mail = sql.read_sql(query, conn, index_col='id', coerce_float=False)
    show_df(df_mail)
    conn.close()
    data = []
    for cpt in range(len(df_mail.index)):
        if cpt == milestones[cpt_milestones]:
            message = "OK. First " + str(milestones[cpt_milestones]) + " mails looked up."
            print_to_log(log_file, 4, message)
            cpt_milestones += 1
        mail_id = df_mail.index[cpt]
        mail = df_mail.at[mail_id, 'mail']
        check = check_mot_cle(mail, df_mot)
        if check[0]:
            record = [mail_id, check[1]]
            data.append(record)
    df_result = pd.DataFrame(data, columns = ['id', 'mot_list_id'], index = None)
    show_df(df_result)
    table = "mot_match"
    export_path = "/media/freebox/Fichiers/ImportDB/Pandas/Test"
    file_name = create_file_name(fichier_num, table, len(df_result.index), \
                                 header = list(df_result.columns), comment = "")
    write_to_csv(df_result, export_path, table, file_name)
        
# --- appetence files preparation for import ---

def extract_appetence(string, appetence, duo):
    if string != "NaN":
        try:
            value = int(string)
            if value > 0:
                #return appetence
                if duo == 0:
                    score = value
                elif duo == 1:
                    score = 3 * value
                return [appetence, score]
            return "No"
        except:
            return "No"
    else:
        return "No"
    
def write_prepared_appetence_csv(file_path, file_name, file_name_result, comment):
    df = populate_dataframe(file_path, file_name, header = True)[1]
    #df = pd.read_csv(file_path + "/" + file_name, encoding = 'latin-1', sep = ";", dtype = object)
    header = list(df.columns)
    print header
    
    app_list_db = []
    for cpt in range(9, len(header), 2):
        split_char = ["_O", "_C"]
        col = header[cpt]
        appetence = col.split(split_char[0])[0]
        app_list_db.append(appetence)
    app_table = pd.DataFrame()
    app_table['id'] = list(range(1, len(app_list_db) + 1))
    app_table['interet'] = app_list_db
    print app_table
    #app_table.to_csv(file_path + "/appetence_list_table.csv", index = None)
        
    for cpt in range(9, len(header), 2):
        for duo in range(2):
            col = header[cpt + duo]
            appetence = col.split(split_char[duo])[0]
            df[col] = df[col].apply(lambda x: extract_appetence(x, appetence, duo))
    print df
    
    app_col = []
    milestones = {1 : 1000, 2 : 2000, 3 : 5000, 4 : 10000, 5 : 20000, 6 : 50000, \
                  7 : 100000, 8 : 200000, 9 : 500000, 10 : 1000000, 11 : 2000000, 12: 5000000}
    cpt_milestones = 1
    for cpt in range(len(df.index)):
        if cpt == milestones[cpt_milestones]:
            message = "OK. First " + str(milestones[cpt_milestones]) + " mails looked up."
            print_to_log(log_file, 4, message)
            cpt_milestones += 1
        interet_list = []
        score_list = []
        for cpt_col in range(9, len(header)):
            cell = df.at[df.index[cpt], header[cpt_col]]
            if cell != "No":
                interet_list.append(cell[0])
                score_list.append(cell[1])
        if len(interet_list) > 0:
            score_df = pd.DataFrame()
            score_df['interet'] = interet_list
            score_df['score'] = score_list
            score_df = score_df.groupby(by = 'interet').sum()
            #show_df(score_df)
            interet_dict = {}
            for cpt_score in range(len(score_df.index)):
                interet = score_df.index[cpt_score]
                ref_app = np.asscalar(app_table['id'][app_table['interet'] == interet])
                interet_dict[ref_app] = score_df.at[interet, 'score']
            app_col.append(interet_dict)
        else:
            app_col.append("NaN")
    
    df['interet'] = app_col
    df_short = df[['email', 'civilite','nom', 'prenom', 'zip', 'tel1', 'tel2', 'interet']]
    if not file_name_result:
        file_name_w = "127_TOPER_[mail]_[appetence_dict]_" + str(comment) + ".csv"
    else:
        file_name_w = file_name_result + str(comment) + ".csv"
    df_short.to_csv(file_path + "/" + file_name_w, index = False)
    
""" Script to prepare appetence files
file_path = "/media/freebox/Fichiers/TOPER/Done"
from os import walk
import_files = []
for (dirpath, dirnames, filenames) in walk(file_path):
    import_files.extend(filenames)
    break
cpt_file = 0
for file_name in sorted(import_files):
    cpt_file = cpt_file + 1
    write_prepared_appetence_csv(file_path, file_name, comment = cpt_file)
"""

""" ARCHIVE : deprecetad populate_dataframe() """

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


""" ARCHIVE : psycopg prepared queries UNSAFE way
        
def lookup_record(query_name, mail):
    mail_lookup_query = "EXECUTE " + query_name + "(E'" + mail + "');"
    try:
        direct_DB_cursor.execute(mail_lookup_query)
        records = direct_DB_cursor.fetchall()
        if records == []:
            return [False]
        else:
            mail_id = int(str(records[0]).strip('()').strip(','))
            return [True, mail_id]
    except:
        return [False, mail]
        print "Failed to execute " + query_name + " with this input : " + mail

def setup_partial_prepared_update_query(query_name, update_dict):
    query_parameters = "("
    for key, value in update_dict.iteritems():
        if isinstance(value, basestring):
            value = "E'" + value + "'"
        query_parameters = query_parameters + str(value) + ", "
    #query_parameters = query_parameters[:len(query_parameters) - 3] + ")" 
    partial_execute_prepared_update_query = \
        "EXECUTE " + query_name + " " + query_parameters
    return partial_execute_prepared_update_query

def update_record(partial_execute, known_field_value):
    if isinstance(known_field_value, basestring):
            known_field_value = "E'" + known_field_value + "'"
    full_execute_prepared_update_query = \
        partial_execute + str(known_field_value) + ");"
        
    try:
        print full_execute_prepared_update_query
        direct_DB_cursor.execute(full_execute_prepared_update_query)
        direct_DB_conn.commit()
        return [True]
    except:
        print "Failed to execute prepared update query with this input : " + str(known_field_value)
        return [False]
"""

""" ARCHIVE : import functions using DB Mapping

def build_attributes(raw_line):
    pass

def build_dataframe_insert_provenance():
    df_syntax = df.where([df.syntax == True], inplace=True)
    df_prov = pd.DataFrame()
    df_prov['mail_id'] = df_syntax['pg_id']
    df_prov['fichier'] = prov_fichier_id
    
    pass
    
def update_fields_in_Base(mail_object, base_attr):
    from DB_mapping import * #to be removed when in prod
    if "b2c" in base_attr:
        mail_object.b2c = True
    if "b2b" in base_attr:
        mail_object.b2b = True
    if "ouvreur" in base_attr:
        mail_object.ouvr = True
    if "npai" in base_attr:
        mail_object.npai = True
    if "plainte" in base_attr:
        mail_object.ok_plainte = False
    if "pcp" in base_attr:
        mail_object.pcp = True
        
def update_fields_in_ID(mail_object, ID_attr):
    from DB_mapping import * #to be removed when in prod
    ID_object = ID(prenom = ID_attr[0], nom = ID_attr[1], civilite = ID_attr[2], \
                   birth = ID_attr[3], cp = ID_attr[4], ville = ID_attr[5])    
    mail_object.ixm = ID_object
    
def update_fields_in_Lead(mail_object, lead_attr):
    from DB_mapping import * #to be removed when in prod
    lead_object = Lead(ip = lead_attr[0], provenance = lead_attr[1], date = lead_attr[2])   
    mail_object.lxm = lead_object

def update_fields(mail_object, base_attr = "", ID_attr = "", lead_attr = ""):
    insert_provenance_in_DB(mail_object)
    if base_attr != "":
        update_fields_in_Base(mail_object, base_attr)
    if ID_attr != "":
        update_fields_in_ID(mail_object, ID_attr)
    if lead_attr != "":
        update_fields_in_ID(mail_object, lead_attr)

def update_existing_mail_in_DB(existing_id, base_attr = "", ID_attr = "", lead_attr = ""):
    from DB_mapping import * #to be removed when in prod
    mail_object = session.query(Base).filter_by(id = existing_id).first()
    update_fields(mail_object, base_attr = "", ID_attr = "", lead_attr = "")
    try:
        session.commit()
        return True
    except:
        print "Failed to update record in DB for mail : " + mail_object.mail      
        return False

def update_existing_mail_arraywide():
    from DB_mapping import * #to be removed when in prod
    for line in raw_data:
        if line [1] and line[2]:
            update_result = update_existing_mail_in_DB(line[3])
            if update_result:
                line[2] = "Updated"
    print "OK : Existing mails in raw_data inserted in DB."

def insert_new_mail_in_DB(new_mail, base_attr = "", ID_attr = "", lead_attr = ""):
    new_mail_domain = new_mail[new_mail.find("@") + 1:]
    from DB_mapping import * #to be removed when in prod
    mail_object = Mail(mail = new_mail, domain = new_mail_domain)
    update_fields(mail_object, base_attr = "", ID_attr = "", lead_attr = "")
    try:
        session.commit()
        return [True, mail_object.id]
    except:
        print "Failed to insert new record in DB for mail : " + new_mail      
        return [False, ""]

def insert_new_mail_arraywide():
    from DB_mapping import * #to be removed when in prod
    for line in raw_data:
        if line [1] and (not line[2]):
            insert_result = insert_new_mail_in_DB(line[0])
            if insert_result[0]:
                line[2] = "Insert"
                line[3] = insert_result[1]
    print "OK : New mails in raw_data inserted in DB."
"""

""" ARCHIVE : cleanup_mail_syntax

def cleanup_mail_syntax(mail, filtre_in, filtre_out):
    initial_syntax = mail
    if mail == "":
        return [False, ""]
    mail = mail.strip()
    mail = mail.lower()
    for item_in in filtre_in:
        if item_in not in mail:
            return [False, ""]
    for item_out in filtre_out:
        if item_out in mail:
            return [False, ""]
    if len(mail) > 100:
        #mail = mail[:100]
        return [False, ""]         
    mail_domain = mail[mail.find("@") + 1:]
    if len(mail_domain) > 60:
        #mail_domain = mail_domain[:60]
        #mail = mail[:mail.find("@") + 1] + mail_domain
        return [False, ""]
    if "'" in mail:
        mail = mail.replace("'","\\'")
    if mail == initial_syntax:
        return [True, "", mail_domain]
    else:
        return [True, mail, mail_domain]

def check_for_bad_text(string):
    unicode_string = unicode(string)
    ascii_list = [unichr(195), unichr(194), unichr(191), unichr(171)]
    for ascii_char in ascii_list:
        if ascii_char in unicode_string:
            print unicode_string
            return fix_bad_unicode(unicode_string)
    return string

def cleanup_mail_syntax_columnwide(dataframe, filtre_in = "", filtre_out = ""):
    if filtre_in == "":
        filtre_in = ['@']
    if filtre_out == "":
        filtre_out = [',',';']
    dataframe['domain'] = ""
    for cpt in range(len(dataframe.index)):
        email = dataframe.mail.iloc[cpt]
        unicode_clean_email = check_for_bad_text(email)
        if email != unicode_clean_email:
            print email, unicode_clean_email
        cleanup_result = cleanup_mail_syntax(unicode_clean_email, filtre_in, filtre_out)
        if cleanup_result[0]:
            if cleanup_result[1] != "":
                dataframe.mail.iloc[cpt] = cleanup_result[1]
            dataframe.domain.iloc[cpt] = cleanup_result[2]
        else:
            dataframe.syntax.iloc[cpt] = False
    df_syntax = dataframe[dataframe.syntax == True]
    message = "OK : Mails cleaned up in DataFrame. Syntax field updated. Domains added"
    print_to_log(log_file, 2, message)
    return df_syntax
"""