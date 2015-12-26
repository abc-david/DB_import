''' Created on 12 juin 2015 '''
# !/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'david'

import pysftp, os
import hashlib
import time
import psycopg2
import psycopg2.extensions
import psycopg2.pool
import pandas as pd
import unicodedata
from unicode_fix import *
from threading import Thread

from contextlib import contextmanager

db_name = "prod"  #"postgres"
db_user = "postgres"
db_host = "localhost"
db_pass = "penny9690"
global db_package
db_package = [db_name, db_user, db_host, db_pass]

ftp_url = "mindsftp1.odiso.net"
ftp_token_showroom = {"host": ftp_url, "username": "showroomstyliste", "password": "A9Lxd4m2"}
ftp_token_youpick = {"host": ftp_url, "username": "youpick", "password": "23smGrX6"}
p_sync = {'ftp' : {'youpick' : ftp_token_youpick, \
                   'showroom' : ftp_token_showroom}, \
          'ftp_folder' : {'youpick' : 'youpick', \
                          'showroom' : 'showroomstyliste'}, \
          'desabo' : {'log_file' : "LOG_desabos_NPAI_plaintes", \
                      'error_file' : "ERR_desabos", \
                      'ftp_file' : "desabos_NPAI_plaintes_"}, \
          'ouvreur' : {'log_file' : "LOG_ouvreurs", \
                       'error_file' : "ERR_ouvreurs", \
                       'ftp_file' : "ouvreurs_"}, \
          'local_dir' : "/home/david/fichiers/sync_mindbaz"}

global campagne_ref
campagne_ref = {}

def strip_accents(s):
    s = s.decode('utf8', errors = 'ignore')
    s = ''.join(c for c in unicodedata.normalize('NFD', s)
                  if unicodedata.category(c) != 'Mn')
    s.encode('utf8')
    s = s.replace("'", "\\'")
    return s

def show_df(df):
    print df.head(5)
    print str(len(df.index)) + " lines."

def get_date_list(yr1, m1, d1, yr2, m2, d2):
    from datetime import date, timedelta as td
    d1 = date(yr1, m1, d1)
    d2 = date(yr2, m2, d2)
    delta = d2 - d1
    date_list = []
    for i in range(delta.days + 1):
        new_date = d1 + td(days=i)
        date_list.append(new_date)
    return date_list

def ftp_file_name(p, action, date):
    return p[action]['ftp_file'] + date.strftime('%Y%m%d') + ".csv"

def get_data_from_sftp(p, base, file):
    with pysftp.Connection(**p['ftp'][base]) as sftp:
        with sftp.cd(p['ftp_folder'][base]):
            if sftp.exists(file):
                local_file = p['local_dir'] + "/" + file
                sftp.get(file, local_file)
                df = pd.read_csv(local_file, sep = ";")
                os.remove(local_file)
                return [True, df]
            else:
                return [False]

def rename_file_on_sftp(p, base, file, prefix = "OK"):
    with pysftp.Connection(**p['ftp'][base]) as sftp:
        with sftp.cd(p['ftp_folder'][base]):
            if sftp.exists(file):
                sftp.rename(file, prefix + "_" + file)

def remove_file_on_sftp(p, base, file):
    with pysftp.Connection(**p['ftp'][base]) as sftp:
        with sftp.cd(p['ftp_folder'][base]):
            if sftp.exists(file):
                sftp.remove(file)

def initiate_threaded_connection_pool(db_package):
    connect_token = "dbname='" + db_package[0] + "' user='" + db_package[1] + \
                    "' host='" + db_package[2] + "' password='" + db_package[3] + "'"
    try:
        global conn_pool
        conn_pool = psycopg2.pool.ThreadedConnectionPool(1, 100, connect_token)
        message = "OK : Threaded connection pool established with DB."
        #print message
    except:
        e = sys.exc_info()
        for item in e:
            message = str(item)
            print message

@contextmanager
def getconnection():
    con = conn_pool.getconn()
    try:
        yield con
    finally:
        conn_pool.putconn(con)

@contextmanager
def getcursor():
    con = conn_pool.getconn()
    try:
        yield con.cursor()
    finally:
        conn_pool.putconn(con)

def get_optin_id(base):
    with getcursor() as cursor:
        cursor.execute("SELECT id FROM optin_list WHERE abreviation = %s", (str(base[:3]), ))
        records = cursor.fetchone()
        if records:
            return records[0]

def prepare_queries(cursor, action):
    get_mail_id = "PREPARE get_mail_id AS " + \
                  "SELECT m.mail_id FROM md5 AS m WHERE m.md5 = $1"
    cursor.execute(get_mail_id)
    if action == "desabo":
        insert_desabo = "PREPARE insert_desabo AS " + \
                        "INSERT INTO optin_desabo " + \
                        "(mail_id, optin_id, date, comment) VALUES ($1,$2,$3,$4)"
        cursor.execute(insert_desabo)
    elif action == "ouvreur":
        get_campaign_id = "PREPARE get_campaign_id AS " + \
                          "SELECT camp.id FROM mindbaz_campagne_list AS camp WHERE " + \
                          "camp.optin_id = $1 AND camp.mindbaz_id = $2"
        cursor.execute(get_campaign_id)
        insert_campagne = "PREPARE insert_campagne AS " + \
                          "INSERT INTO mindbaz_campagne_list " + \
                          "(optin_id, mindbaz_id, nom) VALUES ($1,$2,$3)"
        cursor.execute(insert_campagne)
        insert_ouvreur = "PREPARE insert_ouvreur AS " + \
                         "INSERT INTO mindbaz_ouvreurs " + \
                         "(mail_id, campagne_id, date, ip, os, navigateur) VALUES ($1,$2,$3,$4,$5,$6)"
        cursor.execute(insert_ouvreur)

def hash_mail_to_md5(string):
    string = string.lower().encode()
    hash_object = hashlib.md5(string)
    return hash_object.hexdigest()

def get_mail_id(cursor, mail):
    cursor.execute("EXECUTE get_mail_id (%s)", (str(hash_mail_to_md5(mail)),))
    records = cursor.fetchone()
    if records:
        mail_id = records[0]
        return mail_id
    else:
        return False

def get_mail_id_no_md5(cursor, mail):
    cursor.execute("EXECUTE get_mail_id_no_md5 (%s)", (str(mail),))
    records = cursor.fetchone()
    if records:
        mail_id = records[0]
        return mail_id
    else:
        return False

def prepare_queries_missing_mail(cursor):
    get_mail_id_no_md5 = "PREPARE get_mail_id_no_md5 AS " + \
                         "SELECT b.id FROM base AS b WHERE b.mail = $1"
    cursor.execute(get_mail_id_no_md5)
    insert_mail = "PREPARE insert_mail AS " + \
                  "INSERT INTO base (mail, domain) VALUES ($1, $2)"
    cursor.execute(insert_mail)
    insert_md5 = "PREPARE insert_md5 AS " + \
                 "INSERT INTO md5 (mail_id, md5) VALUES ($1, $2)"
    cursor.execute(insert_md5)
    insert_prov = "PREPARE insert_prov AS " + \
                  "INSERT INTO fichier_match (mail_id, fichier_id) VALUES ($1, $2)"
    cursor.execute(insert_prov)
    insert_optin = "PREPARE insert_optin AS " + \
                   "INSERT INTO optin_match (mail_id, optin_id, date) VALUES ($1, $2, $3)"
    cursor.execute(insert_optin)
    insert_optin = "PREPARE insert_routeur AS " + \
                   "INSERT INTO routeur_match (mail_id, routeur_id, date) VALUES ($1, $2, $3)"
    cursor.execute(insert_optin)

def insert_missing_mail(conn, cursor, mail, optin_id, date, routeur_id = 2, fichier_id = 121):
    split_result = str(mail).split('@')
    domain = split_result[1]
    md5 = hash_mail_to_md5(mail)
    date_iso = date[0:10]
    print mail, domain, md5, date_iso, optin_id, fichier_id
    try:
        cursor.execute("EXECUTE insert_mail (%s, %s)", (str(mail), str(domain)))
        conn.commit()
        mail_id = get_mail_id_no_md5(cursor, mail)
        print mail_id
        cursor.execute("EXECUTE insert_md5 (%s, %s)", (str(mail_id), str(md5)))
        cursor.execute("EXECUTE insert_prov (%s, %s)", (str(mail_id), str(fichier_id)))
        cursor.execute("EXECUTE insert_optin (%s, %s, %s)", (str(mail_id), str(optin_id), str(date_iso)))
        cursor.execute("EXECUTE insert_routeur (%s, %s, %s)", (str(mail_id), str(routeur_id), str(date_iso)))
        conn.commit()
        return [True, mail_id]
    except:
        return [False]

def record_desabo(conn, cursor, mail, optin_id, date, comment):
    mail_id = get_mail_id(cursor, mail)
    if mail_id:
        cursor.execute("EXECUTE insert_desabo (%s, %s, %s, %s)", \
                       (str(mail_id), str(optin_id), str(date), str(comment)))
        return "OK"
    else:
        res_fix = insert_missing_mail(conn, cursor, mail, optin_id, date)
        if res_fix[0]:
            mail_id = res_fix[1]
            cursor.execute("EXECUTE insert_desabo (%s, %s, %s, %s)", \
                       (str(mail_id), str(optin_id), str(date), str(comment)))
            return "OK"
        else:
            return [mail, optin_id, date, comment]

def dict_check_campagne_id(mindbaz_id):
    if mindbaz_id in campagne_ref:
        return campagne_ref[mindbaz_id]
    else:
        return False

def record_campagne(conn, cursor, optin_id, mindbaz_id, nom):
    try:
        nom = strip_accents(nom)
        cursor.execute("EXECUTE insert_campagne (%s, %s, %s)", (str(optin_id), str(mindbaz_id), str(nom)))
        conn.commit()
    except:
        nom = strip_accents(nom)
        print nom
        cursor.execute("EXECUTE insert_campagne (%s, %s, %s)", (str(optin_id), str(mindbaz_id), str(nom)))
        conn.commit()
    cursor.execute("EXECUTE get_campaign_id (%s, %s)", (str(optin_id), str(mindbaz_id)))
    records = cursor.fetchone()
    campagne_id = records[0]
    campagne_ref[optin_id] = campagne_id
    return campagne_id

def get_campagne_id(conn, cursor, optin_id, mindbaz_id, nom):
    cursor.execute("EXECUTE get_campaign_id (%s, %s)", (str(optin_id), str(mindbaz_id)))
    records = cursor.fetchone()
    if records:
        campagne_id = records[0]
        campagne_ref[mindbaz_id] = campagne_id
        return campagne_id
    else:
        return record_campagne(conn, cursor, optin_id, mindbaz_id, nom)

def record_ouvreur(conn, cursor, mail, optin_id, mindbaz_id, nom, date, ip, os, navigateur):
    campagne_id = dict_check_campagne_id(mindbaz_id)
    if not campagne_id:
        campagne_id = get_campagne_id(conn, cursor, optin_id, mindbaz_id, nom)
    mail_id = get_mail_id(cursor, mail)
    if mail_id:
        cursor.execute("EXECUTE insert_ouvreur (%s, %s, %s, %s, %s, %s)", \
                       (str(mail_id), str(campagne_id), str(date), str(ip), str(os), str(navigateur)))
        return "OK"
    else:
        res_fix = insert_missing_mail(conn, cursor, mail, optin_id, date)
        if res_fix[0]:
            mail_id = res_fix[1]
            cursor.execute("EXECUTE insert_ouvreur (%s, %s, %s, %s, %s, %s)", \
                (str(mail_id), str(campagne_id), str(date), str(ip), str(os), str(navigateur)))
            return "OK"
        else:
            return [mail, optin_id, mindbaz_id, date, ip, os, navigateur]

def record_df(df, optin_id, action):
    cpt_insert = 0
    err_list = []
    milestones = {1 : 1000, 2 : 2000, 3 : 5000, 4 : 10000, 5 : 20000, 6 : 50000, \
                  7 : 100000, 8 : 200000, 9 : 500000, 10 : 1000000, 11 : 2000000, 12: 5000000}
    cpt_milestones = 1
    cpt = 0
    with getconnection() as conn:
        cursor = conn.cursor()
        prepare_queries(cursor, action)
        prepare_queries_missing_mail(cursor)
        if action == "desabo":
            for n_line in range(len(df.index)):
                cpt += 1
                if cpt == milestones[cpt_milestones]:
                    message = "OK. First %s data done." % str(milestones[cpt_milestones])
                    print message
                    cpt_milestones += 1
                mail = df.iat[n_line, 0]
                date = df.iat[n_line, 1]
                comment = df.iat[n_line, 2]
                insert_item = record_desabo(conn, cursor, mail, optin_id, date, comment)
                if insert_item == "OK":
                    cpt_insert += 1
                else:
                    err_list.append(insert_item)
            conn.commit()
        elif action == "ouvreur":
            for n_line in range(len(df.index)):
                cpt += 1
                if cpt == milestones[cpt_milestones]:
                    message = "OK. First %s data done." % str(milestones[cpt_milestones])
                    print message
                    cpt_milestones += 1
                mail = df.iat[n_line, 0]
                date = df.iat[n_line, 1]
                mindbaz_id = df.iat[n_line, 2]
                nom = df.iat[n_line, 3]
                ip = df.iat[n_line, 4]
                os = df.iat[n_line, 5]
                navigateur = df.iat[n_line, 6]
                insert_item = record_ouvreur(conn, cursor, mail, optin_id, mindbaz_id, nom, date, ip, os, navigateur)
                if insert_item == "OK":
                    cpt_insert += 1
                else:
                    err_list.append(insert_item)
            conn.commit()
    return [cpt_insert, err_list]

def log_sync(p, action, base, file, len_df, cpt_insert, err_list):
    import time
    message = time.strftime("%d/%m/%Y") + "  --  "
    with open(p['local_dir'] + "/" + p[action]['log_file'], 'a') as f:
        if len_df == cpt_insert:
            message = message + ("OK  --  %s mails traites sur l'optin '%s'" % (str(cpt_insert), str(base))) + \
                                    "  --  " + file
            f.write(message + '\n')
        else:
            message = message + ("PB  --  %s mails sur %s traites sur l'optin '%s'" % (str(cpt_insert), str(len_df), \
                                    str(base))) + "  --  " + file
            f.write(message + '\n')
            if err_list:
                with open(p['local_dir'] + "/" + p[action]['error_file'], 'a') as err:
                    for list in err_list:
                        err.write(";".join(str(x) for x in list))

def sync_mindbaz_file(db_package, p_sync, base, file, action):
    initiate_threaded_connection_pool(db_package)
    sftp_res = get_data_from_sftp(p_sync, base, file)
    if sftp_res[0]:
        df = sftp_res[1]
        show_df(df)
        track = record_df(df, get_optin_id(base), action)
        log_sync(p_sync, action, base, file, len(df.index), track[0], track[1])
        rename_file_on_sftp(p_sync, base, file)

def get_openers_by_campagne(campagne_id):
    with getconnection() as conn:
        sql = "SELECT DISTINCT mail_id FROM test_mindbaz_ouvreurs WHERE campagne_id = '%s';" % str(campagne_id)
        df = pd.read_sql(sql, conn)
        show_df(df)


file = "desabos_NPAI_plaintes_20150619.csv"
#file = "desabos_NPAI_plaintes_FULL.csv"
file = "ouvreurs_20150619.csv"
#file = "ouvreurs_FULL.csv"
action = 'ouvreur'
base = "youpick"

for date in get_date_list(2015,6,19,2015,7,25):
    file = ftp_file_name(p_sync, action, date)
    print file
    sync_mindbaz_file(db_package, p_sync, base, file, action)

#get_openers_by_campagne(10)

from datetime import datetime
now = datetime.date(datetime.now())
#print now


#print (time.strftime("%Y%m%d"))

"""
file_name = "desabos_NPAI_plaintes_20150609.csv"
file_path = "/home/david/fichiers/sync_mindbaz/desabo_from_ftp"

df = pd.read_csv(file_path+"/"+file_name, sep=";")

show_df(df)
for n_line in range(len(df.index)):
   email = df.iat[n_line, 0]
   date = df.iat[n_line, 1]
   comment = df.iat[n_line, 2]
"""