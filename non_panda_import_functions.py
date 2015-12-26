'''
Created on 15 feb. 2014

@author: david
'''
#!/usr/bin/env python
# -*- coding: utf-8 -*-

#from DB_mapping import *

def populate_raw_data_from_file(file_path, file_name):
    global raw_data
    raw_data = []
    import codecs
    try:
        reader = codecs.open(file_path + file_name, 'r', encoding='latin-1')
    except:
        print "Failed to open " + file_path + file_name
        return False
    for line in reader:
        raw_line = []
        row = line.split(',')
        mail = str(row[0].strip())
        raw_line.append(mail)
        for cpt in range(1, 3):
            raw_line.append("")
        for cpt in range(1, len(row)):
            raw_line.append(str[cpt].strip())
        raw_data.append(raw_line)
    reader.close()
    print "OK : Array raw_data populated from file : " + file_name
    return True

def cleanup_mail_syntax_arraywide(filtre_in, filtre_out):
    for line in raw_data:
        mail = line[0]
        cleanup_result = cleanup_mail_syntax(mail, filtre_in, filtre_out)
        if cleanup_result[0]:
            line[0] = cleanup_result[1]
            line[1] = True
        else:
            line[1] = False
    print "OK : Mails cleaned up in raw_data."
    
def check_if_duplicates():
    mail_list = []
    for line in raw_data:
        mail_list.append(line[0])
    old_size = len(mail_list)
    new_size = len(set(mail_list))
    if old_size == new_size:
        print "OK : No duplicates in raw_data."
        return True
    else:
        print "PROBLEM : There is " + str(old_size - new_size) + " duplicates."
        return False
    
def lookup_mail_arraywide(query_name):
    for line in raw_data:
        if line[1]:
            mail = line[0]
            lookup_result = lookup_mail(query_name, mail)
            line[2] = lookup_result[0]
            line[3] = lookup_result[1]
    print "OK : Mails in raw_data associated with existing records in table 'base'."

def insert_provenance_in_DB(mail_object):
    from DB_mapping import * #to be removed when in prod
    prov_object = Fich_x_mail(fichier_id = prov_fichier_id)
    mail_object.fxm = prov_object