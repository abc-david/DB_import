'''
Created on 18 nov. 2013

@author: administrateur
'''
import psycopg2, codecs, sys
from DB_mapping import *
import random, math, datetime
from sqlalchemy import *
from sqlalchemy.orm import *
from sqlalchemy.ext.declarative import declarative_base
dburl = 'postgresql+psycopg2://postgres:postgres@192.168.0.52/postgres'
dossier_pgSQL = "/media/Freebox/pgSQL/"

dburl = 'postgresql+psycopg2://postgres:postgres@192.168.0.52/postgres'
u2_db = create_engine(dburl) #, echo=True
u2_metadata = MetaData(schema="public")
conn = u2_db.connect()

conn = psycopg2.connect("dbname='postgres' user='postgres' host='192.168.0.52' password='postgres'")
cur = conn.cursor()

dossier_source = "/media/Freebox/Fichiers/Plaintes/"
fichier_prov_id = session.query(Fichier.id).filter(Fichier.num == "PLAI").first()[0]
log_file = codecs.open(dossier_pgSQL + "log_update_Plainte.txt", 'a', encoding='latin-1')
old_isolation_level = conn.isolation_level
conn.set_isolation_level(0)
prepared_select_query = "PREPARE mail_existe AS SELECT id FROM base WHERE mail = $1;"
cur.execute(prepared_select_query)
conn.set_isolation_level(old_isolation_level)

fichier_dep = 1
nb_fichier = 1
dep_batch = 1
stop_batch = 1
batch = 5
filtre_syntax_mail = "@"

for fichier_cpt in range (fichier_dep, fichier_dep + nb_fichier):
#    fichier_path = dossier_source + str(fichier_cpt) + "-ok.csv"
    fichier_path = dossier_source + "Plaintes_mimi_novembre.txt"
    log = []
    
    reader = codecs.open(fichier_path, 'r', encoding='latin-1')
    raw_mail_list = []
    cpt_raw_mail = 0
    for line in reader:
        row = line.split(',')
        raw_mail = str(row[0].strip())
        cpt_raw_mail = cpt_raw_mail + 1
        raw_mail_list.append(raw_mail)
    reader.close()
    
    mail_list = list(set(raw_mail_list))
    cpt_mail = len(mail_list)
    print len(mail_list)
    
    if cpt_mail < cpt_raw_mail:
        new_log = "Nombre de mails en doublon:", (cpt_raw_mail-cpt_mail)
        log.append(new_log)
        print new_log
        
    nb_batch = len(mail_list) // batch
    for cpt_batch in range (dep_batch, nb_batch + 2):
         
        if cpt_batch == nb_batch + 1:
            max_mail = len(mail_list)
        else:
            max_mail = (cpt_batch * batch)
        new_log = "Batch num. " + str(cpt_batch) + " : de " + str((cpt_batch - 1) * batch) + " a " + str(max_mail)
        log.append(new_log)
        print new_log
        
        cpt_filtre_mail = 0
        cpt_insert_db = 0
        max_id = session.query(func.max(Mail.id)).first()[0]
        print "max_id:", max_id
        cpt_exclu_mail = 0
        cpt_pb_db = 0
        mail_exclu = []
        pb_set = []
        
        #for cpt_mail_id in range (((cpt_batch - 1) * batch), max_mail):
        for cpt_mail_id in range (((cpt_batch - 1) * batch), max_mail):
            mail_item = mail_list[cpt_mail_id]
            mail_mail = mail_item.lower()
            
            if (filtre_syntax_mail in mail_mail) and ("@" in mail_mail) and (";" not in mail_mail):
                cpt_filtre_mail = cpt_filtre_mail + 1
                if "'" in mail_mail:
                    mail_mail = mail_mail.replace("'","\\'")            
                if len(mail_mail) > 100:
                    mail_mail = mail_mail[:100]          
                mail_domain = mail_mail[mail_mail.find("@") + 1:]
                if len(mail_domain) > 60:
                    mail_domain = mail_domain[:60]
                    
                print "--------------------"
                print mail_mail
                try:
                    #this_mail = session.query(Mail).options(load_only("mail")).\
                    #this_mail = session.query(Mail).\
                    #filter(Mail.mail == mail_mail).first()
                    mail_existe_query = "EXECUTE mail_existe(E'" + mail_mail + "');"
                    cur.execute(mail_existe_query)
                    records = cur.fetchall()  
                    print records
                except:
                    #pass
                    cpt_pb_db = cpt_pb_db + 1
                    new_log = "probleme DB avec: " + str(mail_mail)
                    log.append(new_log)
                    print new_log
                    pb_set.append(str(mail_mail + "," + str(records[0]).strip('()').strip(',') + "," + str(fichier_prov_id)))
                    continue
                    
                if records == []:
                    print "NEW MAIL"
                    cpt_insert_db = cpt_insert_db + 1
                    new_mail = Mail(id=(max_id + cpt_insert_db),\
                                    mail=mail_mail, domain=mail_domain, ok_plainte='False')
                    session.add(new_mail)
                    new_plainte = Mplainte(mail_id=new_mail.id)
                    session.add(new_plainte)
                    new_prov = Fich_x_mail(mail_id=new_mail.id, fichier_id = fichier_prov_id)
                    session.add(new_prov)
                    cpt_insert_db = cpt_insert_db + 1
                    session.commit()
                    print "NEW MAIL INSERTED OK"
                    
                else:
                    print "EXISTING MAIL"
                    this_mail_id = int(str(records[0]).strip('()').strip(','))
                    print "this_mail_id -->", str(this_mail_id)
                    this_mail = session.query(Mail).filter(Mail.id == this_mail_id).first()
                    print this_mail.ok_plainte
                    if not this_mail.ok_plainte:
                        print "THIS MAIL HAS ALREADY BEEN UPDATED"
                    else:
                        this_mail.ok_plainte = 'False'
                        this_mail.multi = this_mail.multi + 1
                        new_plainte = Mplainte(mail_id=this_mail.id)
                        session.add(new_plainte)
                        new_prov = Fich_x_mail(mail_id=this_mail.id, fichier_id = fichier_prov_id)
                        session.add(new_prov)
                        print "EXISTING MAIL UPDATED OK"
                    session.commit()
                      
                
            else:
                cpt_exclu_mail = cpt_exclu_mail + 1
                mail_domain = mail_mail[mail_mail.find("@") + 1:]
                exclu_value = str(mail_mail) + "," + str(mail_domain) 
                mail_exclu.append(exclu_value)
                
        new_log = "  .  " + str(cpt_filtre_mail) + " mails utiles <-- " + str(cpt_mail) + " mails uniques, moins " + str(cpt_exclu_mail) + " mail(s) rejete(s) par le filtre syntaxique."
        log.append(new_log)
        print new_log
        
        if len(mail_exclu) > 0:
            f = open(dossier_pgSQL + "mails_exclus.csv", 'a')
            for x in range(0, len(mail_exclu)):
                f.write(mail_exclu[x] + '\n')
            f.close()
            new_log = "  .  --> fichier EXCLU avec " + str(len(mail_exclu)) + " lignes: " + dossier_pgSQL + "mails_exclus.csv" 
            log.append(new_log)
            print new_log
            
        if len(pb_set) > 0:
            f = codecs.open(dossier_pgSQL + "mails_rejetes_par_DB.csv", 'a', encoding='latin-1')
            for cpt_pb in range(0, len(pb_set)):
                try:
                    f.write(pb_set[cpt_pb] + '\n')
                except:
                    print "Pb. I/O avec :", str(pb_set[cpt_pb])
                    continue 
            f.close()
            new_log = "    ..    --> fichier PB.MAILS avec " + str(len(pb_set)) + " lignes: " + dossier_pgSQL + "mails_rejetes_par_DB.csv"
            log.append(new_log)
            print new_log
        
        new_log = "    ..    " + str(cpt_filtre_mail - cpt_insert_db) + " mails en doublon."
        log.append(new_log)
        print new_log
        new_log = "    ..    " + str(cpt_insert_db) + " mails nouveaux, a inserer dans la base."
        log.append(new_log)
        print new_log
        
        session.commit()
        new_log = "      ...    --> MAJ dans table base: OK"
        log.append(new_log)
        print new_log
        
        for log_line in log:
            log_file.write(str(log_line) + '\n')