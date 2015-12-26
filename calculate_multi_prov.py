'''
Created on 17 nov. 2013

@author: administrateur
'''
#!/usr/bin/env python
# -*- coding: utf-8 -*-
from DB_mapping import *
import random, math, datetime
from sqlalchemy import *
from sqlalchemy.orm import *
from sqlalchemy.ext.declarative import declarative_base
dburl = 'postgresql+psycopg2://postgres:postgres@192.168.0.52/postgres'
u2_db = create_engine(dburl) #, echo=True
u2_metadata = MetaData(schema="public")
conn = u2_db.connect()

base_table = Table('base', u2_metadata,\
                   Column('id', Integer, primary_key = True),\
                   Column('ok_mot', Boolean, default = True))

def get_batch_extrems(b_iter, b_size, end, start_include = 0):
    start_point = ((b_iter - 1) * b_size) + start_include
    end_point = (b_iter * b_size) + start_include - 1
    if end_point > end:
        end_point = end
    return (start_point, end_point)
    
def get_batch_mail_list(b_iter, b_size, max_id):
    mail_list_extrems = get_batch_extrems(b_iter, b_size, max_id, 1)
    #print b_iter, mail_list_extrems
    mail_list = session.query(Mail).options(load_only("id")).\
        filter(and_(Mail.id >= mail_list_extrems[0], Mail.id <= mail_list_extrems[1])).\
        order_by(Mail.id.asc()).all()
    return(mail_list_extrems, mail_list)

update_base = base_table.update().\
    where(base_table.c.id==bindparam('mail_id')).\
    values(ok_mot=bindparam('mail_mot'))

start_time = datetime.datetime.now()
max_id = session.query(func.max(Mail.id)).first()[0]
print max_id
start_id = 17670000
batch_mail_size = 100000
batch_update_size = 10000
min_cpt_batch_mail = int(math.modf(start_id / batch_mail_size)[1] + 1)
max_cpt_batch_mail = int(math.modf(max_id / batch_mail_size)[1] + 1)
print max_cpt_batch_mail
batch_mail_time = start_time
batch_update_time = start_time
for cpt_batch_mail in range(min_cpt_batch_mail, max_cpt_batch_mail + 1):
    mail_list_package = get_batch_mail_list(cpt_batch_mail, batch_mail_size, max_id)
    last_batch_mail_time = batch_mail_time
    batch_mail_time = datetime.datetime.now()
    mail_diff = batch_mail_time - last_batch_mail_time
    start_diff = batch_mail_time - start_time
    print cpt_batch_mail, "-- mail_diff: " + str(mail_diff.seconds) + "s",\
        "-- start_diff: " + str(start_diff.seconds) + "s"
    
    max_cpt_batch_update = int(batch_mail_size / batch_update_size)
    for cpt_batch_update in range(1, max_cpt_batch_update + 1):
        batch_extrems = get_batch_extrems(cpt_batch_update, batch_update_size,\
                                          batch_mail_size)
        update_values = []
        for mail in mail_list_package[1][batch_extrems[0]:batch_extrems[1] + 1]:
            update_dict = {}
            #print mail, mail.mxm
            if len(mail.mxm) > 0:
                #for mxm in mail.mxm:
                    #print "--", mail.mail, mxm.mot.mot
                update_dict['mail_id'] = mail.id
                update_dict['mail_mot'] = 'False'
                update_values.append(update_dict)
        last_batch_update_time = batch_update_time
        batch_update_time = datetime.datetime.now()
        update_diff = batch_update_time - last_batch_update_time
        start_diff = batch_update_time - start_time
        conn.execute(update_base, update_values)
        print "   ", mail.id, "-- update_diff: " + str(update_diff.seconds) + "s",\
        "-- start_diff: " + str(start_diff.seconds) + "s"
