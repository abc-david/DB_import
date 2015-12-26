'''
Created on 10 juin 2014

@author: david
'''
#!/usr/bin/env python
# -*- coding: utf-8 -*-

from import_functions import *
from export_regie_functions import *
from pandas.io import sql
from os import walk


db_name = "test_base" #"postgres"
db_user = "postgres"
db_host = "192.168.0.52"
db_pass = "postgres"
db_package = [db_name, db_user, db_host, db_pass]
db_connect = DB_direct_connection(db_package)
if db_connect[0]:
    conn = db_connect[1]
    cur = db_connect[2]

path = "/media/freebox/Fichiers/Export Regies/PCP/regie_pcp_sm"
result_path = "/media/freebox/Fichiers/Export Regies/PCP"
file_name = "Optin_PCP.csv"
#file_name = "PCP-OUT_Export-specialmode_22-05-2014_[mail]_[pcpsm]_false.csv"
#my_date = '2014-05-22'
#file_name = "PCP-OUT_Export-specialmode_13-05-2014_[mail]_[pcpsm]_false.csv"
#my_date = '2014-05-13'
#file_name = "PCP-OUT_Export-specialmode_07-May-2014_[mail]_[pcpsm]_false.csv"
#my_date = '2014-05-07'
#file_name = "PCP-OUT_Export-specialmode_28-04-2014_[x,mail]_[pcpsm]_false.csv"
#my_date = '2014-04-28'

# md5 table
md5_thread = Thread_Return(target = load_md5_table, \
                              args = (conn, ), \
                              kwargs = {'limit' : 0})
#md5_thread.start()

""" Script pour recuperer le resultat d'une query dans postgres
list_fichier_mimi = ['EXP-PCP']
sub_query_dict = {'select' : {'fichier_list' : 'fichier_id'}, \
                  'where' : {'fichier_num' : list_fichier_mimi}}
select_dict = {'base' : ['id', 'mail'], \
               'id' : ['left', 'prenom', 'nom', 'civilite', 'birth', 'cp', 'ville']}
               #'regie_pcp_sm' : ['left', 'date']}
where_dict = {'fichier_match.fichier_id' : sub_query_dict}
query_dict = {'select' : select_dict, 'where' : where_dict}
#where_date = "CAST(regie_pcp_sm.date AS DATE) = '%s'" % (str(my_date))
query = query_builder(query_dict)
#query = query[:-1] + " AND " + where_date + ";"
print query
df = sql.read_sql(query, conn, coerce_float=False)
df = df.drop_duplicates('mail')
#show_df(df)
df = fix_year_problem_df(df, 'birth')
show_df(df, 10)
df = df[['mail', 'prenom', 'nom', 'civilite', 'birth', 'cp', 'ville']]
"""

""" Script pour faire le tri dans les optins PCP
path = "/media/freebox/Fichiers/Export Regies/PCP/Actifs"
file_name = "EXP-PCP_export-pcp-spmode-optin_2014-05-22_[civilite,nom,prenom,mail,cp,ville]_false.csv"
#file_name = "EXP-PCP_export-pcp-welove-optin_2014-05-22_[civilite,nom,prenom,mail,cp,ville]_false.csv"
args = extract_arguments(file_name)
header = args[1]
load_res = load_text_file(path + "/" + file_name, header)
if load_res[0]:
    df_ref = load_res[1]
    df_ref['optin'] = "spmode"
    df_ref = df_ref[['mail', 'optin']]
    show_df(df_ref, 10)
    
header = ['mail','prenom','nom','civilite','birth','cp','ville']
path = "/media/freebox/Fichiers/Export Regies/PCP/Resultat (juin)"
file_name_all_optin = "Optin_PCP.csv"
load_res = load_text_file(path + "/" + file_name_all_optin, header)
if load_res[0]:
    df_info = load_res[1]
    show_df(df_info, 10)
    
df_merge = pd.merge(df_ref, df_info)
df_merge = df_merge.drop('optin', 1)
show_df(df_merge, 10)
"""

""" Script pour recuperer les exports precedents 
df_pcp = pd.read_csv(path + "/" + file_name)
df_pcp = df_pcp[['mail', 'date_collecte']]
show_df(df_pcp, 10)
df_merge = pd.merge(df, df_pcp, on = 'mail', how = 'left')
show_df(df_merge, 10) """

""" Script pour virer les plaintes
plainte_path = "/media/freebox/Fichiers/Export Regies/Plaintes Mimi"
df_plainte = load_data_for_export_regie(plainte_path, 'plainte_mimi', exclude_fields = "")
show_df(df_plainte, 10)
whats_left = pd.merge(df_merge, df_plainte, on = 'mail', how = 'left')
#show_df(whats_left)
df = whats_left[whats_left['plainte_mimi'] != True]
df = df.drop('plainte_mimi',1)
show_df(df) """

path = "/media/freebox/Fichiers/Export Regies/Predictys"
file_name = "PREDICTYS_Export_15-mai-2014_[mail,prenom,nom,civilite,birth,cp,ville,ip,provenance]_260k.csv"
args = extract_arguments(file_name)
header = args[1]
load_res = load_text_file(path + "/" + file_name, header)
if load_res[0]:
    df_ref = load_res[1]
    show_df(df_ref, 10)
    df_ref = fix_year_problem_df(df_ref, 'birth')
    show_df(df_ref, 10)
    
exclude_file_list = ['plaintes_we-mag.txt','npai_we-mag.txt','desinscrits_we-mag.txt']
for file_csv in exclude_file_list:
    df_exclude = pd.read_csv(path + "/" + file_csv, header = 0, names = ['mail'])
    show_df(df_exclude, 10)
    map_res = map_existing_rows(df_ref, df_exclude, join_key = 'mail', output = 'df', exist_key = 'remove', new_key = 'keep')
    if map_res[0]:
        df_merge = map_res[1]
        result = map_res[2]
        kv = []
        for k, v in result.iteritems():
            kv.append(str(k) + " : " + str(v))
        message = str(" | ".join(kv))
        print_to_log(log_file, 3, message)
        df_merge = df_merge[(df_merge['keep'] == True)]
        df_merge = df_merge.drop('remove', 1)
        df_merge = df_merge.drop('keep', 1)
        df_ref = df_merge
        show_df(df_ref, 10)
    
    
result_path = "/media/freebox/Fichiers/Export Regies/PCP"
#write_to_csv(df_merge, result_path, "Resultat (juin)", file_name, header = True)
write_to_csv(df_ref, path, "260k", file_name, header = True)

with open(path + "/260k/" + file_name, 'r') as f:
    content = f.read()
    replace_list = ['NULL', 'NaN', 'nan', 'NaT', "\\"]
    for item in replace_list:
        content = content.replace(item, '')    
with open(result_path + "/Resultat (juin)/" + file_name, 'w') as f:
    f.write(content)

"""
query_id = "SELECT mail_id, prenom, nom, civilite, birth, cp, ville FROM id WHERE mail_id IN"
query_lead = "SELECT mail_id, ip, provenance, date FROM lead WHERE mail_id IN"

path = "/media/freebox/Fichiers/Export Regies/PCP/regie_pcp_sm"
import_files = []
for (dirpath, dirnames, filenames) in walk(path):
    import_files.extend(filenames)
    break
df_dict = {}
for file_name in sorted(import_files):
    df = pd.read_csv(path + "/" + file_name)
    df = append_md5_field(df)
    #show_df(df)
    df_dict[file_name] = df
    
load_md5_table_result = md5_thread.join()
if load_md5_table_result[0]:
    md5_df = load_md5_table_result[1]
    
for filename, df in df_dict.iteritems():
    lookup_res = lookup_md5(df, md5_df)
    if lookup_res[0]:
        df = lookup_res[1]
        df = df[(df['exist'] == True)]
        df = df[['mail', 'date_collecte', 'pg_id']]
        df = df.rename(columns = {'pg_id' : 'id'})
        #show_df(df)
        # filter out plaintes
        whats_left = pd.merge(df, df_plainte, on = 'mail', how = 'left')
        #show_df(whats_left)
        df = whats_left[whats_left['plainte_mimi'] != True]
        #show_df(df)
        
        id_list = []
        for cpt in range(len(df.index)):
            cpt_index = df.index[cpt]
            raw_id = df.at[cpt_index, 'id']
            str_id = str(raw_id)
            clean_id = remove_floating_part(str_id)
            id_list.append(clean_id)
        id_list_str = ",".join(str(id_item) for id_item in id_list)
        #print id_list_str
        # id table
        query = query_id + " (" + str(id_list_str) + ");"
        df_id = sql.read_sql(query, conn, coerce_float=False)
        df_id = df_id.rename(columns = {'mail_id' : 'id'})
        #show_df(df_id)
        df = pd.merge(df, df_id, on = 'id', how = 'left').drop_duplicates('mail')
        # lead table
        #query = query_lead + " (" + str(id_list_str) + ");"
        #df_lead = sql.read_sql(query, conn)
        #df_lead = df_lead.rename(columns = {'mail_id' : 'id'})
        #show_df(df_lead)
        #df = pd.merge(df, df_lead, on = 'id', how = 'left').drop_duplicates('mail')
        #df = fix_year_problem_df(df, 'birth', out_format = '%m/%d/%Y')
        df = df.drop('id',1)
        df = df.drop('plainte_mimi',1)
        show_df(df)
        write_to_csv(df, result_path, "Resultat (juin)", filename, header = True)
        #df.to_csv(result_path + filename, sep = ',', index = None)
"""