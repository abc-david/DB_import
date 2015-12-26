'''
Created on 23 july. 2014

@author: david
'''
#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys, random, codecs
import pandas as pd
import numpy as np
from functions_log import *
from import_functions import check_mot_cle_columnwide


def show_df(df, n_line = 5):
    #pd.set_option('display.max_columns', 20)
    print df.head(n_line)
    print str(len(df.index)) + " lines."
    
def sample_df(df, n):
    return df.ix[random.sample(df.index, n)]
    
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

def remove_floating_part(value):
    num_sep = [",", "."]
    for item in num_sep:
        if item in value:
            pos = value.find(item)
            return int(value[:pos])
    return value

def remove_unknown_columns(df):
    header = list(df.columns)
    for field in header:
        if field.find("unknown") != -1:
            df = df.drop(field, 1)
    return df

def add_header(dataframe, header, remove_unknown = True):
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

def clean_up(df, uniqueness = ['mail']):
    df = drop_null_or_missing_values(df, ['mail'])
    res_sort = sort_unique(df, uniqueness, ['mail'])
    if res_sort[0]:
        df = res_sort[1]
    df = df.reset_index(drop=True)
    show_df(df)
    return df

def explore_path_for_export_regie(path):
    from os import walk
    from import_functions import extract_arguments
    import_files = []
    for (__, __, filenames) in walk(path):
        import_files.extend(filenames)
        break
    file_dict = {}
    for file_name in sorted(import_files):
        file_dict[file_name] = extract_arguments(file_name, silent = True)
    for file_name, args in file_dict.iteritems():
        file_status = []
        for item in args[2]:
            file_status.append(item[0])
        args.append(file_status)
    return file_dict

def load_dataframe_blacklist(path, keyword_check):
    file_dict = explore_path_for_export_regie(path)
    include = pd.DataFrame(columns = ['mail'])
    for name, args in file_dict.iteritems():
        if keyword_check in args[-1] or not args[-1]:
            #res_load = load_dataframe(path, name, args[1], args[-1], regie)
            #if res_load[0]:
            #    partial = res_load[1]
            print name
            partial = pd.read_csv(path + "/" + name, names = ["mail"], header = 0)
            #show_df(partial)
            include = pd.concat([partial, include]).drop_duplicates(['mail']).sort(['mail'])
    include["plainte"] = True
    return include

def load_dataframe_from_export_file(path, name, header, status = "", regie = ""):
    #res_populate = old_populate_dataframe(path, name, names = "", header = "", \
    #                                  skiprows = "", index_col = "", silent = True)
    #res_populate = old_populate_dataframe(path, name, sep = "", attempt_number = "", \
    #                   header = False, skiprows = [0], index_col = "", silent = False)
    #if res_populate[0]:
    #if True:
        #partial = res_populate[1]
        #partial = pd.read_csv(path + "/" + name)
        #res_header = add_header(partial, header)
        #if res_header[0]:
            #partial = res_header[1]
            #partial = drop_null_or_missing_values(partial, ['mail'])
            #res_sort = sort_unique(partial, ['mail'], ['mail'])
            #if res_sort[0]:
            #    partial = res_sort[1]
    
    try:
        partial = pd.read_csv(path + "/" + name)
    except:
        e = sys.exc_info()
        for item in e:
            message = str(item)
            print_to_log(log_file, 3, message)
        return [False]
    if regie:
        partial["regie"] = regie
    else:
        if status:
            if type(status) is list:
                for item in status:
                    partial[item] = True
            else:
                partial[status] = True
    return [True, partial]
    

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
        #show_df(new_df)
        common_rows_with_ref_df = new_df[new_df['flag'] == True].index
        new_rows_unknown_to_ref_df = new_df[new_df['flag'] != True].index
        new_df = new_df.drop('flag',1)
        if output in ['row', 'rows', 'index']:
            return [True, common_rows_with_ref_df, new_rows_unknown_to_ref_df]
        else:
            new_df.loc[common_rows_with_ref_df, exist_key] = True
            new_df.loc[new_rows_unknown_to_ref_df, new_key] = True
            #show_df(new_df)
            nb_records_dict['sorted'] = len(new_df.index)
            nb_records_dict[exist_key] = len(common_rows_with_ref_df)
            nb_records_dict[new_key] = len(new_rows_unknown_to_ref_df)
            return [True, new_df, nb_records_dict]

def write_to_csv(dataframe, path, folder, file_name, header = False):
    if folder:
        csv_file = path + "/" + folder + "/" + file_name
    else:
        csv_file = path + "/" + file_name
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
            dataframe.to_csv(csv_file, sep = ",", cols = write_cols, \
                             header = header, index = False, \
                             na_rep = '', encoding = 'utf-8')
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

def clean_csv(csv_file):
    try:
        with codecs.open(csv_file, 'r', 'utf-8', 'ignore') as csv:
            data = csv.readlines()
            for cpt_line in range(len(data)):
                if "na" in data[cpt_line].lower():
                    data[cpt_line] = data[cpt_line].replace('NaT', '')
                    data[cpt_line] = data[cpt_line].replace('NaN', '')
                    data[cpt_line] = data[cpt_line].replace(',nan,', ',,')
        with codecs.open(csv_file, 'w', encoding = 'utf-8') as csv:
            csv.writelines(data)
        print "CSV file cleaned."
    except:
        e = sys.exc_info()
        for item in e:
            message = str(item)
            print_to_log(log_file, 3, message)