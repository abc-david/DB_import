'''
Created on 23 july. 2014

@author: david
'''
#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time, codecs

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
#log_path = "/media/freebox/Fichiers/ImportDB/Pandas/Test/log/"
log_path = "/home/david/logs/log_script_import"
#log_file = codecs.open(log_path + "log_test_import_functions.txt", 'a', encoding='utf-8')
log_file = codecs.open(log_path + "log_import_files_DB.txt", 'a', encoding='utf-8')

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