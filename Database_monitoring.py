'''
Created on Jun 13, 2016

@author: walter
'''
#!/usr/bin/env python3
# -*- coding:utf-8 -*-

' The first Python project '

__author__ = 'Walter Xiong'

import sys
import getopt
import psycopg2

def main(argv):
    host1=''
    dbname1=''
    port1=''
    user1=''
    password1=''
    host2=''
    dbname2=''
    port2=''
    user2=''
    password2=''
    target=''
    ignore_schema=''
    ignore_function=''
    ignore_view=''
    choose_schema=''
    choose_table=''
    choose_function=''
    choose_view=''
        
    try:
        opts,args=getopt.getopt(argv,"h:d:p:u:w:H:D:P:U:W:t:s:f:v:S:T:F:V",["host1=", "dbname1=", "port1=", "user1=", "password1=","host2=", "dbname2=", "port2=", "user2=", "password2=","target=","ignore_schema=","ignore_function=","ignore_view=","choose_schema=","choose_table=","choose_function=","choose_view="])
    except getopt.GetoptError:
        sys.exit()
        
    for key,value in opts:
        if key in ('-h','--host1'):
            host1=value
        if key in ('-d','--dbname1'):
            dbname1=value
        if key in ('-p','--port1'):
            if value=='':
                port1='5432'
            else:
                port1=value
        if key in ('-u','--user1'):
            if value=='':
                user1='postgres'
            else:
                user1=value
        if key in ('-w','--password1'):
            password1=value
        if key in ('-H','--host2'):
            host2=value
        if key in ('-D','--dbname2'):
            dbname2=value
        if key in ('-P','--port2'):
            if value=='':
                port2='5432'
            else:
                port2=value
        if key in ('-U','--user2'):
            if value=='':
                user2='postgres'
            else:
                user2=value
        if key in ('-W','--password2'):
            password2=value
        if key in ('-t','--target'):
            target=value
        if key in ('-s','--ignore_schema'):
            ignore_schema=value
        if key in ('-f','--ignore_function'):
            ignore_function=value
        if key in ('-v','--ignore_view'):
            ignore_view=value
        if key in ('-S','--choose_schema'):
            choose_schema=value
        if key in ('-T','--choose_table'):
            choose_table=value
        if key in ('-F','--choose_function'):
            choose_function=value
        if key in ('-V','--choose_view'):
            choose_view=value
                
    connect_db(host1, dbname1, port1, user1, password1,host2, dbname2, port2, user2, password2,target,ignore_schema,ignore_function,ignore_view,choose_schema,choose_table,choose_function,choose_view)
    

def connect_db(host1, dbname1, port1, user1, password1,host2, dbname2, port2, user2, password2,target,ignore_schema='',ignore_function='',ignore_view='',choose_schema='',choose_table='',choose_function='',choose_view=''):
    
    conn_string1= "host=%s dbname=%s port=%s user=%s password=%s"%(host1,dbname1,port1,user1,password1)
    conn1=psycopg2.connect(conn_string1)
    cursor1=conn1.cursor()
    
    conn_string2= "host=%s dbname=%s port=%s user=%s password=%s"%(host2,dbname2,port2,user2,password2)
    conn2=psycopg2.connect(conn_string2)
    cursor2=conn2.cursor()
    
    if target=='table':
        if ignore_schema!='':
            database_schema_check(target,cursor1,cursor2,dbname1,dbname2,ignore_schema=ignore_schema)
        elif choose_schema!='':
            if choose_table!='':
                table_content_check(dbname1,cursor1,dbname2,cursor2,choose_schema,choose_table)
            else:
                database_schema_check(target,cursor1,cursor2,dbname1,dbname2,choose_schema=choose_schema)
        else:
            database_schema_check(target,cursor1,cursor2,dbname1,dbname2)
    elif target=='function':
        if choose_schema!='' and choose_function!='':
            if specified_check(cursor1,cursor2,dbname1,dbname2,choose_schema,choose_function=choose_function):
                print('In schema %r, the defination of function %r in those two databases are matching'%(choose_schema,choose_function))
            else:
                print('In schema %r, the defination of function %r in those two databases are not matching'%(choose_schema,choose_function))
        elif choose_schema!='' and choose_function=='':
            entire_check(target,cursor1,cursor2,dbname1,dbname2,choose_schema=choose_schema)
        elif ignore_schema!='':
            entire_check(target,cursor1,cursor2,dbname1,dbname2,ignore_schema=ignore_schema)
        else:
            entire_check(target,cursor1,cursor2,dbname1,dbname2)
    elif target=='view':
        if choose_schema!='' and choose_view!='':
            if specified_check(cursor1,cursor2,dbname1,dbname2,choose_schema,choose_view=choose_view):
                print('In schema %r, the defination of function %r in those two databases are matching'%(choose_schema,choose_function))
            else:
                print('In schema %r, the defination of function %r in those two databases are not matching'%(choose_schema,choose_function))
        elif choose_schema!='' and choose_view=='':
            entire_check(target,cursor1,cursor2,dbname1,dbname2,choose_schema=choose_schema)
        elif ignore_schema!='':
            entire_check(target,cursor1,cursor2,dbname1,dbname2,ignore_schema=ignore_schema)
        else:
            entire_check(target,cursor1,cursor2,dbname1,dbname2)
    
#     elif ANOTHER PART
            
def table_content_check(dbname1,cursor1,dbname2,cursor2,choose_schema,choose_table):
        
    if not table_schema_compare(choose_schema,choose_table,cursor1,cursor2):
        print('Due to the different table schema in \'%s.%s\', we cannot process the content check\n'%(choose_schema,choose_table))
    else:
        table_content_compare(choose_schema,choose_table,cursor1,cursor2,dbname1,dbname2)
        
def table_schema_compare(choose_schema,choose_table,cursor1,cursor2):
    
    cursor1.execute('select count(*) from information_schema.columns where table_schema=\'%s\' and table_name=\'%s\';'%(choose_schema,choose_table))
    s_columns=cursor1.fetchall()
    
    cursor1.execute('select column_name,data_type from information_schema.columns where table_schema=\'%s\' and table_name=\'%s\';'%(choose_schema,choose_table))
    s_columns_infor=cursor1.fetchall()
    
    cursor2.execute('select count(*) from information_schema.columns where table_schema=\'%s\' and table_name=\'%s\';'%(choose_schema,choose_table))
    c_columns=cursor2.fetchall()
    
    cursor2.execute('select column_name,data_type from information_schema.columns where table_schema=\'%s\' and table_name=\'%s\';'%(choose_schema,choose_table))
    c_columns_infor=cursor2.fetchall()

        
    x,i,count,sign=0,0,0,0
     
    if c_columns!=s_columns:
        print('In table \'%s.%s\', the number of columns are not match!\n'%(choose_schema,choose_table))
    
    while x<len(s_columns_infor):
        while i<len(c_columns_infor):
            while count<len(c_columns_infor[0]):
                if s_columns_infor[x][count]==c_columns_infor[i][count]:
                    break
                else:
                    break
            if s_columns_infor[x][count]==c_columns_infor[i][count] and (count+1)<len(c_columns_infor[0]):
                count=count+1
            elif s_columns_infor[x][count]==c_columns_infor[i][count] and (count+1)>=len(c_columns_infor[0]):
                if (x+1)<len(s_columns_infor):
                    x=x+1
                    i=0
                    count=0
                else:
                    if sign==0:
#                             print('The schema of table \'%s.%s\' in those two databases are completely matching\n'%(choose_schema,choose_table))
                        return True
                    else:
                        print('The schema of table \'%s.%s\' in those two databases are not completely matching\n'%(choose_schema,choose_table))
                        return False
            elif s_columns_infor[x][count]!=c_columns_infor[i][count]:
                if (i+1)<len(c_columns_infor):
                    i=i+1
                    count=0
                else:
                    print('There is no match column ',s_columns_infor[x],' in table \'%s.%s\'\n'%(choose_schema,choose_table))
                    sign=sign+1
                    if (x+1)<len(s_columns_infor):
                        x=x+1
                        i=0
                        count=0
                    else:
                        if sign==0:
#                                 print('\nThe schema of table \'%s.%s\' in those two databases are completely matching'%(choose_schema,choose_table))
                            return True
                        else:
                            print('\nThe schema of table \'%s.%s\' in those two databases are not completely matching'%(choose_schema,choose_table))
                            return False

def table_content_compare(choose_schema,choose_table,cursor1,cursor2,dbname1,dbname2):
    
    cursor1.execute('select * from %s.%s'%(choose_schema,choose_table))
    s_colnames = [desc[0] for desc in cursor1.description]
    
    cursor1.execute('select * from %s.%s'%(choose_schema,choose_table))
    s_data=cursor1.fetchall()
    
    cursor2.execute('select * from %s.%s'%(choose_schema,choose_table))
    c_colnames = [desc[0] for desc in cursor2.description]
    
    cursor2.execute('select * from %s.%s'%(choose_schema,choose_table))
    c_data=cursor2.fetchall()

    
    x,i,sign,s_count,c_count=0,0,0,0,0
    
    if len(s_data)!=len(c_data):
        sign=sign+1
        print('\nThe number of records in table \'%s.%s\' are not match'%(choose_schema,choose_table))
    else:
        print('\nThe number of records in table \'%s.%s\' are match'%(choose_schema,choose_table))
    
    if len(s_colnames)==1 and s_colnames[0]=='id':
        sign=sign+1
        print('\nThe table \'%s.%s\' only has one column'%(choose_schema,choose_table))
    elif len(s_colnames)==1 and s_colnames[0]!='id':
        s_count,c_count=0,0
    elif len(s_colnames)>1:
        for s in s_colnames:
            if s=='id':
                s_count,c_count=1,1
                break
            else:
                s_count,c_count=0,0

    while x<len(s_data):
        while i<len(c_data):
            while s_count<len(s_colnames) and c_count<len(c_colnames):
                if s_colnames[s_count]==c_colnames[c_count] and s_data[x][s_count]==c_data[i][c_count] and (s_count+1)<len(s_colnames):
                    s_count=s_count+1
                    c_count=1
                elif s_colnames[s_count]==c_colnames[c_count] and s_data[x][s_count]==c_data[i][c_count] and (s_count+1)>=len(s_colnames):
                    break
                elif s_colnames[s_count]==c_colnames[c_count] and s_data[x][s_count]!=c_data[i][c_count]:
                    if (i+1)<len(c_data):
                        break
                    else:
                        sign=sign+1
                        print('\nFor database %r, there is no matching record: %r'%(dbname2,s_data[x]))
                        break
                else:
                    c_count=c_count+1
            if s_colnames[s_count]==c_colnames[c_count] and s_data[x][s_count]==c_data[i][c_count] and (s_count+1)>=len(s_colnames):
                break
            elif s_colnames[s_count]==c_colnames[c_count] and s_data[x][s_count]!=c_data[i][c_count] and (i+1)>=len(c_data):
                break
            elif s_colnames[s_count]==c_colnames[c_count] and s_data[x][s_count]!=c_data[i][c_count] and (i+1)<len(c_data):
                i=i+1
                if len(s_colnames)==1 and s_colnames[0]!='id':
                    s_count,c_count=0,0
                elif len(s_colnames)>1:
                    for s in s_colnames:
                        if s=='id':
                            s_count,c_count=1,1
                            break
                        else:
                            s_count,c_count=0,0
            else:
                break
        if (x+1)<len(s_data):
            x=x+1
            i=0
            if len(s_colnames)==1 and s_colnames[0]!='id':
                s_count,c_count=0,0
            elif len(s_colnames)>1:
                for s in s_colnames:
                    if s=='id':
                        s_count,c_count=1,1
                        break
                    else:
                        s_count,c_count=0,0
        else:
            if sign==0:
                print('\nFor database %r, those two tables might be matching'%dbname1)
                break
            else:
                break
    
    x,i=0,0
    
    if len(s_colnames)==1 and s_colnames[0]=='id':
        sign=sign+1
        print('\nThe table \'%s.%s\' only has one column'%(choose_schema,choose_table))
    elif len(s_colnames)==1 and s_colnames[0]!='id':
        s_count,c_count=0,0
    elif len(s_colnames)>1:
        for s in s_colnames:
            if s=='id':
                s_count,c_count=1,1
                break
            else:
                s_count,c_count=0,0
                
    while i<len(c_data):
        while x<len(s_data):
            while s_count<len(s_colnames) and c_count<len(c_colnames):
                if s_colnames[s_count]==c_colnames[c_count] and s_data[x][s_count]==c_data[i][c_count] and (c_count+1)<len(c_colnames):
                    c_count=c_count+1
                    s_count=1
                elif s_colnames[s_count]==c_colnames[c_count] and s_data[x][s_count]==c_data[i][c_count] and (c_count+1)>=len(c_colnames):
                    break
                elif s_colnames[s_count]==c_colnames[c_count] and s_data[x][s_count]!=c_data[i][c_count]:
                    if (x+1)<len(s_data):
                        break
                    else:
                        sign=sign+1
                        print('\nFor database %r, there is no matching record: %r'%(dbname1,c_data[i]))
                        break
                else:
                    s_count=s_count+1
            if s_colnames[s_count]==c_colnames[c_count] and s_data[x][s_count]==c_data[i][c_count] and (c_count+1)>=len(c_colnames):
                break
            elif s_colnames[s_count]==c_colnames[c_count] and s_data[x][s_count]!=c_data[i][c_count] and (x+1)>=len(s_data):
                break
            elif s_colnames[s_count]==c_colnames[c_count] and s_data[x][s_count]!=c_data[i][c_count] and (x+1)<len(s_data):
                x=x+1
                if len(s_colnames)==1 and s_colnames[0]!='id':
                    s_count,c_count=0,0
                elif len(s_colnames)>1:
                    for s in s_colnames:
                        if s=='id':
                            s_count,c_count=1,1
                            break
                        else:
                            s_count,c_count=0,0
            else:
                break
        if (i+1)<len(c_data):
            i=i+1
            x=0
            if len(s_colnames)==1 and s_colnames[0]!='id':
                s_count,c_count=0,0
            elif len(s_colnames)>1:
                for s in s_colnames:
                    if s=='id':
                        s_count,c_count=1,1
                        break
                    else:
                        s_count,c_count=0,0
        else:
            if sign==0:
                print('\nFor database %r, those two tables might be matching'%dbname2)
                break
            else:
                break
    
    if sign==0:
        print('\nThe table \'%s.%s\' in those two databases are totally matching'%(choose_schema,choose_table))
    else:
        print('\nThe table \'%s.%s\' in those two databases are not totally matching'%(choose_schema,choose_table))


def database_schema_check(target,cursor1,cursor2,dbname1,dbname2,ignore_schema='',choose_schema=''):
    
    count=0
    
    if choose_schema=='':
        
        S_schema,judge=database_structure_check(cursor1,cursor2,dbname1,dbname2,ignore_schema)
        
        if not judge:
            count=count+1
            
        for s_s in S_schema:
            t=[[s_s]]
            S_schema_table,judge=schema_structure_check(target,cursor1,cursor2,dbname1,dbname2,t)
            
            if not judge:
                count=count+1
                
            for s_t in S_schema_table:
                if not table_schema_compare(s_s[0],s_t[0],cursor1,cursor2):
                    count=count+1    
        
        if count==0:
            print('The schemas of those two databases are completely matching\n')
        else:
            print('The schemas of those two databases are not completely matching\n')
        
    elif choose_schema!='':
        t=[[choose_schema]]
        
        S_schema_table,judge=schema_structure_check(target,cursor1,cursor2,dbname1,dbname2,t)
        
        if not judge:
            count=count+1
            
        for s_t in S_schema_table:
            if not table_schema_compare(t,s_t[0],cursor1,cursor2):
                count=count+1
            
        if count==0:
            print('The schema %r of those two databases are completely matching\n'%choose_schema)
        else:
            print('The schema %r of those two databases are not completely matching\n'%choose_schema)

        
def database_structure_check(cursor1,cursor2,dbname1,dbname2,ignore_schema=''):
    
    cursor1.execute('select schema_name from information_schema.schemata where schema_name <> \'information_schema\' and schema_name !~ E\'^pg_\' and schema_name not like \'%s\';'%ignore_schema)
    s_schema=cursor1.fetchall()
    
    cursor2.execute('select schema_name from information_schema.schemata where schema_name <> \'information_schema\' and schema_name !~ E\'^pg_\' and schema_name not like \'%s\';'%ignore_schema)
    c_schema=cursor2.fetchall()

    
    s_schema_number,c_schema_number,schema_count=0,0,0
     
    if len(s_schema)!=len(c_schema):
        schema_count=schema_count+1
        print('The number of schemas in those two databases are not matching\n')
#     else:
#         print('The number of schemas in those two databases are matching\n')
        
    while s_schema_number<len(s_schema):
        while c_schema_number<len(c_schema):
            if s_schema[s_schema_number]==c_schema[c_schema_number] and (s_schema_number+1)<len(s_schema):
                s_schema_number=s_schema_number+1
                c_schema_number=0
            elif s_schema[s_schema_number]==c_schema[c_schema_number] and (s_schema_number+1)>=len(s_schema):
                s_schema_number,c_schema_number=0,0
                break
            elif s_schema[s_schema_number]!=c_schema[c_schema_number] and (c_schema_number+1)<len(c_schema):
                c_schema_number=c_schema_number+1
            elif s_schema[s_schema_number]!=c_schema[c_schema_number] and (c_schema_number+1)>=len(c_schema) and (s_schema_number+1)<len(s_schema):
                schema_count=schema_count+1
                print('The schema %r can not be found in database %r\n'%(s_schema[s_schema_number],dbname2))
#                     s_schema.pop(s_schema_number)
                s_schema_number=s_schema_number+1
                c_schema_number=0
            elif s_schema[s_schema_number]!=c_schema[c_schema_number] and (c_schema_number+1)>=len(c_schema) and (s_schema_number+1)>=len(s_schema):
                schema_count=schema_count+1
                print('The schema %r can not be found in database %r\n'%(s_schema[s_schema_number],dbname2))
#                     s_schema.pop(s_schema_number)
                s_schema_number,c_schema_number=0,0
                break
        break
    
    s_schema_number,c_schema_number=0,0
    
    while c_schema_number<len(c_schema):
        while s_schema_number<len(s_schema):
            if c_schema[c_schema_number]==s_schema[s_schema_number] and (c_schema_number+1)<len(c_schema):
                c_schema_number=c_schema_number+1
                s_schema_number=0
            elif c_schema[c_schema_number]==s_schema[s_schema_number] and (c_schema_number+1)>=len(c_schema):
                s_schema_number,c_schema_number=0,0
                break
            elif c_schema[c_schema_number]!=s_schema[s_schema_number] and (s_schema_number+1)<len(s_schema):
                s_schema_number=s_schema_number+1
            elif c_schema[c_schema_number]!=s_schema[s_schema_number] and (s_schema_number+1)>=len(s_schema) and (c_schema_number+1)<len(c_schema):
                schema_count=schema_count+1
                print('The schema %r can not be found in database %r\n'%(c_schema[c_schema_number],dbname1))
                c_schema.pop(c_schema_number)
#                     c_schema_number=c_schema_number+1
                s_schema_number=0
            elif c_schema[c_schema_number]!=s_schema[s_schema_number] and (s_schema_number+1)>=len(s_schema) and (c_schema_number+1)>=len(c_schema):
                schema_count=schema_count+1
                print('The schema %r can not be found in database %r\n'%(c_schema[c_schema_number],dbname1))
                c_schema.pop(c_schema_number)
                s_schema_number,c_schema_number=0,0
                break
        break
        
    if schema_count==0:
        print('The schemas of those two databases are matching\n')
        return c_schema,True
    else:
        print('The schemas of those two databases are not matching\n')
        return c_schema,False

def schema_structure_check(target,cursor1,cursor2,dbname1,dbname2,schema_name):
    
    s_schema_table_number,c_schema_table_number,schema_table_count,count=0,0,0,0
    
    for s in schema_name:
        s_schema_table_number,c_schema_table_number,schema_table_count=0,0,0
        if target=='table':
            cursor1.execute('select tablename as table from pg_tables where schemaname = \'%s\';'%s[0])
            s_schema_table=cursor1.fetchall()
            
            cursor2.execute('select tablename as table from pg_tables where schemaname = \'%s\';'%s[0])
            c_schema_table=cursor2.fetchall()
        elif target=='function':
            cursor1.execute('SELECT routine_name FROM information_schema.routines WHERE routine_type=\'FUNCTION\' AND specific_schema=\'%s\';'%s[0])
            s_schema_table=cursor1.fetchall()
            
            cursor2.execute('SELECT routine_name FROM information_schema.routines WHERE routine_type=\'FUNCTION\' AND specific_schema=\'%s\';'%s[0])
            c_schema_table=cursor2.fetchall()

        elif target=='view':
            cursor1.execute('select viewname as view from pg_views where schemaname = \'%s\';'%s[0])
            s_schema_table=cursor1.fetchall()
            
            cursor2.execute('select viewname as view from pg_views where schemaname = \'%s\';'%s[0])
            c_schema_table=cursor2.fetchall()
            
#         elif ANOTHER PART

        
        if len(s_schema_table)!=len(c_schema_table):
            print('The number of tables in schema %r are not matching \n'%s[0])
            count=count+1
#         else:
#             print('The number of tables in schema %r are matching \n'%s[0])
            
        while s_schema_table_number<len(s_schema_table):
            while c_schema_table_number<len(c_schema_table):
                if s_schema_table[s_schema_table_number]==c_schema_table[c_schema_table_number] and (s_schema_table_number+1)<len(s_schema_table):
                    s_schema_table_number=s_schema_table_number+1
                    c_schema_table_number=0
                elif s_schema_table[s_schema_table_number]==c_schema_table[c_schema_table_number] and (s_schema_table_number+1)>=len(s_schema_table):
                    s_schema_table_number,c_schema_table_number=0,0
                    break
                elif s_schema_table[s_schema_table_number]!=c_schema_table[c_schema_table_number] and (c_schema_table_number+1)<len(c_schema_table):
                    c_schema_table_number=c_schema_table_number+1
                elif s_schema_table[s_schema_table_number]!=c_schema_table[c_schema_table_number] and (c_schema_table_number+1)>=len(c_schema_table) and (s_schema_table_number+1)<len(s_schema_table):
                    schema_table_count=schema_table_count+1
                    print('In schema %r database %r, the table %r can not be found\n'%(s[0],dbname2,s_schema_table[s_schema_table_number][0]))
#                         s_schema_table.pop(s_schema_table_number)
                    s_schema_table_number=s_schema_table_number+1
                    c_schema_table_number=0
                elif s_schema_table[s_schema_table_number]!=c_schema_table[c_schema_table_number] and (c_schema_table_number+1)>=len(c_schema_table) and (s_schema_table_number+1)>=len(s_schema_table):
                    schema_table_count=schema_table_count+1
                    print('In schema %r database %r, the table %r can not be found\n'%(s[0],dbname2,s_schema_table[s_schema_table_number][0]))
#                         s_schema_table.pop(s_schema_table_number)
                    s_schema_table_number,c_schema_table_number=0,0
                    break
            break
        s_schema_table_number,c_schema_table_number=0,0
        
        while c_schema_table_number<len(c_schema_table):
            while s_schema_table_number<len(s_schema_table):
                if c_schema_table[c_schema_table_number]==s_schema_table[s_schema_table_number] and (c_schema_table_number+1)<len(c_schema_table):
                    c_schema_table_number=c_schema_table_number+1
                    s_schema_table_number=0
                elif c_schema_table[c_schema_table_number]==s_schema_table[s_schema_table_number] and (c_schema_table_number+1)>=len(c_schema_table):
                    s_schema_table_number,c_schema_table_number=0,0
                    break
                elif c_schema_table[c_schema_table_number]!=s_schema_table[s_schema_table_number] and (s_schema_table_number+1)<len(s_schema_table):
                    s_schema_table_number=s_schema_table_number+1
                elif c_schema_table[c_schema_table_number]!=s_schema_table[s_schema_table_number] and (s_schema_table_number+1)>=len(s_schema_table) and (c_schema_table_number+1)<len(c_schema_table):
                    schema_table_count=schema_table_count+1
                    print('In schema %r database %r, the table %r can not be found\n'%(s[0],dbname1,c_schema_table[c_schema_table_number][0]))
                    c_schema_table.pop(c_schema_table_number)
#                         c_schema_table_number=c_schema_table_number+1
                    s_schema_table_number=0
                elif c_schema_table[c_schema_table_number]!=s_schema_table[s_schema_table_number] and (s_schema_table_number+1)>=len(s_schema_table) and (c_schema_table_number+1)>=len(c_schema_table):
                    schema_table_count=schema_table_count+1
                    print('In schema %r database %r, the table %r can not be found\n'%(s[0],dbname1,c_schema_table[c_schema_table_number][0]))
                    c_schema_table.pop(c_schema_table_number)
                    s_schema_table_number,c_schema_table_number=0,0
                    break
            if schema_table_count!=0:
                count=count+1
                break
            else:
                break
        
    if count==0:
        print('The structure of schemas in those two databases are matching\n')
        return c_schema_table,True
    else:
        print('The structure of schemas in those two databases are not matching\n')
        return c_schema_table,False


def specified_check(cursor1,cursor2,dbname1,dbname2,choose_schema,choose_function='',choose_view=''):
    
    if choose_function!='':
        cursor1.execute('SELECT routine_definition FROM information_schema.routines WHERE specific_schema LIKE \'%s\' AND routine_name LIKE \'%s\';'%(choose_schema,choose_function))
        specified1=cursor1.fetchall()
        
        cursor2.execute('SELECT routine_definition FROM information_schema.routines WHERE specific_schema LIKE \'%s\' AND routine_name LIKE \'%s\';'%(choose_schema,choose_function))
        specified2=cursor2.fetchall()
        
    elif choose_view!='':
        cursor1.execute('select definition from pg_views where viewname = \'%s\' and schemaname=\'%s\';'%(choose_view,choose_schema))
        specified1=cursor1.fetchall()
        
        cursor2.execute('select definition from pg_views where viewname = \'%s\' and schemaname=\'%s\';'%(choose_view,choose_schema))
        specified2=cursor2.fetchall()
        
#     elif ANOTHER PART
    
        
    if specified1==[] or specified2==[]:
        return False
    else:
        if specified1==specified2:
            return True
        else:
            return False


def entire_check(target,cursor1,cursor2,dbname1,dbname2,ignore_schema='',choose_schema=''):
    
    count=0
    
#     If choose_schema=='' and ignore_schema=='', then we check entire database.
#     If choose_schema=='' and ignore_schema!='', then we check entire database except specified one schema.
    if choose_schema=='':
        S_schema,judge=database_structure_check(cursor1,cursor2,dbname1,dbname2,ignore_schema)
        
        if not judge:
            count=count+1
            
        for s_s in S_schema:
            t=[[s_s]]
            S_schema_table,judge=schema_structure_check(target,cursor1,cursor2,dbname1,dbname2,t)
            
            if not judge:
                count=count+1
                
            for s_t in S_schema_table:
                if not specified_check(cursor1,cursor2,dbname1,dbname2,t,choose_function=s_t[0]):
                    print('The %r \'%s.%s\' in those two databases are not match\n'%(target,t,s_t[0]))
                    count=count+1
    
#     If choose_schema!='', then we check specified one schema.
    elif choose_schema!='':
        t=[[choose_schema]]
        
        S_schema_table,judge=schema_structure_check(target, cursor1, cursor2, dbname1, dbname2, t)
        
        if not judge:
            count=count+1
            
        for s_t in S_schema_table:
            if not specified_check(cursor1,cursor2,dbname1,dbname2,t,choose_function=s_t[0]):
                print('The %r \'%s.%s\' in those two databases are not match\n'%(target,t,s_t[0]))
                count=count+1
        
    if count==0:
        print('The %r of those two databases are completely matching\n'%target)
    else:
        print('The %r of those two databases are not completely matching\n'%target)
           
    
                                                
if __name__=='__main__':
    main(sys.argv[1:])