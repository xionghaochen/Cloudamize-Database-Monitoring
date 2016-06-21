'''
Created on Jun 20, 2016

@author: walter
'''
#!/usr/bin/env python3
# -*- coding:utf-8 -*-

' The first Python project '

__author__ = 'Walter Xiong'

import sys
import getopt
import psycopg2
import argparse
from difflib import Differ

def main(argv):
    parser = argparse.ArgumentParser()
    
    parser.add_argument('--host1')
    parser.add_argument('--dbname1')
    parser.add_argument('--port1')
    parser.add_argument('--user1')
    parser.add_argument('--password1')
    parser.add_argument('--host2')
    parser.add_argument('--dbname2')
    parser.add_argument('--port2')
    parser.add_argument('--user2')
    parser.add_argument('--password2')
    parser.add_argument('--target')
    parser.add_argument('--ignore_schema',nargs='*')
    parser.add_argument('--ignore_table',nargs='*')
    parser.add_argument('--ignore_function',nargs='*')
    parser.add_argument('--ignore_view',nargs='*')
    parser.add_argument('--choose_schema',nargs='*')
    parser.add_argument('--choose_table')
    parser.add_argument('--choose_function')
    parser.add_argument('--choose_view')

    args = parser.parse_args()
    
    host1=args.host1
    dbname1=args.dbname1
    port1=args.port1
    user1=args.user1
    password1=args.password1
    host2=args.host2
    dbname2=args.dbname2
    port2=args.port2
    user2=args.user2
    password2=args.password2
    target=args.target
    if args.ignore_schema==None:
        ignore_schema=''
    else:
        ignore_schema=args.ignore_schema
    
    if args.ignore_table==None:
        ignore_table=''
    else:
        ignore_table=args.ignore_table
        
    if args.ignore_function==None:
        ignore_function=''
    else:
        ignore_function=args.ignore_function
        
    if args.ignore_view==None:
        ignore_view=''
    else:
        ignore_view=args.ignore_view
        
    if args.choose_schema==None:
        choose_schema=''
    else:
        choose_schema=args.choose_schema
        
    if args.choose_table==None:
        choose_table=''
    else:
        choose_table=args.choose_table
        
    if args.choose_function==None:
        choose_function=''
    else:
        choose_function=args.choose_function
        
    if args.choose_view==None:
        choose_view=''
    else:
        choose_view=args.choose_view
                
    conn_string1= "host=%s dbname=%s port=%s user=%s password=%s"%(host1,dbname1,port1,user1,password1)
    conn1=psycopg2.connect(conn_string1)
    cursor1=conn1.cursor()
    
    conn_string2= "host=%s dbname=%s port=%s user=%s password=%s"%(host2,dbname2,port2,user2,password2)
    conn2=psycopg2.connect(conn_string2)
    cursor2=conn2.cursor()
        
    if target=='table':
        condition_table=table(cursor1,cursor2,dbname1,dbname2,host1,host2,port1,port2,target,ignore_schema,choose_schema,ignore_table,choose_table)
        if choose_schema!='' and choose_table!='':
            condition_table.specified_exist_check()
        else:
            condition_table.database_schema_check()
    elif target=='function':
        condition_function=function_view(cursor1,cursor2,dbname1,dbname2,host1,host2,port1,port2,target,ignore_schema,choose_schema,ignore_function,choose_function,ignore_view,choose_view)
        if choose_schema!='' and choose_function!='':
            condition_function.specified_exist_check()
        else:
            condition_function.entire_check()
    elif target=='view':
        condition_view=function_view(cursor1,cursor2,dbname1,dbname2,host1,host2,port1,port2,target,ignore_schema,choose_schema,ignore_function,choose_function,ignore_view,choose_view)
        if choose_schema!='' and choose_view!='':
            condition_view.specified_exist_check()
        else:
            condition_view.entire_check()

class base(object):
    
    def __init__(self,cursor1,cursor2,dbname1,dbname2,host1,host2,port1,port2,ignore_schema,choose_schema):
        self.cursor1=cursor1
        self.cursor2=cursor2
        self.dbname1=dbname1
        self.dbname2=dbname2
        self.host1=host1
        self.host2=host2
        self.port1=port1
        self.port2=port2
        self.ignore_schema=ignore_schema
        self.choose_schema=choose_schema
        
    def database_structure_check(self):
        
        self.cursor1.execute('select schema_name from information_schema.schemata where schema_name <> \'information_schema\' and schema_name !~ E\'^pg_\';')
        s_schema=self.cursor1.fetchall()
        
        self.cursor2.execute('select schema_name from information_schema.schemata where schema_name <> \'information_schema\' and schema_name !~ E\'^pg_\';')
        c_schema=self.cursor2.fetchall()
        
        if self.ignore_schema!='':
            for i in self.ignore_schema:
                if (i,) not in s_schema:
                    print(' * [Host: %s][Port: %s][Database: %s]There is no \'schema\' %r\n'%(self.host1,self.port1,self.dbname1,i))
                else:
                    s_schema.remove((i,))
                if (i,) not in c_schema:
                    print(' * [Host: %s][Port: %s][Database: %s]There is no \'schema\' %r\n'%(self.host2,self.port2,self.dbname2,i))
                else:
                    c_schema.remove((i,))
                    
        if self.choose_schema!='':
            for c in self.choose_schema:
                if (c,) not in s_schema:
                    print(' * [Host: %s][Port: %s][Database: %s]There is no \'schema\' %r'%(self.host1,self.port1,self.dbname1,c))
                    self.choose_schema.remove(c)
                if (c,) not in c_schema:
                    print(' * [Host: %s][Port: %s][Database: %s]There is no \'schema\' %r'%(self.host2,self.port2,self.dbname2,c))
                    self.choose_schema.remove(c)
            if self.choose_schema!='':
                return self.choose_schema, True 
                    
        s_schema_number,c_schema_number,schema_count=0,0,0
         
        if len(s_schema)!=len(c_schema):
            schema_count=schema_count+1
            print(' ** The number of schemas in those two databases are not matching\n')
        if len(s_schema)==0 and len(c_schema)==0:
            print(' * There is no \'schema\' in those two databases\n')
        if len(s_schema)==0 and len(c_schema)!=0:
            print(' * [Host: %s][Port: %s][Database: %s]There is no \'schema\'\n'%(self.host1,self.port1,self.dbname1))
        if len(s_schema)!=0 and len(c_schema)==0:
            print(' * [Host: %s][Port: %s][Database: %s]There is no \'schema\'\n'%(self.host2,self.port2,self.dbname2))
            
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
                    print(' * [Host: %s][Port: %s][Database: %s]There is no schema %r\n'%(self.host2,self.port2,self.dbname2,s_schema[s_schema_number]))
                    s_schema_number=s_schema_number+1
                    c_schema_number=0
                elif s_schema[s_schema_number]!=c_schema[c_schema_number] and (c_schema_number+1)>=len(c_schema) and (s_schema_number+1)>=len(s_schema):
                    schema_count=schema_count+1
                    print(' * [Host: %s][Port: %s][Database: %s]There is no schema %r\n'%(self.host2,self.port2,self.dbname2,s_schema[s_schema_number]))
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
                    print(' * [Host: %s][Port: %s][Database: %s]There is no schema %r\n'%(self.host1,self.port1,c_schema[c_schema_number],self.dbname1))
                    c_schema.pop(c_schema_number)
                    s_schema_number=0
                elif c_schema[c_schema_number]!=s_schema[s_schema_number] and (s_schema_number+1)>=len(s_schema) and (c_schema_number+1)>=len(c_schema):
                    schema_count=schema_count+1
                    print(' * [Host: %s][Port: %s][Database: %s]There is no schema %r\n'%(self.host1,self.port1,c_schema[c_schema_number],self.dbname1))
                    c_schema.pop(c_schema_number)
                    s_schema_number,c_schema_number=0,0
                    break
            break
            
        if schema_count==0:
            return c_schema,True
        else:
            print(' *** The structure of \'schema\' in those two databases are not matching\n')
            return c_schema,False

    @staticmethod
    def schema_structure_check(target,cursor1,cursor2,dbname1,dbname2,host1,host2,port1,port2,schema_name):
        
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
                print(' ** The number of %r in schema %r are not matching \n'%(target,s[0]))
                count=count+1
            if len(s_schema_table)==0 and len(c_schema_table)==0:
                print(' * In schema %r, there is no %r in those two databases\n'%(s[0],target))
            if len(s_schema_table)==0 and len(c_schema_table)!=0:
                print(' * [Host: %s][Port: %s][Database: %s]There is no %r in schema %r\n'%(host1,port1,dbname1,target,s[0]))
            if len(s_schema_table)!=0 and len(c_schema_table)==0:
                print(' * [Host: %s][Port: %s][Database: %s]There is no %r in schema %r\n'%(host2,port2,dbname2,target,s[0]))
                
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
                        print(' * [Host: %s][Port: %s][Database: %s]There is no %s %r in schema %r\n'%(host2,port2,dbname2,target,s_schema_table[s_schema_table_number][0],s[0]))
                        s_schema_table_number=s_schema_table_number+1
                        c_schema_table_number=0
                    elif s_schema_table[s_schema_table_number]!=c_schema_table[c_schema_table_number] and (c_schema_table_number+1)>=len(c_schema_table) and (s_schema_table_number+1)>=len(s_schema_table):
                        schema_table_count=schema_table_count+1
                        print(' * [Host: %s][Port: %s][Database: %s]There is no %s %r in schema %r\n'%(host2,port2,dbname2,target,s_schema_table[s_schema_table_number][0],s[0]))
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
                        print(' * [Host: %s][Port: %s][Database: %s]There is no %s %r in schema %r\n'%(host1,port1,dbname1,target,c_schema_table[c_schema_table_number][0],s[0]))
                        c_schema_table.pop(c_schema_table_number)
                        s_schema_table_number=0
                    elif c_schema_table[c_schema_table_number]!=s_schema_table[s_schema_table_number] and (s_schema_table_number+1)>=len(s_schema_table) and (c_schema_table_number+1)>=len(c_schema_table):
                        schema_table_count=schema_table_count+1
                        print(' * [Host: %s][Port: %s][Database: %s]There is no %s %r in schema %r\n'%(host1,port1,dbname1,target,c_schema_table[c_schema_table_number][0],s[0]))
                        c_schema_table.pop(c_schema_table_number)
                        s_schema_table_number,c_schema_table_number=0,0
                        break
                if schema_table_count!=0:
                    count=count+1
                    break
                else:
                    break
            
            if count==0:
                return c_schema_table,True
            else:
                return c_schema_table,False
            
class table(base):
    
    def __init__(self,cursor1,cursor2,dbname1,dbname2,host1,host2,port1,port2,target,ignore_schema,choose_schema,ignore_table,choose_table):
        self.cursor1=cursor1
        self.cursor2=cursor2
        self.dbname1=dbname1
        self.dbname2=dbname2
        self.host1=host1
        self.host2=host2
        self.port1=port1
        self.port2=port2
        self.target=target
        self.ignore_schema=ignore_schema
        self.choose_schema=choose_schema
        self.ignore_table=ignore_table
        self.choose_table=choose_table

    def database_schema_check(self):
        
        count=0
        
        if self.choose_schema=='':
            
            S_schema,judge=self.database_structure_check()
            
            if not judge:
                count=count+1
                
            for s_s in S_schema:
                t=[s_s]
                
                S_schema_table,judge=self.schema_structure_check(self.target,self.cursor1,self.cursor2,self.dbname1,self.dbname2,self.host1,self.host2,self.port1,self.port2,t)
                
                for i in self.ignore_table:
                    if (i,) in S_schema_table:
                        S_schema_table.remove((i,))
                
                if not judge:
                    count=count+1
                    
                for s_t in S_schema_table:
                    if not self.table_schema_compare(s_s[0],s_t[0],self.cursor1,self.cursor2,self.host1,self.host2,self.port1,self.port2,self.dbname1,self.dbname2):
                        count=count+1
                        
            
            if count==0:
                print(' *** The schema of \'table\' in those two databases are completely matching\n')
            else:
                print(' *** The schema of \'table\' in those two databases are not completely matching\n')
             
        elif self.choose_schema!='':
            
            S_schema,judge=self.database_structure_check()
            
            if not judge:
                count=count+1
                
            for s_s in S_schema:
                
                t=[(s_s,),]
                 
                S_schema_table,judge=self.schema_structure_check(self.target,self.cursor1,self.cursor2,self.dbname1,self.dbname2,self.host1,self.host2,self.port1,self.port2,t)
                 
                for i in self.ignore_table:
                    if (i,) in S_schema_table:
                        S_schema_table.remove((i,))
     
                if not judge:
                    count=count+1
                     
                for s_t in S_schema_table:
                    if not self.table_schema_compare(s_s,s_t[0],self.cursor1,self.cursor2,self.host1,self.host2,self.port1,self.port2,self.dbname1,self.dbname2):
                        count=count+1
                     
                if count==0:
                    print(' *** In schema %r, the schema of \'table\' in those two databases are completely matching\n'%self.choose_schema)
                else:
                    print(' *** In schema %r, the schema of \'table\' in those two databases are not completely matching\n'%self.choose_schema)
     
    @staticmethod            
    def table_schema_compare(choose_schema,choose_table,cursor1,cursor2,host1,host2,port1,port2,dbname1,dbname2):
        
        cursor1.execute('select count(*) from information_schema.columns where table_schema=\'%s\' and table_name=\'%s\';'%(choose_schema[0],choose_table))
        s_columns=cursor1.fetchall()
        
        cursor1.execute('select column_name,data_type from information_schema.columns where table_schema=\'%s\' and table_name=\'%s\';'%(choose_schema[0],choose_table))
        s_columns_infor=cursor1.fetchall()
        
        cursor2.execute('select count(*) from information_schema.columns where table_schema=\'%s\' and table_name=\'%s\';'%(choose_schema[0],choose_table))
        c_columns=cursor2.fetchall()
        
        cursor2.execute('select column_name,data_type from information_schema.columns where table_schema=\'%s\' and table_name=\'%s\';'%(choose_schema[0],choose_table))
        c_columns_infor=cursor2.fetchall()
                
        x,i,count,sign=0,0,0,0

        if c_columns!=s_columns:
            sign=sign+1
            print(' ** For table \'%s.%s\', the number of columns are not matching in those two databases!\n'%(choose_schema[0],choose_table))
        
        while count<len(c_columns_infor[0]):
            if s_columns_infor[x][count]==c_columns_infor[i][count] and (count+1)<len(c_columns_infor[0]):
                count=count+1
            elif s_columns_infor[x][count]==c_columns_infor[i][count] and (count+1)>=len(c_columns_infor[0]):
                if (x+1)<len(s_columns_infor):
                    x=x+1
                    i=0
                    count=0
                else:
                    break
            elif s_columns_infor[x][count]!=c_columns_infor[i][count]:
                if (i+1)<len(c_columns_infor):
                    i=i+1
                    count=0
                else:
                    print(' * [Host: %s][Port: %s][Database: %s]In table \'%s.%s\', there is no match column '%(host1,port1,dbname1,choose_schema[0],choose_table),s_columns_infor[x],'\n')
                    sign=sign+1
                    if (x+1)<len(s_columns_infor):
                        x=x+1
                        i=0
                        count=0
                    else:
                        break
                    
        x,i,count=0,0,0
         
        while count<len(c_columns_infor[0]):
            if s_columns_infor[x][count]==c_columns_infor[i][count] and (count+1)<len(c_columns_infor[0]):
                count=count+1
            elif s_columns_infor[x][count]==c_columns_infor[i][count] and (count+1)>=len(c_columns_infor[0]):
                if (i+1)<len(c_columns_infor):
                    i=i+1
                    x=0
                    count=0
                else:
                    break
            elif s_columns_infor[x][count]!=c_columns_infor[i][count]:
                if (x+1)<len(s_columns_infor):
                    x=x+1
                    count=0
                else:
                    print(' * [Host: %s][Port: %s][Database: %s]In table \'%s.%s\', there is no match column '%(host2,port2,dbname2,choose_schema[0],choose_table),c_columns_infor[i],'\n')
                    sign=sign+1
                    if (i+1)<len(c_columns_infor):
                        i=i+1
                        x=0
                        count=0
                    else:
                        break
        if sign==0:
            return True
        else:
            print(' * The schema of table \'%s.%s\' in those two databases are not completely matching\n'%(choose_schema[0],choose_table))
            return False

    def specified_exist_check(self):
        
        self.cursor1.execute('SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = \'%s\' AND table_name = \'%s\');'%(self.choose_schema[0],self.choose_table))
        db1=self.cursor1.fetchone()
        
        self.cursor2.execute('SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = \'%s\' AND table_name = \'%s\');'%(self.choose_schema[0],self.choose_table))
        db2=self.cursor2.fetchone()

        if  db1[0]==True and db2[0]==True:
            self.table_content_check()
        else:
            if db1[0]==True and db2[0]==False:
                print(' * [Host: %s][Port: %s][Database: %s]There is no table \'%s.%s\''%(self.host2,self.port2,self.dbname2,self.choose_schema[0],self.choose_table))
            elif db2[0]==True and db1[0]==False:
                print(' * [Host: %s][Port: %s][Database: %s]There is no table \'%s.%s\''%(self.host1,self.port1,self.dbname1,self.choose_schema[0],self.choose_table))
            elif db1[0]==False and db2[0]==False:
                print(' * The table \'%s.%s\' can not be found in those two databases'%(self.choose_schema[0],self.choose_table))
                
    def table_content_check(self):
            
        if not self.table_schema_compare(self.choose_schema,self.choose_table,self.cursor1,self.cursor2,self.host1,self.host2,self.port1,self.port2,self.dbname1,self.dbname2):
            print(' *** Due to the different table schema in \'%s.%s\', we cannot process the content check\n'%(self.choose_schema[0],self.choose_table))
        else:
            self.table_content_compare()
            
    def table_content_compare(self):
        
        self.cursor1.execute('select * from %s.%s'%(self.choose_schema,self.choose_table))
        s_colnames = [desc[0] for desc in self.cursor1.description]
        
        self.cursor1.execute('select * from %s.%s'%(self.choose_schema,self.choose_table))
        s_data=self.cursor1.fetchall()
        
        self.cursor2.execute('select * from %s.%s'%(self.choose_schema,self.choose_table))
        c_colnames = [desc[0] for desc in self.cursor2.description]
        
        self.cursor2.execute('select * from %s.%s'%(self.choose_schema,self.choose_table))
        c_data=self.cursor2.fetchall()
    
        
        x,i,sign,s_count,c_count=0,0,0,0,0
        
        if len(s_data)!=len(c_data):
            sign=sign+1
            print('\n ** The number of records in table \'%s.%s\' are not matching'%(self.choose_schema,self.choose_table))
        
        if len(s_colnames)==1 and s_colnames[0]=='id':
            sign=sign+1
            print('\n * [Host: %s][Port: %s][Database: %s]The table \'%s.%s\' only has one column (id)'%(self.host1,self.port1,self.dbname1,self.choose_schema,self.choose_table))
            return   
        elif len(c_colnames)==1 and c_colnames[0]=='id':
            sign=sign+1
            print('\n * [Host: %s][Port: %s][Database: %s]The table \'%s.%s\' only has one column (id)'%(self.host2,self.port2,self.dbname2,self.choose_schema,self.choose_table))
            return
        else:
            for s in s_colnames:
                if s=='id':
#                     We assume the primary key always in the first place
                    s_count=1
                    break
                else:
                    s_count=0
            for c in c_colnames:
                if c=='id':
                    c_count=1
                    break
                else:
                    c_count=0
    
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
                            print('\n * [Host: %s][Port: %s][Database: %s]There is no matching record: %r'%(self.host2,self.port2,self.dbname2,s_data[x]))
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
                s_count,c_count=0,0
                for s in s_colnames:
                    if s=='id':
    #                     We assume the primary key always in the first place
                        s_count=1
                        break
                    else:
                        s_count=0
                for c in c_colnames:
                    if c=='id':
                        c_count=1
                        break
                    else:
                        c_count=0
            else:
                break
        
        x,i=0,0
        
        for s in s_colnames:
            if s=='id':
#                     We assume the primary key always in the first place
                s_count=1
                break
            else:
                s_count=0
        for c in c_colnames:
            if c=='id':
                c_count=1
                break
            else:
                c_count=0

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
                            print('\n * [Host: %s][Port: %s][Database: %s]There is no matching record: %r'%(self.host1,self.port1,self.dbname1,c_data[i]))
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
                for s in s_colnames:
                    if s=='id':
    #                     We assume the primary key always in the first place
                        s_count=1
                        break
                    else:
                        s_count=0
                for c in c_colnames:
                    if c=='id':
                        c_count=1
                        break
                    else:
                        c_count=0
            else:
                break
        
        if sign==0:
            print('\n *** The table \'%s.%s\' in those two databases are totally matching'%(self.choose_schema,self.choose_table))
        else:
            print('\n *** The table \'%s.%s\' in those two databases are not totally matching'%(self.choose_schema,self.choose_table))
    
class function_view(base):
    
    def __init__(self,cursor1,cursor2,dbname1,dbname2,host1,host2,port1,port2,target,ignore_schema,choose_schema,ignore_function,choose_function,ignore_view,choose_view):
        self.cursor1=cursor1
        self.cursor2=cursor2
        self.dbname1=dbname1
        self.dbname2=dbname2
        self.host1=host1
        self.host2=host2
        self.port1=port1
        self.port2=port2
        self.target=target
        self.ignore_schema=ignore_schema
        self.choose_schema=choose_schema
        self.ignore_function=ignore_function
        self.choose_function=choose_function
        self.ignore_view=ignore_view
        self.choose_view=choose_view
        
    def entire_check(self):
        
        count=0
        
    #     If self.choose_schema=='' and self.ignore_schema=='', then we check entire database.
    #     If self.choose_schema=='' and self.ignore_schema!='', then we check entire database except specified one schema.
        if self.choose_schema=='':
            S_schema,judge=self.database_structure_check()
            
            if not judge:
                count=count+1
                
            for s_s in S_schema:
                t=[s_s]
                S_schema_table,judge=self.schema_structure_check(self.target,self.cursor1,self.cursor2,self.dbname1,self.dbname2,self.host1,self.host2,self.port1,self.port2,t)
                
                if self.ignore_function!='':
                    for i in self.ignore_function:
                        if (i,) in S_schema_table:
                            S_schema_table.remove((i,))

                if self.ignore_view!='':
                    for i in self.ignore_view:
                        if (i,) in S_schema_table:
                            S_schema_table.remove((i,))                

                if not judge:
                    count=count+1
                    
                for s_t in S_schema_table:
                    if self.target=='function':
                        if not self.specified_check(self.target,self.cursor1,self.cursor2,self.dbname1,self.dbname2,self.host1,self.host2,self.port1,self.port2,s_s[0],choose_function=s_t[0]):
                            print(' * The %r \'%s.%s\' in those two databases are not match\n'%(self.target,s_s[0],s_t[0]))
                            count=count+1
                    elif self.target=='view':
                        if not self.specified_check(self.target,self.cursor1,self.cursor2,self.dbname1,self.dbname2,self.host1,self.host2,self.port1,self.port2,s_s[0],choose_view=s_t[0]):
                            print(' * The %r \'%s.%s\' in those two databases are not match\n'%(self.target,s_s[0],s_t[0]))
                            count=count+1
        
    #     If self.choose_schema!='', then we check specified one schema.
        elif self.choose_schema!='':
            
            S_schema,judge=self.database_structure_check()
            
            if not judge:
                count=count+1
                
            for s_s in S_schema:
                t=[(s_s,),]
                
                S_schema_table,judge=self.schema_structure_check(self.target, self.cursor1, self.cursor2, self.dbname1, self.dbname2,self.host1,self.host2,self.port1,self.port2, t)
                if self.ignore_function!='':
                    for i in self.ignore_function:
                        if (i,) in S_schema_table:
                            S_schema_table.remove((i,))
    
                if self.ignore_view!='':
                    for i in self.ignore_view:
                        if (i,) in S_schema_table:
                            S_schema_table.remove((i,))                
    
                if not judge:
                    count=count+1
                    
                for s_t in S_schema_table:
                    if self.target=='function':
                        if not self.specified_check(self.target,self.cursor1,self.cursor2,self.dbname1,self.dbname2,self.host1,self.host2,self.port1,self.port2,s_s,choose_function=s_t[0]):
                            print(' * The %r \'%s.%s\' in those two databases are not match\n'%(self.target,s_s,s_t[0]))
                            count=count+1
                    elif self.target=='view':
                        if not self.specified_check(self.target,self.cursor1,self.cursor2,self.dbname1,self.dbname2,self.host1,self.host2,self.port1,self.port2,s_s,choose_view=s_t[0]):
                            print(' * The %r \'%s.%s\' in those two databases are not match\n'%(self.target,s_s,s_t[0]))
                            count=count+1
                
        if count==0:
            print(' *** The schema of %r in those two databases are completely matching\n'%self.target)
        else:
            print(' *** The schema of %r in those two databases are not completely matching\n'%self.target)
    
    @staticmethod        
    def specified_check(target,cursor1,cursor2,dbname1,dbname2,host1,host2,port1,port2,choose_schema,choose_function='',choose_view=''):
        
        if choose_function!='':
            cursor1.execute('SELECT routine_definition FROM information_schema.routines WHERE specific_schema LIKE \'%s\' AND routine_name LIKE \'%s\';'%(choose_schema,choose_function))
            specified1=cursor1.fetchall()
            
            cursor2.execute('SELECT routine_definition FROM information_schema.routines WHERE specific_schema LIKE \'%s\' AND routine_name LIKE \'%s\';'%(choose_schema,choose_function))
            specified2=cursor2.fetchall()
            
        if choose_view!='':
            cursor1.execute('select definition from pg_views where viewname = \'%s\' and schemaname=\'%s\';'%(choose_view,choose_schema))
            specified1=cursor1.fetchall()
            
            cursor2.execute('select definition from pg_views where viewname = \'%s\' and schemaname=\'%s\';'%(choose_view,choose_schema))
            specified2=cursor2.fetchall()
            
        if specified1==[]:
            print(' * [Host: %s][Port: %s][Database: %s]There is no %r \'%s.%s%s\'\n'%(host1,port1,dbname1,target,choose_schema,choose_view,choose_function))
            return False
        elif specified2==[]:
            print(' * [Host: %s][Port: %s][Database: %s]There is no %r \'%s.%s%s\'\n'%(host2,port2,dbname2,target,choose_schema,choose_view,choose_function))
            return False
        else:
            if specified1==specified2:
                return True
            else:
                print(' * [Type: %s][Name: %s%s]Detail: \n'%(target,choose_function,choose_view))
                l1=specified1[0][0].splitlines(keepends=True)
                l2=specified2[0][0].splitlines(keepends=True)
                dif=list(Differ().compare(l1,l2))
                print(" ".join(dif))
                print('')
                return False
            
    def specified_exist_check(self):
        
        if self.choose_function!='':
            self.cursor1.execute('select exists(select 1 from information_schema.routines where specific_schema like \'%s\' and routine_name like \'%s\');'%(self.choose_schema[0],self.choose_function))
            db1=self.cursor1.fetchone()
            
            self.cursor2.execute('select exists(select 1 from information_schema.routines where specific_schema like \'%s\' and routine_name like \'%s\');'%(self.choose_schema[0],self.choose_function))
            db2=self.cursor2.fetchone()
    
            if  db1[0]==True and db2[0]==True:
                if self.specified_check(self.target,self.cursor1,self.cursor2,self.dbname1,self.dbname2,self.host1,self.host2,self.port1,self.port2,self.choose_schema[0],choose_function=self.choose_function):
                    print(' * In schema %r, the defination of function %r in those two databases are matching\n'%(self.choose_schema[0],self.choose_function))
                else:
                    print(' * In schema %r, the defination of function %r in those two databases are not matching\n'%(self.choose_schema[0],self.choose_function))
            else:
                if db1[0]==True and db2[0]==False:
                    print(' * [Host: %s][Port: %s][Database: %s]There is no function \'%s.%s\'\n'%(self.host2,self.port2,self.dbname2,self.choose_schema[0],self.choose_function))
                elif db2[0]==True and db1[0]==False:
                    print(' * [Host: %s][Port: %s][Database: %s]There is no function \'%s.%s\'\n'%(self.host1,self.port1,self.dbname1,self.choose_schema[0],self.choose_function))
                elif db1[0]==False and db2[0]==False:
                    print(' * The function \'%s.%s\' can not be found in those two databases\n'%(self.choose_schema[0],self.choose_function))
    
        elif self.choose_view!='':
            print('self.choose_schema=',self.choose_schema)
            print('self.choose_view=',self.choose_view)
            self.cursor1.execute('select exists(select 1 from pg_views where schemaname=\'%s\' and viewname=\'%s\');'%(self.choose_schema[0],self.choose_view))
            db1=self.cursor1.fetchone()
            
            self.cursor2.execute('select exists(select 1 from pg_views where schemaname=\'%s\' and viewname=\'%s\');'%(self.choose_schema[0],self.choose_view))
            db2=self.cursor2.fetchone()
    
            if  db1[0]==True and db2[0]==True:
                if self.specified_check(self.target,self.cursor1,self.cursor2,self.dbname1,self.dbname2,self.host1,self.host2,self.port1,self.port2,self.choose_schema[0],choose_view=self.choose_view):
                    print(' * In schema %r, the defination of view %r in those two databases are matching\n'%(self.choose_schema[0],self.choose_view))
                else:
                    print(' * In schema %r, the defination of view %r in those two databases are not matching\n'%(self.choose_schema[0],self.choose_view))
            else:
                if db1[0]==True and db2[0]==False:
                    print(' * [Host: %s][Port: %s][Database: %s]There is no view \'%s.%s\'\n'%(self.host2,self.port2,self.dbname2,self.choose_schema[0],self.choose_view))
                elif db2[0]==True and db1[0]==False:
                    print(' * [Host: %s][Port: %s][Database: %s]There is no view \'%s.%s\'\n'%(self.host1,self.port1,self.dbname1,self.choose_schema[0],self.choose_view))
                elif db1[0]==False and db2[0]==False:
                    print(' * The view \'%s.%s\' can not be found in those two databases\n'%(self.choose_schema[0],self.choose_view))
                    
if __name__=='__main__':
    main(sys.argv[1:])