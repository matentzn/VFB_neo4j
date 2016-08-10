'''
Created on 9 Aug 2016

Script to import data_sources from lmb mySQL DB to VFBneoKB and hook up these data_sources to channels.
This script can be retired once data is fully migrated from lmb.

Requires ssh tunnel to LMB server

@author: davidos
'''

import pymysql
import sys
from ..tools import neo4j_connect


nc = neo4j_connect(base_uri=sys.argv[1], usr=sys.argv[2], pwd=sys.argv[3])
c = pymysql.connect(host='localhost', user='flycircuit', db='flycircuit', 
                    cursorclass=pymysql.cursors.DictCursor, port = 3307, 
                    charset='utf8mb4',
                    password = 'flycircuit')

cursor = c.cursor()
cursor.execute("SELECT * FROM data_source")
statements = []
for d in cursor.fetchall():
    statement = "MERGE (d:data_source { name: '%s' }) " % d['name']
    if d['pub_pmid']:
        statement += "MERGE (d)-[:has_reference]->(:pub { PMID: '%s' } )" % d['pub_pmid']
    statements.append(statement)
    statement = ''
    statement = "MATCH (d:data_source { name: '%s' }) " % d['name']
    if d['dataset_link']:
        statement += "SET d.dataset_link = '%s' " % d['dataset_link']
    if d['data_link_pre']:
        statement += "SET d.data_link_pre = '%s' " % d['data_link_pre']
    if d['data_link_post']:
        statement += "SET d.data_link_post = '%s' " % d['data_link_post']
    if d['dataset_link']:
        statement += "SET d.dataset_link =  '%s' " % d['dataset_link']
nc.commit_list(statements)

        
cursor.execute("SELECT oi.shortFormID, ds.name FROM owl_individual oi " \
               "JOIN data_source ds ON oi.source_id = ds.id " \
               "WHERE oi.shortFormID like 'VFBi\_%'")

statements = []

for d in cursor.fetchall():
    statements.append("MATCH (c:Individual { short_form : '%s' }) " \
                      "MERGE (c)-[:has_source]->(ds:data_source { name : '%s'})" 
                      % (d['shortFormID'], d['name']))

nc.commit_list_in_chunks(statements, chunk_length = 1000)
cursor.close()
c.close()

