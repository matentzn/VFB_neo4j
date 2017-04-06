'''
Created on 9 Aug 2016

Script to import data_sources from lmb mySQL DB to VFBneoKB and hook up these data_sources to channels.
This script can be retired once data is fully migrated from lmb.

Requires ssh tunnel to LMB server

@author: davidos
'''

import pymysql
import sys
from uk.ac.ebi.vfb.neo4j.neo4j_tools import neo4j_connect
from uk.ac.ebi.vfb.neo4j.lmb2neoKB.lmb_query_tools import get_conn



nc = neo4j_connect(base_uri=sys.argv[1], usr=sys.argv[2], pwd=sys.argv[3])
c = get_conn(sys.argv[4], sys.argv[4])

statements.append("CREATE CONSTRAINT ON (ds:data_source) ASSERT ds.name IS UNIQUE")
statements.append("CREATE CONSTRAINT ON (p:pub) ASSERT p.PMID IS UNIQUE")
nc.commit_list(statements)

cursor = c.cursor()
cursor.execute("SELECT * FROM data_source")
statements = []
for d in cursor.fetchall():
    statement = "MERGE (d:data_source { name: '%s' }) " % d['name']
    if d['pub_pmid']:
        statement += "MERGE (p:pub { PMID: '%s' } ) " \
        "MERGE (d)-[:has_reference]->(p)" % d['pub_pmid']
    statements.append(statement)
    if d['dataset_link'] or d['data_link_pre'] or d['data_link_post'] or d['dataset_link']:
        statement = "MATCH (d:data_source { name: '%s' }) " % d['name']
    if d['dataset_link']:
        statement += "SET d.dataset_link = '%s' " % d['dataset_link']
    if d['data_link_pre']:
        statement += "SET d.data_link_pre = '%s' " % d['data_link_pre']
    if d['data_link_post']:
        statement += "SET d.data_link_post = '%s' " % d['data_link_post']
    if d['dataset_link']:
        statement += "SET d.dataset_link =  '%s' " % d['dataset_link']
    statements.append(statement)
nc.commit_list(statements)




        
cursor.execute("SELECT oi.shortFormID, ds.name FROM owl_individual oi " \
               "JOIN data_source ds ON oi.source_id = ds.id " \
               "WHERE oi.shortFormID like 'VFBc\_%'")

# statements = []
# 
# for d in cursor.fetchall():
#     statements.append("MATCH (c:Individual { short_form : '%s' }), (ds:data_source { name : '%s'})" \
#                       "MERGE (c)-[:has_source]->(ds)" 
#                       % (d['shortFormID'], d['name']))
# 
# nc.commit_list_in_chunks(statements, chunk_length = 1000)
cursor.close()
c.close()

