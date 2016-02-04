import sys
from .dbtools import dict_cursor, get_fb_conn
from ..tools import neo4j_connect

base_uri = sys.argv[1]
usr = sys.argv[2]
pwd = sys.argv[3]

nc = neo4j_connect(base_uri, usr, pwd)
statements = ['MATCH (pub) RETURN DISTINCT pub.FLYBASE']
pub_list_results = nc.commit_list(statements)
pub_list = [] # Parsing returned Json for results.

c = get_fb_conn()
cursor=c.cursor()

# Query DB for all pubs.

# Get basic attributes

# Will be slow to iterate over pub list and make separate queries for each.  Better to use IN CLAUSE?  Could chunk first.

def set_pub_att(k,v):
        statements.append("MATCH (p:pub) WHERE pub.FLYBASE = '%s' ")


for c in pub_list:
    cursor.execute("SELECT pub.title, pub.miniref, pub.pyear, pub.pages, " \
                    "pub.volume, type.name, pub.uniquename as fbrf " \
                    "FROM pub JOIN cvterm typ on typ.cvterm_id = pub.type_id " \
                    "WHERE fbrf = %s", c)

    dc = dict_cursor(cursor)
    statements = []
    for d in dc:
        statements.append("MATCH (p:pub) WHERE pub.FLYBASE = '%s' " \
                          "SET p.title = '%s', p.miniref = '%s', " \
                          "p.volume = '%s', p.year = '%s', " \
                          "p.pages" % (d['fbrf'], d['title'], d['miniref'], d['volume'], d['year'], d['pages']))
        

        cursor.execute("SELECT db.name AS db_name, dbx.accession AS acc FROM pub " \
                        "JOIN pub_dbxref pdbx on pdbx.pub_id=pub.pub_id" \
                        "JOIN dbxref dbx on pdbx.dbxref_id=dbx.dbxref_id " \
                        "JOIN db on dbx.db_id=db.db_id " \
                        "WHERE pub.uniquename= '%s'", c) 
        
        dc = dict_cursor(cursor)
        
        for d in dc: 
            if d['name'] == 'pubmed':
                statements.append("MATCH (p:pub) WHERE pub.FLYBASE = '%s' " \
                          "SET p.PMID = '%s'" % (d['acc']))
            if d['name'] == 'PMCID':
                statements.append("MATCH (p:pub) WHERE pub.FLYBASE = '%s' " \
                          "SET p.PMID = '%s'" % (d['acc']))
            if d['name'] == 'pubmed':
                statements.append("MATCH (p:pub) WHERE pub.FLYBASE = '%s' " \
                          "SET p.PMID = '%s'" % (d['acc']))
            if d['name'] == 'pubmed':
                statements.append("MATCH (p:pub) WHERE pub.FLYBASE = '%s' " \
                          "SET p.PMID = '%s'" % (d['acc']))
        
        nc.commit_list_in_chunks(statements, verbose = True, chunk_length = 1000)
