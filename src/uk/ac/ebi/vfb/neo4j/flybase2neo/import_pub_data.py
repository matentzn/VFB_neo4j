import sys
from uk.ac.ebi.vfb.neo4j.flybase2neo.fb_tools import dict_cursor, get_fb_conn
from uk.ac.ebi.vfb.neo4j.neo4j_tools import neo4j_connect
import re

"""Populate pub data.  Should be run as a final step, once all content added."""

## TODO: Add microrefs - for more compact views (P1)
## TODO: Add pub types (P2)
## TODO: Add authors (P3)
## TODO: Add pub relationships (P3)

base_uri = sys.argv[1]
usr = sys.argv[2]
pwd = sys.argv[3]

nc = neo4j_connect(base_uri, usr, pwd)

# Pull all pub FBrfs from graph
statements = ['MATCH (pub) RETURN DISTINCT pub.FlyBase']
pub_list_results = nc.commit_list(statements)
pub_list = [str(x['row'][0]) for x in pub_list_results[0]['data']] # Parsing returned Json for results.

c = get_fb_conn()
cursor=c.cursor()

def gen_micro_ref_from_miniref(miniref):
    # Use regex to truncate after year, remove brackets.
    
    return

def gen_micro_ref_from_db():
    # Use author list + year.  
    # if > 2 authors, use et al
    return

statements = []    
    # Pull basic pub data
cursor.execute("SELECT pub.title, pub.miniref, pub.pyear, pub.pages, " \
            "pub.volume, typ.name, pub.uniquename as fbrf " \
            "FROM pub JOIN cvterm typ on typ.cvterm_id = pub.type_id " \
            "WHERE pub.uniquename = ANY(%s)", (pub_list,))  # Odd syntax necessary for cast

    
dc = dict_cursor(cursor)
for d in dc:
    if d['title']:
        title = re.sub('"', "\\'", d['title'])
    else:
        title = ''
    statements.append("MATCH (p:pub) WHERE p.FlyBase = '%s' " \
                      "SET p.title = \"%s\", p.miniref = \"%s\", " \
                      "p.volume = '%s', p.year = '%s', p.pages = '%s'" \
                      % (d['fbrf'], title, d['miniref'], d['volume'], d['pyear'], d['pages']))
    # Note on quoting: Double quotes safer for longer text which may have single quotes internally
    # Titles occasionally have double quotes in.  These are escaped via re.sub  

cursor.execute("SELECT pub.uniquename as fbrf, db.name AS db_name, dbx.accession AS acc FROM pub " \
               "JOIN pub_dbxref pdbx on pdbx.pub_id=pub.pub_id " \
               "JOIN dbxref dbx on pdbx.dbxref_id=dbx.dbxref_id " \
               "JOIN db on dbx.db_id=db.db_id " \
               "WHERE pub.uniquename = ANY(%s)", (pub_list,)) 
    
for d in dict_cursor(cursor): 
    if d['db_name'] == 'pubmed':
        statements.append("MATCH (p:pub) WHERE p.FlyBase = '%s' " \
                  "SET p.PMID = '%s'" % (d['fbrf'],d['acc']))
    if d['db_name'] == 'PMCID':
        statements.append("MATCH (p:pub) WHERE p.FlyBase = '%s' " \
                  "SET p.PMCID = '%s'" % (d['fbrf'],d['acc']))
    if d['db_name'] == 'ISBN':
        statements.append("MATCH (p:pub) WHERE p.FlyBase = '%s' " \
                  "SET p.PMID = '%s'" % (d['fbrf'],d['acc']))
    if d['db_name'] == 'DOI':
        statements.append("MATCH (p:pub) WHERE p.FlyBase = '%s' " \
                  "SET p.DOI = '%s'" % (d['fbrf'],d['acc']))
            
        
nc.commit_list_in_chunks(statements, verbose = True, chunk_length = 1)
c.close()
# Ways to extend:  
##  Add authors
##  Add pub relationships, pub types... (via FBcv typing)
