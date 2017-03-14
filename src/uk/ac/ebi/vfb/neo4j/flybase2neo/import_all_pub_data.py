import sys
from uk.ac.ebi.vfb.neo4j.flybase2neo.dbtools import dict_cursor, get_fb_conn
from uk.ac.ebi.vfb.neo4j.neo4j_tools import neo4j_connect
import re

"""Populate pub data.  Should be run as a final step, once all content added."""

## TODO: Add pub relationships (P3)

base_uri = sys.argv[1]
usr = sys.argv[2]
pwd = sys.argv[3]

nc = neo4j_connect(base_uri, usr, pwd)

# Pull all pub FBrfs from graph
statements = ['MATCH (p:pub) RETURN DISTINCT p.FlyBase']
pub_list_results = nc.commit_list(statements)
pub_list = [str(x['row'][0]) for x in pub_list_results[0]['data']]  # Parsing returned Json for results.

c = get_fb_conn()
cursor = c.cursor()


def gen_micro_ref_from_miniref(miniref):
    # Use regex to truncate after year, remove brackets.

    return


def gen_micro_ref_from_db():
    # Use author list + year.  
    # if > 2 authors, use et al
    return

pubs = {}

iri_head = 'http://virtualflybrain.org/reports/'
fb_url = 'http://flybase.org/'
fb_sub = 'reports/'
fb_desc = 'A Database of Drosophila Genes and Genomes'


print('Loading from FB...')

statements = []
statements.append('MERGE (s:site {label:"FlyBase"}) ON CREATE SET s.iri="%s", s.description="%s"' % (fb_url, fb_desc))

# Pull basic pub data
cursor.execute("SELECT pub.title as title, pub.miniref as miniref, pub.pyear as year, pub.pages as pages, " \
               "pub.volume as volume, typ.name as type, pub.uniquename as fbrf, " \
               "db.name AS db_name, dbx.accession AS acc " \
               "FROM pub JOIN cvterm typ on typ.cvterm_id = pub.type_id " \
               "JOIN pub_dbxref pdbx on pdbx.pub_id=pub.pub_id " \
               "JOIN dbxref dbx on pdbx.dbxref_id=dbx.dbxref_id " \
               "JOIN db on dbx.db_id=db.db_id ")

dc = dict_cursor(cursor)
for d in dc:
    if 'FBrf' in d['fbrf']:
        if d['title']:
            title = re.sub("\'", "\\'", d['title'])
            title = re.sub('"', "\\'", title)
        pubs[d['fbrf']]={}
        piri = iri_head + str(d['fbrf'])
        pubs[d['fbrf']].update({"FlyBase":d['fbrf'], "short_form":d['fbrf'],"iri":piri})
        if title:
            pubs[d['fbrf']].update({"title":title})
        for name in ['miniref','volume','year','pages','type']:
            if d[name] and d[name] not in ['', 'null', 'None']:
                pubs[d['fbrf']].update({name: d[name]})
        if d['db_name'] == 'pubmed':
            pubs[d['fbrf']].update({'PMID': d['acc']})
        else:
            pubs[d['fbrf']].update({d['db_name']: d['acc']})


c.close()



# for pub in pubs:
#     if pub not in pub_list:
#         statement = 'MERGE (p:pub {iri:"%s"}) ' % (pubs[pub]['iri'])
#         statement = statement + 'SET '
#         for param in pubs[pub]:
#             statement = statement + r'p.`%s`="%s", ' % (param, pubs[pub][param])
#         statement = statement + '.'
#         statement = statement.replace(', .', '')
#
#         statements.append(statement)
#         print('-', end="")


nc.commit_list_in_chunks(statements, verbose=False, chunk_length=1000)
statements = []


# Add Author Nodes
c = get_fb_conn()
cursor = c.cursor()
print('Loading from FB...')
cursor.execute(
    "SELECT pub.uniquename as fbrf, pa.rank AS rank, pa.surname as surname, pa.givennames as givennames, "
    "pa.pubauthor_id as paid FROM pub " \
    "JOIN pubauthor pa on pa.pub_id=pub.pub_id ") \
    # "WHERE pub.uniquename = ANY(%s)", (pub_list,))

for d in dict_cursor(cursor):
    pub = str(d['fbrf'])
    paid = 'VFBa_' + str(d['paid']).zfill(8)
    pairi = iri_head + paid
    piri = iri_head + pub
    statement = ''
    if pub in pubs.keys():
        for param in pubs[pub]:
            statement = statement + r', p.`%s`="%s"' % (param, pubs[pub][param])
        statement = statement + ' '
    subdomain = fb_sub + pub
    if d['givennames'] and d['givennames'] not in ['', 'null', 'None']:
        statements.append('MERGE (a:person {{iri:"{0}{1}"}}) ' 
                          'ON CREATE SET a.short_form = "{1}", a.name = "{d[surname]}, {d[givennames]}", '
                          'a.surname="{d[surname]}", a.givennames="{d[givennames]}" ' 
                          'WITH a '
                          'MERGE (p:pub {{iri:"{3}{6}{2}"}}) '
                          'ON CREATE SET p.short_form = "{2}"{5}'
                          'WITH a, p '
                          'MERGE (p)-[r:creator {{rank:{d[rank]}}}]->(a) '
                          'WITH p,r,a '
                          'WHERE p.short_form =~ "FB.*" '
                          'MATCH (s:site {{iri:"{3}"}}) '
                          'MERGE (p)-[l:link_to {{subdomain:"{4}"}}]->(s) '.format(iri_head, paid, pub,
                                                                                      fb_url, subdomain,
                                                                                      statement, fb_sub, d=d))
    else:
        statements.append('MERGE (a:person {{iri:"{0}{1}"}}) ' 
                          'ON CREATE SET a.short_form = "{1}", a.name = "{d[surname]}", a.surname="{d[surname]}" ' 
                          'WITH a '
                          'MERGE (p:pub {{iri:"{3}{6}{2}"}}) '
                          'ON CREATE SET p.short_form = "{2}"{5}'
                          'WITH a, p '
                          'MERGE (p)-[r:creator {{rank:{d[rank]}}}]->(a) '
                          'WITH p,r,a '
                          'WHERE p.short_form =~ "FB.*" '
                          'MATCH (s:site {{iri:"{3}"}}) '
                          'MERGE (p)-[l:link_to {{subdomain:"{4}"}}]->(s) '.format(iri_head, paid, pub,
                                                                                      fb_url, subdomain,
                                                                                      statement, fb_sub, d=d))


statements.append('MATCH (n:pub)-[r:creator]->(a:person) '
                  'WHERE has(n.year) AND has(a.surname) '
                  'WITH n, r.rank as rank, a.surname as name ORDER BY rank ASC '
                  'WITH n, collect(name) as authors, COUNT(name) as number '
                  'WITH n, number, authors[0] + ", " + n.year as ref1, '
                  'authors[0] + " and " + authors[1] + ", " + n.year as ref2, '
                  'authors[0] + " et al., " + n.year as ref3 '
                  'WITH n, CASE '
                  'WHEN number=1 THEN ref1 '
                  'WHEN number=2 THEN ref2 '
                  'WHEN number > 2 THEN ref3 '
                  'ELSE null '
                  'END as microref '
                  'SET n.microref=microref')

nc.commit_list_in_chunks(statements, verbose=False, chunk_length=1000)
c.close()
# Ways to extend:  
##  Add authors
##  Add pub relationships, pub types... (via FBcv typing)
