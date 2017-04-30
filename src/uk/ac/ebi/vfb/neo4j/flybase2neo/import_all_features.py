import json

import sys
from uk.ac.ebi.vfb.neo4j.flybase2neo.dbtools import dict_cursor, get_fb_conn, FB2Neo, FeatureRelationship
from uk.ac.ebi.vfb.neo4j.tools import neo4j_connect
import re

# neo connection
base_uri = sys.argv[1]
usr = sys.argv[2]
pwd = sys.argv[3]

nc = neo4j_connect(base_uri, usr, pwd)

statements = []

# fb connection

c = get_fb_conn()
cursor = c.cursor()


def map_feature_type(fbid, ftype):
    mapping = {'transgenic_transposon': 'SO_0000796',
               'insertion_site': 'SO_0001218',
               'transposable_element_insertion_site': 'SO_0001218',
               'natural_transposon_isolate_named': 'SO_0000797',
               'chromosome_structure_variation': 'SO_1000183'
               }
    # see if there is some alternative query method, as classification is on FB site.
    if ftype == 'gene':
        if re.match('FBal', fbid):
            return 'SO_0001023'
        else:
            return 'SO_0000704 '
    elif ftype in mapping.keys():
        return mapping[ftype]
    else:
        return 'SO_0000110'  # Sequence feature

def clean_sgml_tags(sgml_string):
    sgml_string = re.sub('up>', 'sup>', sgml_string)
    sgml_string = re.sub('down>', 'sub>', sgml_string)
    sgml_string = re.sub(r'\\', '&#92;', sgml_string)  # This looks hacky.  Shouldn't we have a generic unicode soln?
    return sgml_string

# site check
iri_head = 'http://virtualflybrain.org/reports/'
fb_url = 'http://flybase.org/'
fb_sub = 'reports/'
fb_desc = 'A Database of Drosophila Genes and Genomes'

statements.append('MERGE (s:site {label:"FlyBase"}) ON CREATE SET s.iri="%s", s.description="%s"' % (fb_url, fb_desc))

# pull all features and synonyms

cursor.execute("SELECT DISTINCT f.uniquename AS fbid, c.name AS ftype, fs.is_current as latest, "
               "s.synonym_sgml as unicode_name, s.name as ascii_name "
               "FROM feature f "
               "LEFT OUTER JOIN cvterm c ON f.type_id=c.cvterm_id "
               "LEFT OUTER JOIN feature_synonym fs ON f.feature_id=fs.feature_id "
               "LEFT OUTER JOIN synonym s on (fs.synonym_id=s.synonym_id) "
               "WHERE c.name in ('transgenic_transposon', "
               "'insertion_site', "
               "'transposable_element_insertion_site', "
               "'natural_transposon_isolate_named', "
               "'chromosome_structure_variation', "
               "'gene') AND NOT f.is_obsolete")

# fbid          ; ftype                 ; latest  ;                    unicode_name                                       ; ascii_name
# "FBtp0016029" ;"transgenic_transposon";    t    ;"P{UAS-comm.P230A}"                                                    ;"P{UAS-comm.P230A}"
# "FBtp0016322" ;"transgenic_transposon";    f    ;"P{Scer\UAS(FRT.CD2)arm-arm.Sev.M<up>-</up>N<up>-</up>.Flu}"           ;"P{Scer\UAS(FRT.CD2)arm-arm.Sev.M[-]N[-].Flu}"
# "FBtp0016322" ;"transgenic_transposon";    t    ;"P{Scer(FRT.CD2,y<up>+</up>)arm.Sev.M<up>-</up>N<up>-</up>.Flu}"       ;"P{Scer(FRT.CD2,y[+])arm.Sev.M[-]N[-].Flu}"

dc = dict_cursor(cursor)
f = {}
for d in dc:
    if d['fbid'] not in f.keys():
        f[d['fbid']] = {'synonyms': []}
    if d['latest']:
        f[d['fbid']].update({'label': clean_sgml_tags(str(d['unicode_name']))})
        f[d['fbid']]['synonyms'].append(clean_sgml_tags(str(d['ascii_name'])))
        f[d['fbid']].update({'type': d['ftype']})
    else:
        if str(d['unicode_name']) not in ['','None','null']:
            f[d['fbid']]['synonyms'].append(clean_sgml_tags(str(d['unicode_name'])))
            if str(d['ascii_name']) not in ['', 'None', 'null']:
                f[d['fbid']]['synonyms'].append(clean_sgml_tags(str(d['ascii_name'])))

c.close()



for i in f:
    if 'label' in f[i].keys():
        statement = 'MERGE (f:Class {{short_form:"{0}"}}) '.format(i) + \
                    'ON CREATE SET f.label = "{0}", f.synonym = {1} '.format(f[i]['label'], json.dumps(list(set(f[i]['synonyms']))))
    else:
        statement = 'MERGE (f:Class {{short_form:"{0}"}}) '.format(i)
        if 'synonyms' in f[i].keys():
            statement += 'ON CREATE SET f.synonym = {0} '.format(json.dumps(list(set(f[i]['synonyms']))))
        print('Warning:')
        print(i)
        print(json.dumps(f[i])) # Expose odd case
    statement += 'WITH f ' + \
                 'MATCH (s:site {{iri:"{0}"}}) '.format(fb_url) + \
                 'WITH f, s ' + \
                 'MERGE (f)-[:link_to {{subdomain:"{0}{1}"}}]->(s) '.format(fb_sub, i)
    if 'type' in f[i].keys():
        statement += 'WITH f ' + \
                     'MERGE (t:Class {{short_form:"{0}"}}) '.format(map_feature_type(i, f[i]['type'])) + \
                     'ON CREATE SET t.label = "{0}" '.format(f[i]['type'])
        statement += 'WITH f, t ' + \
                     'MERGE (f)-[:is_a]->(t) ' # TODO: is 'is_a' correct?
        statement += 'WITH t ' + \
                     'MATCH (s:site {{iri:"{0}"}}) '.format(fb_url) + \
                     'WITH t, s ' + \
                     'MERGE (t)-[:link_to {{subdomain:"{0}{1}"}}]->(s) '.format(fb_sub, map_feature_type(i, f[i]['type']))
    statements.append(statement)


nc.commit_list_in_chunks(statements, verbose=False, chunk_length=1000)
statements = []

c = get_fb_conn()
cursor = c.cursor()

cursor.execute("SELECT DISTINCT s.uniquename AS subj, o.uniquename AS obj, r.name as rel "
               "FROM feature s " \
               "JOIN feature_relationship fr ON fr.subject_id=s.feature_id " \
               "JOIN cvterm r ON fr.type_id=r.cvterm_id " \
               "JOIN feature o ON fr.object_id=o.feature_id " \
               "WHERE r.name in ('alleleof', 'associated_with')")

dc = dict_cursor(cursor)
for d in dc:
    statement = 'MATCH (s:Class {short_form:"%s"})' % d['subj']
    statement += ', (o:Class {short_form:"%s"}) ' % d['obj']
    statement += 'MERGE (o)-[:%s]->(s)' % d['rel']
    statements.append(statement)
    print('.', end="")

nc.commit_list_in_chunks(statements, verbose=False, chunk_length=1000)

