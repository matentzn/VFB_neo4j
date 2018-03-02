'''
Import Expression data from FlyBase.
Prerequisites:  
 * Complete FBdv, FBbt & VFB KB must be loaded 
 * Edges named for relations
 * Uniqueness constraints on short_form ids should be in place.

Created on 4 Feb 2016
@author: davidos
'''

from uk.ac.ebi.vfb.neo4j.flybase2neo.dbtools import get_fb_conn, dict_cursor
from uk.ac.ebi.vfb.neo4j.tools import neo4j_connect, chunks
import sys

def expand_stage_range(nc, start, end):
    """nc = neo4j_connect object
    start = start stage (short_form_id string)
    end = end stage (short_form_id string)
    Returns list of intermediate stages.
    """
    stages = [start, end]
    statements = [
        'MATCH p=shortestPath((s:FBDV {short_form:"%s"})<-[:immediately_preceded_by*]-(e:FBDV {short_form:"%s"})) RETURN extract(x IN nodes(p) | x.short_form)' % (start, end)]
    r = nc.commit_list(statements)
    stages.append(r[0]['data'][0]['row'][0])
    return stages
    

### Spec: http://portal.graphgist.org/graph_gists/1cead583-7fdf-4f4d-95c8-07b828168b8c

# CREATE (ep:Class:Expression_pattern { label:  "expression pattern of P{GMR10A06-GAL4}" }),
#(ep)-[:SUBCLASSOF]->(:Class { label: 'expression pattern' }),
#(ep)-[:expresses]->(:Class { label: 'P{GMR10A06-GAL4}' }),
#(ep)<-[:INSTANCEOF]-(iep:Individual { label: 'GMR_10A06_AE_01_08-fA01b' }),
#(ep)-[:overlaps { pubs: ['FBrf12343567']}]->(as1:Class { label:  'lateral horn - S1 to S3'}),
#(as1)-[:SUBCLASSOF]->(:Class { label:  'lateral horn' }),
#(as1)-[:exists_during]->(s1:Class { label: 'stage S1' }),
#(as1)-[:exists_during]->(s2:Class { label: 'stage S2' }),
#(as1)-[:exists_during]->(s3:Class { label: 'stage S3' }),
#(sc:Individual:VFB { label: 'GMR_10A06_AE_01_08-fA01b image channel'} )-[:depicts]->(iep),
#(i:Individual:VFB { short_form: 'VFBi_1234567' })-[:has_signal_channel]->(sc),
#(i)-[:has_background_channel]->(bc:Individual { label: 'JFRC2010 image channel' }),
#(bc)-[:depicts]->(ri:Individual { label: 'JFRC 2010' }),
#(ri)-[:INSTANCEOF]->(ab:Class { label: 'adult brain'})

## NOTE: This glosses over gene product nodes - missing important information about whether gene or transgene expressed!
## Consider adding this back, but will need to settle on correct relationships to do so.

# Strategy
## Phase 1a: Add all classes transgenes/genes for which we have expression data
## Phase 1b: Link individuals to expression pattern classes 
## Phase 2: Add all expression statements (relying on FBex as unique identifier)
## Phase 3: Link transgenes to expression statement nodes via expression pattern nodes & occurs in edges, adding pubs.
#### MATCH (ep:Class { label: 'Expression pattern' })<-[:InstanceOf]-(n:Individual)
#### -[:expresses]->(:Class { short_form: 'FBbi1234567' } )  RETURN  ep

c = get_fb_conn()
cursor=c.cursor()
nc = neo4j_connect(base_uri=sys.argv[1], usr=sys.argv[2], pwd=sys.argv[3])


## Phase 1: Add nodes for all transgenes/genes for which we have expression data
### Note: 

cursor.execute("SELECT DISTINCT tgtyp.name as tg_type, obj2.feature_id as transgene_feature_id, obj2.name as transgene_name, " \
               "obj2.uniquename as transgene_uniquename, stype.name as gp_type,  " \
               "ex.uniquename as expession_id, subj.name as gp_name, subj.uniquename as gp_uname " \
               "    FROM feature_expression fe " \
               "    JOIN feature subj ON (fe.feature_id=subj.feature_id) " \
               "    JOIN cvterm stype ON (subj.type_id=stype.cvterm_id) " \
               "    JOIN feature_relationship fr1 ON (fe.feature_id = fr1.subject_id) " \
               "    JOIN cvterm rel1 ON (fr1.type_id = rel1.cvterm_id) " \
               "    JOIN feature_relationship fr2 ON (fr1.object_id = fr2.subject_id) " \
               "    JOIN cvterm rel2 ON (fr2.type_id = rel2.cvterm_id) " \
               "    JOIN feature obj2 ON (fr2.object_id = obj2.feature_id) " \
               "    JOIN feature_relationship fr3 ON (fr1.object_id = fr3.subject_id) " \
               "    JOIN cvterm rel3 ON (fr3.type_id = rel3.cvterm_id) " \
               "    JOIN cvterm tgtyp ON (tgtyp.cvterm_id=obj2.type_id) "
               "    JOIN expression ex ON (ex.expression_id=fe.expression_id)" \
               "    WHERE rel1.name='associated_with' " \
               "    AND rel2.name='associated_with' " \
               "    AND rel3.name='alleleof' " \
               "    AND obj2.uniquename ~ 'FBtp|FBti'")



#          tg_type                      | transgene_feature_id |  transgene_name       | transgene_uniquename | gp_type    | expression_id |         gp_name          |      gp_uname
# --------------------------------------+----------------------+-----------------------+----------------------+------------+---------------+--------------------------+--------------------
#"transposable_element_insertion_site"  |          43422913    |   "P{GawB}71y"        |    "FBti0131450"     |   "mRNA"   | FBex0027395   |   "Scer\GAL4[71y]RA"     |    "FBtr0308392"
#   "transgenic_transposon"             |          44372907    |   "P{Gr58b-GAL4.1}"   |    "FBtp0057261"     |   "mRNA"   | FBex0027395   |   "Scer\GAL4[Gr58b.1]RA" |    "FBtr0308414"

statements = []

### Better to have general classes for TG, mRNA?

iri_head = 'http://virtualflybrain.org/iri/'
iri_head = 'http://virtualflybrain.org/iri/'
fb_url = 'http://flybase.org/'
fb_sub = 'reports/'
fb_desc = 'A Database of Drosophila Genes and Genomes'
subclass_short = 'SUBCLASSOF' # TODO: Replace with official ID
expresses_short = 'expresses' # TODO: Replace with official ID
fb_base_uri = fb_url + fb_sub
link_short = 'link_to' # TODO: Replace with official ID
express_pat_short = 'CARO_0030002'
FBrt_edge_label = 'TODO' # TODO: Replace with official label
FBrt_edge_short = 'TODOid' # TODO: Replace with official ID
instance_short = 'instanceof' # TODO: Replace with official ID

# A dict to map from FB to more sensible classifications in SO.  Also consistent with current fb_feature generation.
def map_tg_type(name):
    map = { 'transgenic_transposon': 'SO_0000796', 'insertion_site': 'SO_0001218', 
     'transposable_element_insertion_site': 'SO_0001218', 'natural_transposon_isolate_named': 'SO_0000797'} 
    if name in map.keys():
        return map['name']
    else:
        return 'SO_0000110' # Sequence feature

# Iterate over expressed transgenes: adding nodes for each TG + expression pattern, and hooking these up to individuals
for d in dict_cursor(cursor):
    
    # How to specify to CREATE new ep class unless one exists.  Need to make this sufficient to merge in fb features coming from OWL (!):
    
    # Currently using uniqueness constraint on :VFB.  Should probably switch to one for FB features - perhaps based on SO.
    
    # Not adding name.  This should be added later as a job that populates features
    statements.append(
        'MERGE (tg:Class:VFB {{short_form: "{d[transgene_uniquename]}"}}) '
        'ON CREATE SET iri="{0}{d[transgene_uniquename]}", label="{d[transgene_name]}" '
        'WITH tg '
        'MERGE (t:VFB:Class {{short_form:"{1}"}}) '
        'ON CREATE SET iri="{0}{1}", label="{d[tg_type]}" '
        'WITH tg, t '
        'MERGE (tg)-[rts:SUBCLASSOF {{iri:"{0}{2}"}}]->(t) '
        'ON CREATE SET rts.short_form = "{2}" '
        'WITH tg '
        'MATCH (s:site {{iri:"{3}"}}) '
        'WITH tg, s '
        'MERGE (tg)-[l:link_to {{subdomain:"{4}{d[transgene_uniquename]}"}}]->(s) '
        'ON CREATE SET iri="{0}{5}", short_form:"{5}" '
        'WITH tg '
        'MERGE (cep:Class:ExpressionPattern {{ label: "expression pattern of {d[transgene_name]}" }}), '
        'ON CREATE SET cep.short_form="VFBe_{d[transgene_uniquename]}", cep.iri="{0}VFBe_{d[transgene_uniquename]}" '
        'WITH tg, cep '
        'MERGE '
        'MERGE (epc:VFB:Class {{ short_form: "{6}"}}) '
        'ON CREATE SET epc.label="expression pattern", epc.iri="{0}{6}"'
        'WITH tg, cep, epc '
        'MERGE (cep)-[rs:SUBCLASSOF {{iri:"{0}{7}"}}]->(epc) '
        'ON CREATE SET rs.short_form = "{7}" '
        'WITH cep, tg '
        'MERGE (cep)-[re:expresses {{iri:"{0}{8}"}}]->(tg) '
        'ON CREATE SET re.short_form = "{8}" '
        'WITH tg '
        'MERGE (egp:VFB:Class {{short_form:"{d[gp_uname]}"}}) '
        'ON CREATE SET epg.iri="{0}{d[gp_uname]}", egp.label="{d[gp_name]}", egp.type="{d[gp_type]}" '
        'WITH tg, egp '
        'MERGE (tg)<-[rgp:{9} {{iri:"{0}{10}"}}]-(egp) '
        'ON CREATE SET rgp.short_form="{10}" '.format(iri_head, map_tg_type(d['tg_type']), subclass_short, fb_url,
                                                      fb_sub, link_short, express_pat_short, subclass_short,
                                                      expresses_short, FBrt_edge_label, FBrt_edge_short, d=d))

    statements.append('MATCH (gep:Class { iri: "%s" })<-[:InstanceOf]-(iep:Individual)' \
                      '-[:expresses]->(tg:Class { iri: "%s" })<-[:expresses]-(cep:Class) ' \
                      'MERGE (iep)-[r:INSTANCEOF {iri:"%s"}]->(cep) '
                      'ON CREATE SET r.short_form="%s"' %
                      (iri_head + express_pat_short, iri_head + d['transgene_uniquename'],
                       iri_head + instance_short, instance_short))

nc.commit_list_in_chunks(statements)
    
# Ditto for expressed genes

# Could actually add in individuals at this point.

# Phase 2 - Add expression statements.  Need to make sure these merge to nodes loaded into KB.

cursor.execute("SELECT c.name as cvt, db.name as cvt_db, dbx.accession as cvt_acc, ec.rank as ec_rank, " \
                 "t1.name as ec_type, ectp.value as ectp_value, " \
                 "t2.name as ectp_name, ectp.rank as ectp_rank, " \
                 "e.uniquename as fbex " \
                 "FROM expression_cvterm ec " \
                 "JOIN expression e on ec.expression_id=e.expression_id " \
                 "LEFT OUTER JOIN expression_cvtermprop ectp on ec.expression_cvterm_id=ectp.expression_cvterm_id  " \
                 "JOIN cvterm c on ec.cvterm_id=c.cvterm_id  " \
                 "JOIN dbxref dbx ON (dbx.dbxref_id = c.dbxref_id) " \
                 "JOIN db ON (dbx.db_id=db.db_id) " \
                 "JOIN cvterm t1 ON ec.cvterm_type_id=t1.cvterm_id  " \
                 "LEFT OUTER JOIN cvterm t2 on ectp.type_id=t2.cvterm_id")


#         cvt         |      cvt_db      |                cvt_acc                 | ec_rank | ec_type | ectp_value | ectp_name | ectp_rank |    fbex     
# --------------------+------------------+----------------------------------------+---------+---------+------------+-----------+-----------+-------------
#  embryonic stage 4  | FBdv             | 00005306                               |       0 | stage   |            |           |           | FBex0000001
#  immunolocalization | FlyBase_internal | experimental assays:immunolocalization |       0 | assay   |            |           |           | FBex0000001
#  organism           | FBbt             | 00000001                               |       0 | anatomy |            |           |           | FBex0000001
#  70-100% egg length | FBcv             | 0000132                                |       1 | anatomy |            | qualifier |         0 | FBex0000001
#  embryonic stage 4  | FBdv             | 00005306                               |       0 | stage   |            |           |           | FBex0000002
#  immunolocalization | FlyBase_internal | experimental assays:immunolocalization |       0 | assay   |            |           |           | FBex0000002
#  organism           | FBbt             | 00000001                               |       0 | anatomy |            |           |           | FBex0000002
#  90-100% egg length | FBcv             | 0000139                                |       1 | anatomy |            | qualifier |         0 | FBex0000002
#  embryonic stage 1  | FBdv             | 00005291                               |       0 | stage   | FROM       | operator  |         0 | FBex0000003
#  embryonic stage 5  | FBdv             | 00005311                               |       1 | stage   | TO         | operator  |         0 | FBex0000003

### Where do the different lines get merged?  Do we make a intermediate data structure, or do it all in cypher?
### Given that these are already sorted on FBex, couldn't this be done within the loop structure?

### Spec :
# https://github.com/VirtualFlyBrain/VFB_neo4j/issues/2
# (as:Class:Anatomy { "label" :  'lateral horn  - from S-x to S-y', short_form : 'FBex...', assay: ''})
# (as)-[SubClassOf]->(:Anatomy { label:  'lateral horn', short_form: "FBbt_...." })
# (as)-[during]->(sr:stage { label: 'stage x to y'} )
# (sr)-[start]->(:stage { label: 'stage x', short_form: 'FBdv_12345678' }
# (sr)-[end]->(:stage { label: 'stage y', short_form: 'FBdv_22345678' }

fbex_dict = {}
statements = []

## Need to be able to operate on each FBex independently
for d in dict_cursor(cursor):
    fbex_dict[d['fbex']] = {}
    fbex_dict[d['fbex']][d['ec_type']] = {}
    if 'stage' in d['ec_type']:
        fbex_dict[d['fbex']][d['ec_type']][d['ectp_value']] = {}
        fbex_dict[d['fbex']][d['ec_type']][d['ectp_value']].update({"short_form": d['cvt_db'] + '_' + d['cvf_acc'], "label": d['cvt'], 'rank1': d['ec_rank'], 'rank2': d['ectp_rank']})
    else:
        if 'anatomy' in d['ec_type']:
            if 'qualifier' in d['ectp_name']:
                fbex_dict[d['fbex']][d['ec_type']][d['ectp_name']] = {}
                fbex_dict[d['fbex']][d['ec_type']].update(
                    {'short_form': d['cvt_db'] + '_' + d['cvf_acc'], 'label': d['cvt'], 'rank1': d['ec_rank'], 'rank2': d['ectp_rank']})
            else:
                fbex_dict[d['fbex']][d['ec_type']].update(
                    {'short_form': d['cvt_db'] + '_' + d['cvf_acc'], 'label': d['cvt'], 'rank1': d['ec_rank']})
        else:
            if 'assay' in d['ec_type']:
                fbex_dict[d['fbex']][d['ec_type']].update(
                    {'short_form': d['cvt_db'] + '_' + d['cvf_acc'], 'label': d['cvt'], 'rank1': d['ec_rank']})

# Now need a loop structure that stays on 

during_short = 'exists_during' # TODO: Replace with official ID

for ex in fbex_dict:
    stages=[]
    exid = 'unknown'
    if 'stage' in fbex_dict[ex].keys():
        if 'FROM' in fbex_dict[ex]['stage'].keys():
            exid = fbex_dict[ex]['stage']['FROM']['short_form']
            if 'TO' in fbex_dict[ex]['stage'].keys():
                exid = exid + '-' + fbex_dict[ex]['stage']['TO']['short_form']
                stages=expand_stage_range(nc, fbex_dict[ex]['stage']['FROM']['short_form'], fbex_dict[ex]['stage']['TO']['short_form'])
            else:
                print('Warning no TO in %s' % ex)
                print(fbex_dict[ex]['stage'].keys())
        else:
            print('Warning no FROM in %s' % ex)
            print(fbex_dict[ex]['stage'].keys())
    else:
        print('Warning no stage details in %s' % ex)
        print(fbex_dict[ex].keys())
    statement = 'MERGE (cep:Class:ExpressionPattern {{iri:{0}{ex}}}) WITH cep '
    if 'anatomy' in fbex_dict[ex].keys():
        statement = statement + 'MERGE (ant:Class {short_form:"{0}"}) ' \
                                'ON CREATE SET ant.label="{1}" WITH cep, ant '.format(
            fbex_dict[ex]['anatomy']['short_form'], fbex_dict[ex]['anatomy']['label']) + \
                    'MERGE (cep)-[:SUBCLASSOF]->(ant)'
    for stage in stages:
        statement = statement + 'MATCH (st:FBDV {{short_form:"{0}"}}) '.format(stage) + 'WITH st, cep ' + \
                    'MERGE (cep)-[:exists_during {{iri:"{0}{1}"}}]->(st) WITH cep '.format(iri_head, during_short)



if d['cvt_db'] == 'FBbt':
    statements.append("MATCH (a:Class:Anatomy { short_form: '%s_%s' } ) MERGE (x:Class: { short_form = '%s' } ) MERGE (x)-[:SUBCLASSOF]-(a)"  %
                      (d['cvt_db'], d['cvt_acc'], d['fbex']))
if d['cvt_db'] == 'FBdv' and not d['extp_value']:
        statements.append("MATCH (d:Class:Stage { short_form: '%s_%s' } ) MERGE (x:Class: { short_form = '%s' } ) MERGE (x)-[:during]-(d)"  %
                      (d['cvt_db'], d['cvt_acc'], d['fbex']))

        # Expansion of stage annotation can be done with a neo4J query.


# Phase 3 - linking expression to features


cursor.execute("SELECT f.name as fname, f.uniquename as funame, e.uniquename as fbex, pub.uniquename as fbrf" \
"FROM feature_expression fe " \
"JOIN feature f ON (f.feature_id=fe.feature_id) " \
"JOIN expression e on (e.expression_id = fe.expression_id) " \
"JOIN pub ON (pub.pub_id=fe.pub_id) ")

#  Difficulty here is how to generate a pub list on the edge without duplicating the edge.  Then again - are multiple edges so bad?  --- not Great for OWL version.
for d in dict_cursor(cursor):
    statements = []
    statements.append("MATCH (:GeneProduct { short_form: '%s' )<-[:expresses]-(ep:Expression_pattern), " \
                      "(fbex:Class { short_form = '%s')" \
                      "MERGE (ep)-[:overlaps { pub : '%s' , short_form: 'RO_' }]->(fbex) "
                      % (d['funame'], d['fbex'], d['fbrf']))




    
