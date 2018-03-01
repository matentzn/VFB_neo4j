#!/usr/bin/env python3
import psycopg2
from uk.ac.ebi.vfb.neo4j.neo4j_tools import neo4j_connect, chunks
#from ..vfb_neo_tools import VFBCypherGen
import re
import pandas as pd

'''
Created on 4 Feb 2016

General classes

@author: davidos
'''

### Sketch of usage:
# 1. query for expressed gene products
# generate feature, relation gp -> FBti/FBtp -> Allele (typed) -> gene
# Query genotype table -> generate genotypes and



def clean_sgml_tags(sgml_string):
    sgml_string = re.sub('<up>', '[', sgml_string)
    sgml_string = re.sub('</up>', ']', sgml_string)
    sgml_string = re.sub('<down>', '[[', sgml_string)
    sgml_string = re.sub("</down>", ']]', sgml_string)
    return sgml_string

def dict_cursor(cursor):
    """Takes cursor as an input, following execution of a query, returns results as a list of dicts"""
    # iterate over rows in cursor.description, pulling first element
    description = [x[0] for x in cursor.description] 
    l = []
    for row in cursor: # iterate over rows in cursor
        d = dict(zip(description, row))
#    yield dict(zip(description, row))  # This yields an iterator.  Doesn't actually run until needed.
        l.append(d)
    return l


def get_fb_conn():
    return psycopg2.connect(dbname='flybase',
                            host='chado.flybase.org',
                            user='flybase')


def map_feature_type(fbid, ftype):
    mapping = {'transgenic_transposon': 'SO_0000796',
               'insertion_site': 'SO_0001218',
                'transposable_element_insertion_site': 'SO_0001218',
                'natural_transposon_isolate_named': 'SO_0000797',
                'chromosome_structure_variation': 'SO_1000183'
                }
    if ftype == 'gene':
        if re.match('FBal', fbid):
            return 'SO_0001023'
        else:
            return 'SO_0000704 '
    elif ftype in mapping.keys():
        return mapping[ftype]
    else:
        return 'SO_0000110' # Sequence feature

def expand_stage_range(nc, start, end):
    """nc = neo4j_connect object
    start = start stage (short_form_id string)
    end = end stage (short_form_id string)
    Returns list of intermediate stages.
    """
    stages = [start, end]
    statements = [
        'MATCH p=shortestPath((s:FBDV {short_form:"%s"})<-[:immediately_preceded_by*]-" \
        "(e:FBDV {short_form:"%s"})) RETURN extract(x IN nodes(p) | x.short_form)' % (start, end)]
    r = nc.commit_list(statements)
    stages.append(r[0]['data'][0]['row'][0])
    return stages

class FB2Neo(object):
    """A general class for moving content between FB and Neo.
    Includes connections to FB and neo4J and a generic method for running queries
    SubClass this for specific transfer jobs."""
    
    def __init__(self, endpoint, usr, pwd, file_path=''):
        """Specify Neo4J server endpoint, username and password"""
        self._init(endpoint, usr, pwd)
        self.file_path = file_path  # A path for temp csv files
        self.fb_base_URI = 'http://www.flybase.org/reports/' # Should use curie_tools


    def _init(self, endpoint, usr, pwd):
        self.conn = get_fb_conn()
        self.nc = neo4j_connect(endpoint, usr, pwd)

    def query_fb(self, query):
        """Runs a query of public Flybase, 
        returns results as interable of dicts keyed on columns names"""
        cursor = self.conn.cursor()  # Investigate using with statement
        cursor.execute(query)
        dc = dict_cursor(cursor)
        cursor.close()
        return dc
        
    def commit(self):
        #probably better to call this FB commit?
        # Ermm - actually - waht is this for?
        self.conn.commit()

    def commit_via_csv(self, statement, dict_list):
        df = pd.DataFrame.from_records(dict_list)
        df.to_csv(self.file_path + "tmp.csv", sep='\t')
        self.nc.commit_csv("file:///" + "tmp.csv",
                           statement=statement,
                           sep="\t")
        # add something to delete csv here.


        
    def close(self):
        self.close()  # Investigate implementing using with statement.  Then method not required.


class pubMover(FB2Neo):

    def fu(self):
        return



class FeatureMover(FB2Neo):


    def name_synonym_lookup(self, fbids):
        """Makes unicode name primary.  Makes everything else a synonym"""
        #stypes: symbol nickname synonym fullname
        query = "SELECT f.uniquename as fbid, s.name as ascii_name, " \
                     "stype.name AS stype, " \
                     "fs.is_current, s.synonym_sgml as unicode_name " \
                     "FROM feature f " \
                     "JOIN feature_synonym fs on (f.feature_id=fs.feature_id) " \
                     "JOIN synonym s on (fs.synonym_id=s.synonym_id) " \
                     "JOIN cvterm stype on (s.type_id=stype.cvterm_id) " \
                     "WHERE f.uniquename IN ('%s')"
        dc = self.query_fb(query % "','".join(fbids))
        results = []
        old_key = ''
        out = {}
        for d in dc:
            key = d['fbid']
            if not (key == old_key):
                if out: results.append(out)
                out = {}
                out['fbid'] = d['fbid']
                out['synonyms'] = set()
            if d['stype'] == 'symbol' and d['is_current']:
                out['label'] = clean_sgml_tags(d['unicode_name'])
            else:
                out['synonyms'].add(clean_sgml_tags(d['ascii_name']))
                out['synonyms'].add(clean_sgml_tags(d['unicode_name']))
            old_key = key
        return results

    def add_features(self, fbids):
        """Takes a list of fbids, generates a csv and uses this to merge feature nodes,
        adding a unicode label and a list of synonyms"""
        names = self.name_synonym_lookup(fbids)
        proc_names = [{'fbid': r['fbid'], 'label': r['label'],
                       'synonyms': '|'.join(r['synonyms'])}
                      for r in names] # bit ugly...
        statement = "MERGE (n:Feature { short_form : line.fbid } ) " \
                    "SET n.label = line.label , n.synonyms = split(line.synonyms, '|')" # Need to set iri
        self.commit_via_csv(statement, proc_names)

    # Typing

    def grossType(self, fbids):
        query = "SELECT f.uniquename AS fbid, c.name as ftype " \
                "FROM feature f " \
                "JOIN cvterm c on f.type_id=c.cvterm_id " \
                "WHERE f.uniquename in ('%s')" % "','".join(fbids)  
        dc = self.query_fb(query)
        results = []
        for d in dc:
            results.append((d['fbid'],
                            map_feature_type(fbid=d['fbid'],
                                             ftype=d['ftype'])))
        return results

    
    def addTypes2Neo(self, fbids, detail='gross'):
        """Classify FlyBase features identified by a list of fbids.
        Optionally choose detailed classification with detail = 'fine'.  
        (This option is currently experimental)."""
        statements = []
        if detail == 'gross':
            types = self.grossType(fbids) 
        elif detail == 'fine':
            types = self.fineType(fbids)
        else:
            raise ValueError('detail arg invalid %s' % detail)

        feature_classifications = [{'child': t[0], 'parent': t[1]} for t in types]
        statement = "MATCH (p:Class { short_form: line.parent })" \
                    ",(c:Feature { short_form: line.child }) " \
                    "MERGE (p)<-[:SUBCLASSOF]-(c)"
        self.commit_via_csv(statement, feature_classifications)

    def abberationType(self, abbs):
        """abbs = a list of abberation fbids
        Returns a list of (fbid, type) tuples where type is a SO ID"""
        # Super slow and broken!
        results = []
        abbs_proc = [] # For tracking processed abbs
        query = "SELECT f.uniquename AS fbid, db.name AS db," \
                "dbx.accession AS acc " \
                "FROM feature f " \
                "JOIN cvterm gross_type ON gross_type.cvterm_id=f.type_id " \
                "JOIN feature_cvterm fc ON fc.feature_id = f.feature_id " \
                "JOIN cvterm fine_type ON fine_type.cvterm_id = fc.cvterm_id " \
                "JOIN feature_cvtermprop fctp ON fctp.feature_cvterm_id = fc.feature_cvterm_id " \
                "JOIN cvterm meta ON meta.cvterm_id = fctp.type_id " \
                "JOIN cvterm gtyp ON gtyp.cvterm_id = f.type_id " \
                "JOIN dbxref dbx ON fine_type.dbxref_id = dbx.dbxref_id " \
                "JOIN db ON dbx.db_id = db.db_id " \
                "WHERE gross_type.name = 'chromosome_structure_variation' -- double checks input gross type" \
                "AND  meta.name = 'wt_class'" \
                "AND f.uniquename in (%s)" % ("'" +"'.'".join(abbs))
        dc = self.query_fb(query)
        for d in dc:
            results.append((d['fbid'], d['db'] + '_' + d['acc']))
            abbs_proc.append(d['fbid'])
        [results.append((a, 'SO_0000110')) for a in abbs if a not in abbs_proc] # Defaulting to generic feature id not abb
        return results
            
    def fineType(self, fbids):
        gt = self.grossType()
        abbs_list = []
        results = []
        for g in gt:
            if g[1] == '':
                abbs_list.append(g[0])
            else:
                results.append(g)
            results.extend(self.abberationType(abbs_list))

    def _get_objs(self, subject_ids, chado_rel, out_rel, o_idp):
        query_template = "SELECT s.uniquename AS subj, o.uniquename AS obj FROM feature s " \
                        "JOIN feature_relationship fr ON fr.subject_id=s.feature_id "  \
                        "JOIN cvterm r ON fr.type_id=r.cvterm_id "  \
                        "JOIN feature o ON fr.object_id=o.feature_id "  \
                        "WHERE s.uniquename IN ('%s') " \
                        "AND r.name = '%s' "  \
                        "AND o.uniquename like '%s'"
        query = query_template % ("','".join(subject_ids), chado_rel, o_idp + '%')
        dc = self.query_fb(query)
        results = []
        for d in dc:
            results.append((d['subj'], out_rel, d['obj']))
        return results

    def allele2Gene(self, subject_ids):
        """Takes a list of allele IDs, returns a list of triples as python tuples:
         (allele rel gene) where rel is appropriate for addition to prod."""
        return self._get_objs(subject_ids, chado_rel='alleleof', out_rel='is_allele_of', o_idp='FBgn')

    # gp - transgene R associated_with Type object by uniquename FBgn
    def gp2Transgene(self, subject_ids):
        """Takes a list of gene product IDs, returns a list of triples as python tuples:
         (gene_product rel transgene) where rel is appropriate for addition to prod."""
        return self._get_objs(subject_ids, chado_rel='associated_with', out_rel='fu', o_idp='FBti|FBtp')

    # gp - gene associated_with Type object by uniquename FBgn
    def gp2Gene(self, subject_ids):
        """Takes a list of gene product IDs, returns a list of triples as python tuples:
         (gene_product rel gene) where rel is appropriate for addition to prod."""
        return self._get_objs(subject_ids, chado_rel='associated_with', out_rel='expressed_by', o_idp='FBgn')

    # transgene - allele  R associated_with Type object by uniquename FBal
    def transgene2allele(self, subject_ids):
        """Takes a list of transgene IDs, returns a list of triples as python tuples:
         (transgene rel allele) where rel is appropriate for addition to prod."""
        return self._get_objs(subject_ids, chado_rel='associated_with', out_rel='fu', o_idp='FBal')

    def add_feature_relations(self, triples, assume_subject=True):
        if not assume_subject:
            subjects = [t[0] for t in triples]
            self.add_features(subjects)
            self.addTypes2Neo(subjects)
        objects = [t[2] for t in triples]
        self.add_features(objects)
        self.addTypes2Neo(objects)
        statements = []
        for t in triples:
            statements.append(
                "MATCH (s:Feature { short_form: '%s'}), (o:Feature { short_form: '%s'}) " \
                "MERGE (s)-[r:%s]->(o)" % (t[0], t[2], t[1])
            )
        self.nc.commit_list_in_chunks(statements)


    def generate_expression_pattern(self):
        return

class expression_writer(FB2Neo):

    def get_all_expression(self, limit=False):
        query = 'SELECT c.name as cvt, db.name as cvt_db, dbx.accession as cvt_acc, ec.rank as ec_rank, ' \
                't1.name as ec_type, ectp.value as ectp_value, ' \
                't2.name as ectp_name, ectp.rank as ectp_rank, ' \
                'e.uniquename as fbex ' \
                'FROM expression_cvterm ec ' \
                'JOIN expression e on ec.expression_id=e.expression_id ' \
                'LEFT OUTER JOIN expression_cvtermprop ectp on ec.expression_cvterm_id=ectp.expression_cvterm_id  ' \
                'JOIN cvterm c on ec.cvterm_id=c.cvterm_id  ' \
                'JOIN dbxref dbx ON (dbx.dbxref_id = c.dbxref_id) ' \
                'JOIN db ON (dbx.db_id=db.db_id) ' \
                'JOIN cvterm t1 on ec.cvterm_type_id=t1.cvterm_id  ' \
                'LEFT OUTER JOIN cvterm t2 on ectp.type_id=t2.cvterm_id'

        if limit:
            query += " limit %d" % limit

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

        exp = self.query_fb(query)

        # make dict keyed on FBex : TAP-like structure
        FBex_lookup = {}

        for d in exp:
            FBex_lookup[d['fbex']] = {}
            FBex_lookup[d['fbex']][d['ec_type']] = {}
            if 'stage' in d['ec_type']:
                FBex_lookup[d['fbex']][d['ec_type']][d['ectp_value']] = {}
                FBex_lookup[d['fbex']][d['ec_type']][d['ectp_value']].update(
                        {"short_form": d['cvt_db'] + '_' + d['cvf_acc'],
                         "label": d['cvt'], 'rank1': d['ec_rank'],
                         'rank2': d['ectp_rank']})
            elif 'anatomy' in d['ec_type']:
                if 'qualifier' in d['ectp_name']:
                    FBex_lookup[d['fbex']][d['ec_type']][d['ectp_name']] = {}
                    FBex_lookup[d['fbex']][d['ec_type']].update(
                                {'short_form': d['cvt_db'] + '_' + d['cvf_acc'],
                                 'label': d['cvt'],
                                 'rank1': d['ec_rank'],
                                 'rank2': d['ectp_rank']})
                else:
                    FBex_lookup[d['fbex']][d['ec_type']].update(
                                {'short_form': d['cvt_db'] + '_' + d['cvf_acc'],
                                 'label': d['cvt'],
                                 'rank1': d['ec_rank']})
            elif 'assay' in d['ec_type']:
                FBex_lookup[d['fbex']][d['ec_type']].update(
                                {'short_form': d['cvt_db'] + '_' + d['cvf_acc'],
                                 'label': d['cvt'],
                                 'rank1': d['ec_rank']})

        self.FBex_lookup = FBex_lookup

    def write_expression(self, pub, expression_pattern, FBex):


        # Phase 1 Generate intermediate (stage restricted) anatomy nodes
        # Phase 2



        ### Where do the different lines get merged?  Do we make a intermediate data structure, or do it all in cypher?
        ### Given that these are already sorted on FBex, couldn't this be done within the loop structure?

        ### Schema for EP
        # https://github.com/VirtualFlyBrain/VFB_neo4j/issues/2
        # (as:Class:Anatomy { "label" :  'lateral horn  - from S-x to S-y', short_form : 'FBex...', assay: ''})
        # (as)-[SubClassOf]->(:Anatomy { label:  'lateral horn', short_form: "FBbt_...." })
        # (as)-[during]->(sr:stage { label: 'stage x to y'} )
        # (sr)-[start]->(:stage { label: 'stage x', short_form: 'FBdv_12345678' }
        # (sr)-[end]->(:stage { label: 'stage y', short_form: 'FBdv_22345678' }




        return











    

