#!/usr/bin/env python3
import psycopg2
from uk.ac.ebi.vfb.neo4j.neo4j_tools import neo4j_connect, chunks
#from ..vfb_neo_tools import VFBCypherGen
import re

'''
Created on 4 Feb 2016

General classes

@author: davidos
'''


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
    return psycopg2.connect(dbname = 'flybase', host ='chado.flybase.org', user = 'flybase')


class FB2Neo(object):
    """A general class for moving content between FB and Neo.
    Includes connections to FB and neo4J and a generic method for running queries
    SubClass this for specific transfer jobs."""
    
    def __init__(self, endpoint, usr, pwd):
        """Specify Neo4J server endpoint, username and password"""
        self._init(endpoint, usr, pwd)
    
    def _init(self, endpoint, usr, pwd):
        self.conn = get_fb_conn()
        self.nc = neo4j_connect(endpoint, usr, pwd)
        self.fb_base_URI = 'http://www.flybase.org/reports/'
        
    def run_query(self, query):
        """Runs a query of public Flybase, 
        returns results as interable of dicts keyed on columns names"""
        cursor = self.conn.cursor() # Investigate using with statement
        cursor.execute(query)
        dc  = dict_cursor(cursor)
        cursor.close()
        return dc
        
    def commit(self):
        self.conn.commit()
        
    def close(self):
        self.close()  # Investigate implementing using with statement.  Then method not required.
    
    def update_features(self, fbids):
        pass
    
        

class FeatureRelationship(FB2Neo):
    """A class for navigating the feature relationship graph.  
    Methods all take lists of short_form IDs and return a list of triples as python tuples.
    """    
    def get_objs(self, subject_ids, chado_rel, out_rel, o_idp):
        query_template = "SELECT s.uniquename AS subj, o.uniquename AS obj FROM feature s " \
                        "JOIN feature_relationship fr ON fr.subject_id=s.feature_id "  \
                        "JOIN cvterm r ON fr.type_id=r.cvterm_id "  \
                        "JOIN feature o ON fr.object_id=o.feature_id "  \
                        "WHERE s.uniquename IN ('%s') " \
                        "AND r.name = '%s' "  \
                        "AND o.uniquename like '%s'"
        query = query_template % ("','".join(subject_ids), chado_rel, o_idp + '%')
        dc = self.run_query(query)
        results = []
        for d in dc:
            results.append((d['subj'], out_rel, d['obj']))
        return results
    
    #allele - gene  R alleleOf Type object by uniquename FBgn    
    def allele2Gene(self, subject_ids):
        return self.get_objs(subject_ids, chado_rel='alleleof', out_rel='alleleof', o_idp='FBgn')
    
    # gp - transgene R associated_with Type object by uniquename FBgn
    def gp2Transgene(self, subject_ids):
        return self.get_objs(subject_ids, chado_rel='associated_with', out_rel='', o_idp='FBti|FBtp')
    
    # gp - gene associated_with Type object by uniquename FBgn   
    def gp2Gene(self, subject_ids):
        return self.get_objs(subject_ids, chado_rel='associated_with', out_rel='', o_idp='FBgn')
    
    # transgene - allele  R associated_with Type object by uniquene FBal
    def transgene2allele(self, subject_ids):
        return self.get_objs(subject_ids, chado_rel='associated_with', out_rel='', o_idp='FBal')

        
class nameFeatures(FB2Neo):
    """Looks up synonyms and official symbol in unicode
    Adds them to Neo.
    """
    ### Aim here is to add full names and synonyms.  Query looks fine.  Seems a bit odd as a class. 
    ### Why would you pass this round as an object?

    ### If implmented in the same way as other ontology classes, then could add synonyms on edge links to pubs.
    ### This would rather bloat the DB though...  Or Should I not be worrying about size...
    def __init__(self):
        self.init()
        self.query = "SELECT f.uniquename as fbid, s.name as ascii_name, stype.name AS stype, " \
                    "fs.is_current, s.synonym_sgml as unicode_name " \
                    "FROM feature f " \
                    "JOIN feature_synonym fs on (f.feature_id=fs.feature_id) " \
                    "JOIN synonym s on (fs.synonym_id=s.synonym_id) " \
                    "JOIN cvterm stype on (s.type_id=stype.cvterm_id) " \
                    "WHERE f.uniquename IN ('%s')"


    def nameSynonymLookup(self, fbids):
        """Makes unicode name primary.  Makes everything else a synonym"""
        #stypes: symbol nickname synonym fullname
        dc = self.run_query(self.query % "','".join(fbids))
        results = {}
        old_key = ''
        for d in dc:
            key = d['uniquename']
            if not (key == old_key):
                results[d['fbid']] = {}
                results[d['fbid']]['synonyms'] = []
            if d['stype'] == 'symbol' and d['is_current']:
                results[d['fbid']]['label'] = d['unicode_name']
            else:
                results[d['fbid']]['synonyms'].append(d['ascii_name'])
                results[d['fbid']]['synonyms'].append(d['unicode_name'])
            old_key = key
        return results             
      
    def addSynsToNeo(self, fbids):
        """Adds unicode label and a list of synonyms.
        """
        names = self.nameSynonymLookup(fbids)
        statements = []  
        for fbid, v in names.items():            
            statements.append("MATCH (n:short_form : '%s') SET n.label = '%s', n.synonyms = %s"  
                              % (fbid, v['label'], str(v['synonyms'])))
        self.nc.commit_list_in_chunks(statements)         

class FeatureType(FB2Neo):
    
    def __init__(self):
        self._init()
        
    def grossType(self, fbids):
        query = "SELECT f.uniquename AS fbid, typ.name as ftype; FROM feature f " \
                "JOIN cvterm c on f.type_id=c.cvterm_id " \
                "WHERE f.uniquename in ('%s')" % "','".join(fbids)  
        dc = self.run_query(query)
        results = []
        for d in dc:
            results.append((d['fbid'], self.map_feature_type(fbid = d['fbid'], ftype = d['ftype'])))
        return results
        
    def map_feature_type(self, fbid, ftype):
        mapping = { 'transgenic_transposon': 'SO_0000796',
                      'insertion_site': 'SO_0001218', 
                      'transposable_element_insertion_site': 'SO_0001218',
                      'natural_transposon_isolate_named': 'SO_0000797',
                      'chromosome_structure_variation' : 'SO_1000183'
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
    
    def addTypes2Neo(self, fbids, detail = 'gross'):
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
        for t in types:
            statements.append("MATCH (p:Class { short_form: '%s' }),(c:Class { short_form: '%s' }) " \
            "MERGE (p)<-[:SUBCLASSOF]-(c)"  % (t[1], t[0]))
        self.nc.commit_list_in_chunks(statements)
                           
        
    def abberationType(self, abbs):
        """abbs = a list of abberation fbids
        Returns a list of (fbid, type) tuples where type is a SO ID"""
        
        results = []
        abbs_proc = [] # For tracking processed abbs
        query = "SELECT f.uniquename AS fbid db.name AS db, dbx.accession AS acc " \
        "FROM feature f " \
        "JOIN cvterm gross_type ON gross_type.cvterm_id=f.type_id"
        "JOIN feature_cvterm fc ON fc.feature_id = f.feature_id " \
        "JOIN cvterm fine_type ON fine_type.cvterm_id = fc.cvterm_id " \
        "JOIN feature_cvtermprop fctp ON fctp.feature_cvterm_id = fc.feature_cvterm_id " \
        "JOIN cvterm meta ON meta.cvterm_id = fctp.type_id " \
        "JOIN cvterm gtyp ON gtyp.cvterm_id = f.type_id " \
        "JOIN dbxref dbx ON fine_type.dbxref_id = dbx.dbxref_id"
        "JOIN db ON db.dbxref_id = dbx.dbxref_id"
        "WHERE gross_type.name = 'chromosome_structure_variation' -- double checks input gross type" \
        "AND  meta.name = 'wt_class' " \
        "AND f.uniquename in ('%s')"
        dc = self.run_query(query)
        for d in dc:
            results.append((d['fbid'], d['db'] + '_' + d['acc']))
            abbs_proc.append(d['fbid'])
        [results.append((a, 'SO_0000110')) for a in abbs if a not in abbs_proc] # Defaulting to generic feature id not abb
        return results
            
        def fineType():
            gt = self.grossType()
            abbs_list = []
            results = []
            for g in gt:
                if g[1] == '':
                    abbs_list.append(g[0])
                else:
                    results.append(g)
            results.extend(self.abberationType(abbs_list))
            


    

