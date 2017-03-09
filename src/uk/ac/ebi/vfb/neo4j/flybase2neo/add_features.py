'''
Created on Oct 28, 2016

@author: davidos
'''

from .FBtools import FB2Neo
from ..tools import chunks
import re
from uk.ac.ebi.vfb.neo4j.flybase2neo.FBtools import FeatureRelationship,\
    nameFeatures
    
# Aims
## Add FlyBase features that are directly used in genotypes and expression annotation.
## Retain genotype -> feature links

def generate_monarch_genotype_short_form(chado_id):
    base_iri = 'https://monarchinitiative.org/'
    short_form = "MONARCH_FBgeno" + chado_id
    return { 'base_iri' : base_iri , 'short_form' : short_form, 'iri' : base_iri + short_form }

def clean_sgml_tags(sgml_string):
    sgml_string = re.sub('\<up\>', '[', sgml_string)
    sgml_string = re.sub('\<\\up\>', ']', sgml_string)
    sgml_string = re.sub('\<\\up\>', '[[', sgml_string)
    sgml_string = re.sub('\<\\down>', ']]', sgml_string)
    return sgml_string
    

class AddFeatures(FB2Neo):
    """A general class for safely adding/updating features from FB.
    To construct, specify neo endpoint, usr, pwd"""
    
    def add_genotypes_components(self, limit = False):
        """
        """
        genotype_base = 'https://monarchinitiative.org/'
        genotype_pre = 'MONARCH_FBgeno'
        query = "SELECT DISTINCT g.id AS chado_id, g.uniquename AS genotype_name," \
                "gcomp.uniquename AS gcomp_id, gcomp.name AS gcomp_name" \
                "FROM genotype g " \
                "JOIN phenstatement ps ON (ps.genotype_id = g.genotype_id) -- limit to simple phens" \
                "JOIN feature_genotype fg ON (g.genotype_id = fg.genotype_id) " \
                "JOIN feature gcomp ON (fg.feature_id = gcomp.feature_id) "               
        gc = self.run_query(query)
        statements = []
        if limit:
            limit_clause = 'limit 5' % limit
        else:
            limit_clause = ''
        for g in gc:
            statements.append("MATCH (gc:Class { short_form : 'GENO_0000536' }) " \
                      "MERGE (f:Class { iri : '%s' } " \
                      "MERGE (g:Class:genotype { iri : '%s'}) " \
                      "MERGE (g)-[:SUBCLASSOF]->(gc) " \
                      "MERGE (g)-[:has_part]->(f) " \
                      "SET g.short_form = '%s' " \
                      "SET g.name = '%s' %s"
                        % (self.fb_base_URI + g['gcomp_id'],
                           genotype_base + genotype_pre + g['chado_id'],
                           genotype_pre + g['chado_id'],
                           clean_sgml_tags(sgml_string = g['genotype_name']),
                           limit_clause))
                                             
        self.nc.commit_list_in_chunks(statements)
    
    def gen_cypher_add_feature(self, fbid):
        """RETURNS a cypher statement for merging a feature on its iri, adding"""
        
        return "MERGE (f:Class { iri : '%s' }) " \
                "SET f.short_form = '%s, label = '%s' " % (                    
                self.fb_base_URI + fbid, fbid, ) 

   

    def update_expressed_features(self):
        query = "SELECT DISTINCT f.uniquename AS fbid, f.name" \
               "FROM feature_expression fe " \
               "JOIN feature f ON (fe.feature_id=f.feature_id)"
        dc = self.run_query(query)    
        gps = [d['fbid'] for d in dc]
        fr = FeatureRelationship(self.endpoint, self.usr, self.pwd)
        expressed_features = fr.gp2Transgene(gps) # Get expressed transgenes
        expressed_features.extend(fr.gp2Gene(gps))
        statements = []
        for f in features:
            statements.append(self.gen_cypher_add_feature(f))
        self.nc.commit_list_in_chunks(statements)                    
        return expressed_features
            
    
    def upadate_expression_patterns(self):
        features = self.update_expressed_features()
        
        # Problem1:  These need to be update-able, 
        # but how can we track IDs if we have to generate new ones at each update?
        # Can do match queries first to check whether they exist.
        
        
        cs = chunks(features, 1000)
        for c in cs:
            statement = "MATCH (epg:Class { iri : '%s' })<-[:SUBCLASSOF]-" \
            "(ep:Class { iri : '%s' })-[:expresses { type : 'rel' }]-" \
            "(f:Class) WHERE f.short_form IN ('%s') " \
            "RETURN f " % "','".join(c)
            f = self.nc.commit_list(statement)
        
        
        # Can match on names - but this will be screwed up when name updates... 
        # Unless that update is done first... 
        statements = []
        for f in features:
            statements.append("MATCH (epg:Class { iri : '%s' }),  " \
                              "(f:Class { iri : '%s' }) " \
                              "MERGE (epg)<-[:SUBCLASSOF]-(ep:Class { iri : '%s' } ) "  \
                              "-[:expresses { type : 'rel' }]-(f) " \
                              "SET ep.short_form ='%s', ep.label = '%s'"
                              % ())
        
        features = self.get_expressed_features()
        statements = []
        for f in features:
             statments.append(self.gen_cypher_add_feature(fbid = d['fbid'], ascii_sym = d['ascii_sym']))
             
        self.nc.commit_list_in_chunks()
        nf = nameFeatures()
        nf.addSynsToNeo(features)

        
        
        self.nc.commit_list_in_chunks(statements)
            
        statements = []
        for ef in features:
            
            ep_id = gen_ep_id()
            ep_name = 
            "CREATE (gp:Class { iri : '%s' }) " \
            "SET f.short_form = '%s, label = '%s' 
            statement = self.gen_cypher_add_feature(fbid = d['fbid'],
                                                          ascii_sym = d['ascii_sym'])
            statement += self.gen_cypher_add_feature(fbid = d['fbid'],
                                                          ascii_sym = d['ascii_sym'])" \
            
                      +=             "MERGE (ep)-[:expresses]->(f) "
                         % gen_ep_id()
                                                          
            
      
            
        
        
        
        pass
