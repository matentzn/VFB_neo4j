'''
Created on Mar 6, 2017

@author: davidos
'''
import warnings
from .tools import neo4j_connect

# Architecture question:  Should this wrap neo4j_connect?  Should it run and test batches of cypher statments?


#  * OWL - Only edges of types Related, INSTANCEOF, SUBCLASSOF are exported to OWL.
#    * (:Individual)-[:Related { URI: '', name: ''}]-(:Individual)  -> OWL FACT (OPA)
#    * (:Individual)-[:Related { URI: '', name: ''}]-(:Class) -> OWL Type: R some C
#    * (:Class)-[:Related { URI: '', name: ''}]-(:Individual) -> OWL SubClassOf: R value I

# Match statements checks for all relevant entites, including relations if applicable. Implementing methods should 
# check return values and warn/fail as appropriate if no match.

class add_owl(object):
    
    def __init__(self, endpoint, usr, pwd):
        self.nc = neo4j_connect(endpoint, usr, pwd)
        self.statements = []
        self.output = []
           
    def commit(self, verbose = False, chunk_length = 5000):
        self.output = self.nc.commit_list_in_chunks(statements = self.statements, 
                                      verbose = verbose, 
                                      chunk_length = chunk_length)
        return self.output
        
    def _set_attributes_from_dict(self, var, attribute_dict):
        """Generates CYPHER `SET` sub-clauses 
        from key value pairs in a dict (attribute_dict).
        Values must be int, float, string or list.
        var = variable name in CYPHER statement."""
        out = ''
        for k,v in attribute_dict.items():
            if type(v) == int:
                out += "SET %s.%s = %d " % (var,k,v)
            elif type(v) == float:   
                out += "SET %s.%s = %f " % (var,k,v)                    
            elif type(v) == str:
                out += "SET %s.%s = '%s' " % (var,k,v)
            elif type(v) == list:        
                out += "SET %s.%s = %s " % (var,k, str(v))
            else: 
                warnings.warn("Can't use a %s as an attribute value in Cypher. Content :%s" 
                              % (type(v), (str(v))))
        return out
    
    def _add_related_edge(self, s, r, o, stype, otype, edge_annotations = {}):
        out =  "MATCH (s:%s { IRI: '%s'} ), (rn:Relation { IRI: '%s' }), (o:%s { IRI: '%s'} ) " % (stype, s, r, otype, o)
        out += self._set_attributes_from_dict('r', edge_annotations)        
        out += "MERGE (s)-[:Related { IRI: '%s'}]-(o) RETURN 's:%s,r:%s,o:%s'" % (r, s, r, o)
        self.statements.append(out)
    
    def add_fact(self, s, r, o, edge_annotations = {}):
        self._add_related_edge(s, r, o, stype = 'Individual', otype = 'Individual', 
                               edge_annotations = edge_annotations) 
                
    def add_anon_type_ax(self, s, r, o, edge_annotations = {}):
        self._add_related_edge(s, r, o, stype = 'Individual', otype = 'Class', 
                               edge_annotations = edge_annotations) 
        
    def add_named_type_ax(self, s,o):
        return "MATCH (s:Individual { IRI: '%s'} ), (o:Class { IRI: '%s'} ) " \
                "MERGE (s)-[:INSTANCEOF]-(o)" % (s, o)  
                
    def add_anon_subClassOf_ax(self, s,r,o, edge_annotations = {}):
        ### Should probably only support adding individual:individual edges in KB...
        self._add_related_edge(s, r, o, stype = 'Individual', otype = 'Class', 
                               edge_annotations = edge_annotations) 

    def add_named_subClassOf_ax(self, s,o):
        return "MATCH (s:Class { IRI: '%s'} ), (o:Class { IRI: '%s'} ) " \
                "MERGE (s)-[:SUBCLASSOF]-(o)" % (s, o)            
    
    def add_ind(self, iri, short_form, label, synonyms = [], additional_attributes = {}):
        out = "MERGE (i:Individual { IRI: '%s'} ) " \
                "SET i.short_form = '%s' " \
                "SET i.label = '%s' " % (iri, short_form, label)
        if synonyms:
                out += "SET i.synonyms = %s  " % str(synonyms)     
        out += self._set_attributes_from_dict('i', additional_attributes)
        return out
    
    def add_relation_node(self, iri, short_form, label):
        return "MERGE (i:Relation { IRI: '%s'} ) " \
                "SET i.short_form = '%s' " \
                "SET i.label = '%s' " % (iri, short_form, label)
                
    def populate_edge_from_relation_node(self, r):
        """Populates a related edge using the attributes found on relation node"""
        return "MATCH (rn:Relation { IRI: '%s' }), ()-[re:Related { IRI: '%s' }]-() " \
                "SET re.label = rn.label SET re.short_form = re.short_form" % r
    
    
    def test_edge_addition(self):
        """Tests lists of return values from RESTFUL API for edge creation
         by checking "relationships_created": as a boolean, generates warning
        """
        missed_edges = [x['columns'][0] for x in self.output[0] if not x['data']]
        if missed_edges:
            for e in missed_edges:
                warnings.warn("Edge not added. Something doesn't match here %s" % e)
    

