'''
Created on Mar 6, 2017

@author: davidos
'''
import warnings
import re
import json
import psycopg2
import requests
from .neo4j_tools import neo4j_connect
from .SQL_tools import get_fb_conn, dict_cursor
from ..curie_tools import map_iri

# Architecture question:  Should this wrap neo4j_connect?  Should it run and test batches of cypher statments?


#  * OWL - Only edges of types Related, INSTANCEOF, SUBCLASSOF are exported to OWL.
#    * (:Individual)-[:Related { IRI: '', label: ''}]-(:Individual)  -> OWL FACT (OPA)
#    * (:Individual)-[:Related { IRI: '', label: ''}]-(:Class) -> OWL Type: R some C
#    * (:Class)-[:Related { IRI: '', label: ''}]-(:Individual) -> OWL SubClassOf: R value I

# Match statements checks for all relevant entites, including relations if applicable. Implementing methods should 
# check return values and warn/fail as appropriate if no match.

# TODO: Add lookup for attributes -> Properties.  Ideally this would be with a specific cypher label for APs.
# May want to follow a prefixed pattern to indicate OWL compatible APs.

class kb_writer (object):
      
    def __init__(self, endpoint, usr, pwd):
        self.nc = neo4j_connect(endpoint, usr, pwd)
        self.statements = []
        self.output = []
        self.properties = set([])
        
    def commit(self, verbose = False, chunk_length = 5000):
        """Commits Cypher statements stored in object.
        Flushes existing statement list.
        Returns REST API output.
        Optionally set verbosity and chunk length for commits."""
        self.output = self.nc.commit_list_in_chunks(statements = self.statements, 
                                      verbose = verbose, 
                                      chunk_length = chunk_length)
        self.statements = []
        return self.output

    def escape_string(self, strng):
        if type(strng) == str:
            strng = re.sub("'", "\\'", strng)
            strng = re.sub(r'\\', r'\\\\', strng)
        return strng
  
    def _add_textual_attribute(self, var, key, value):
        return 'SET %s.%s = "%s" ' % (var, key, self.escape_string(value)) # Note arrangement single and double quotes
    
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
                out += 'SET %s.%s = "%s" ' % (var, k, self.escape_string(v))           
            elif type(v) == list:                        
                out += 'SET %s.%s = %s ' % (var,k, str([self.escape_string(i) for i in v]))
            elif type(v) == bool:
                out += "SET %s.%s = %s " % (var,k, str(v))                
            else: 
                warnings.warn("Can't use a %s as an attribute value in Cypher. Key %s Value :%s" 
                              % (type(v), k, (str(v))))
        return out

class kb_owl_edge_writer(kb_writer):
    """A class wrapping methods for updating imported entities in the KB.
    Constructor: kb_owl_edge_writer(endpoint, usr, pwd)
    """
    
    def check_proprties(self):
        q = self.nc.commit_list(["MATCH (n) WHERE n.IRI in %s RETURN n.IRI" % (str(list(self.properties)))])
        if q:
            in_kb = [x['row'][0] for x in q[0]['data'] if q[0]['data']] # YUK!
            not_in_kb = self.properties.difference(set(in_kb))
            if not_in_kb:
                warnings.warn("Not in KB! %s" % str(not_in_kb))
                return False
            else:
                return True
        else: 
            return False
            
    def _add_related_edge(self, s, r, o, stype, otype, edge_annotations = {}):
        self.properties.add(r)
        out =  "MATCH (s:%s { IRI:'%s'} ), (rn:Property { IRI: '%s' }), (o:%s { IRI:'%s'} ) " % (
                                                                           stype, s, r, otype, o)
        out += "MERGE (s)-[re:Related { IRI: '%s'}]-(o) " % r
        out += self._set_attributes_from_dict('re', edge_annotations)        
        out += "SET re.label = rn.label SET re.short_form = rn.short_form "
        out += "RETURN '%s', '%s', '%s' " % (s,r,o) # returning input for ref in debugging
        # If the match fails, no input is returned.
        self.statements.append(out)
    
    def add_fact(self, s, r, o, edge_annotations = {}):
        """Add OWL fact to statement queue.
        s=subject individual iri, 
        r= relation (ObjectProperty) iri,
        o = object individual iri.
        Optionally add edge annotations specified as key value 
        pairs in dict."""
        self._add_related_edge(s, r, o, stype = 'Individual', otype = 'Individual', 
                               edge_annotations = edge_annotations) 
                
    def add_anon_type_ax(self, s, r, o, edge_annotations = {}):
        """Add anonymous OWL Type statement queue.
        s= subject individual iri, 
        r= relation (ObjectProperty) iri,
        o = object Class iri.
        Optionally add edge annotations specified as key value 
        pairs in dict."""
        self._add_related_edge(s, r, o, stype = 'Individual', otype = 'Class', 
                               edge_annotations = edge_annotations) 
        
    def add_named_type_ax(self, s,o):
        return "MATCH (s:Individual { IRI: '%s'} ), (o:Class { IRI: '%s'} ) " \
                "MERGE (s)-[:INSTANCEOF]-(o) " \
                "RETURN " % (s, o)  
                
    def add_anon_subClassOf_ax(self, s,r,o, edge_annotations = {}):
        ### Should probably only support adding individual:individual edges in KB...
        self._add_related_edge(s, r, o, stype = 'Individual', otype = 'Class', 
                               edge_annotations = edge_annotations) 

    def add_named_subClassOf_ax(self, s,o):
        return "MATCH (s:Class { IRI: '%s'} ), (o:Class { IRI: '%s'} ) " \
                "MERGE (s)-[:SUBCLASSOF]-(o)" % (s, o)            
    
    
    def test_edge_addition(self):
        """Tests lists of return values from RESTFUL API for edge creation
         by checking "relationships_created": as a boolean, generates warning
        """
        missed_edges = [x['columns'] for x in self.output[0] if not x['data']]
        if missed_edges:
            for e in missed_edges:
                warnings.warn("Edge not added. Something doesn't match here: %s" % str(e))
            return False
        else:
            return True

class node_importer(kb_writer):
    """A class wrapping methods for updating imported entities in the KB,
    e.g. from ontologies, FlyBase, CATMAID.
    Constructor: owl_import_updater(endpoint, usr, pwd)
    """   
    def add_constraints(self, uniqs= {}, indexes = {} ):
        """Specify addition uniqs and indexes via dicts.
        { label : [attributes] } """
        for k,v in uniqs.items():
            for a in v:
                self.statements.append("CREATE CONSTRAINT ON (n:%s) ASSERT n.%s IS UNIQUE" % (k,a))
        for k,v in indexes.items():
            for a in v:
                self.statements.append("CREATE INDEX ON :%s(%s)" % (k,a))
            
    def add_default_constraint_set(self, labels):
        """SETS IRI and short_form as uniq, indexes label"""
        uniqs = {}
        indexes = {}
        for l in labels:
            uniqs[l] = ['IRI', 'short_form']
            indexes[l] = ['label']
        self.add_constraints(uniqs, indexes)
        self.commit()
            
    def add_node(self, labels, IRI, attribute_dict = {}):
        statement = "MERGE (n:%s { IRI: '%s' }) " % ((':'.join(labels)), IRI)
        statement += self._set_attributes_from_dict(var = 'n', attribute_dict = attribute_dict)
        self.statements.append(statement)
    
    def update_from_obograph(self, file_path = '', url = ''):
        """Update property and class nodes from an OBOgraph file"""
        """(currently does not distinguish OPs from APs!)
        """
        
        ## Warns if a class in-use has been obsoleted?  - Punt this to other code.
        if file_path:   
            f = open(file_path, 'r')
            obographs = json.loads(f.read())
            primary_graph = obographs['graphs'][0]
        elif url:
            r = requests.get(url)
            if r.status_code == 200:
                obographs = r.json()
                primary_graph = obographs['graphs'][0] # Add a check for success here!
        else:
            warnings.warn('Please provide a file_path or a URL')
            return False

        for node in primary_graph['nodes']:
            IRI = node['id']
            attribute_dict = {}
            if 'type' in node.keys():
                if node['type'] == 'CLASS':
                    labels = ['Class']
                if node['type'] == 'PROPERTY':    
                    labels = ['Property']
                else:
                    continue
            m = re.findall('.+(#|/)(.+?)$', node['id'])
            attribute_dict['short_form'] =  m[0][1]
            if 'lbl' in node.keys(): attribute_dict['label']=  node['lbl']
            if 'meta' in node.keys():
                if 'deprecated' in node['meta'].keys():
                    attribute_dict['is_obsolete'] = node['meta']['deprecated']
            self.add_node(labels, IRI, attribute_dict)
        ### For effeciency - could try concatenating statements.
            
        
    def update_from_flybase(self, load_list = []):            
            fbc = get_fb_conn()
            cursor = fbc.cursor()
            # STUB
            query = "SELECT f.uniquename, f.name, f.is_obsolete from feature f " \
            "JOIN cvterm typ on f.type_id = typ.cvterm_id " 
            if load_list:
                load_list_string = "'" + "','".join(load_list) + "'"
                query += "WHERE f.uniquename in (%s) " % load_list_string
            else:
                query += "WHERE typ.name in ('gene', " \
                "'transposable_element_insertion_site', 'transgenic_transposon') "
            
            cursor.execute(query)
            dc = dict_cursor(cursor)
            for d in dc:
                IRI = map_iri('fb') +  d['uniquename']
                attribute_dict = {}
                attribute_dict['short_form'] = d['uniquename']
                attribute_dict['is_obsolete'] = bool(d['is_obsolete'])       
                self.add_node(labels = ['Class', 'Feature'],
                              IRI = IRI,
                              attribute_dict = attribute_dict)
            cursor.close()
            fbc.close()
    
    def update_current_features_from_FlyBase(self):
        s = ["MATCH (f:Feature:Class) return f.short_form"]
        r = self.nc.commit_list(s)    
        features = [result['row'][0] for result in r[0]['data']]
        self.update_from_flybase(load_list = features)


# Specs for a fb_feature_update
## Pull current feature nodes from DB
 #   query = "SELECT uniquename, name, is_obsolete from feature"

#class fb_feature_update(kb_writer):   
    

# def add_ind(self, iri, short_form, label, synonyms = [], additional_attributes = {}):
#     out = "MERGE (i:Individual { IRI: '%s'} ) " \
#             "SET i.short_form = '%s' " \
#             "SET i.label = '%s' " % (iri, short_form, label)
#     if synonyms:
#             out += "SET i.synonyms = %s  " % str(synonyms)     
#     out += self._set_attributes_from_dict('i', additional_attributes)
#     return out

# def add_relation_node(self, iri, short_form, label):
#     return "MERGE (i:Relation { IRI: '%s'} ) " \
#             "SET i.short_form = '%s' " \
#             "SET i.label = '%s' " % (iri, short_form, label)
