'''
Created on 11 Aug 2016
This script moves content directly between the VFB_neo_KB and VFB_neo_Prod.

Background
* KB is the master representation of the VFB knowledge base.  
  It contains only minimal representation of referenced ontologies:
  Each class is a node with label (for reference only), short_form, full URI & version.
  In future vfb.owl will be generated directly from this DB.
* Prod is an extended version of OLS neo4J + vfb.owl & various other support ontologies
  The extension consists of some side loaded content from vfb.owl (pending OLS support) + 
  content directly translated from the KB (moved by this script).

Generic loading

Round 1: add triples using merge, specifying short_form + 'labels'.
Need to be careful here.  May be best to have 2 steps: Add nodes via
merge. Then add triple via merge.
Round 2: Add properties to nodes thus added.

Exceptions:  
Classes - just use short_form for merge.  Don't load any other
properties or labels.  (i.e. no round 2)

Don't load INSTANCEOF edges for anatomical individuals.  Loading this will
=> redundant types.  Needs a way to specify anatomical individuals

Potential issue:  Dumping an entire DB via bolt is going to => very large data transfers.  
Can these be paged or streamed?


@author: davidos
'''


from neo4j.v1 import GraphDatabase, basic_auth
import sys
from uk.ac.ebi.vfb.neo4j.tools import neo4j_connect 

d = GraphDatabase.driver(url='bolt://blanik.inf.ed.ac.uk:7687',
                         auth=basic_auth("neo4j", "DL1adPN"))
session = d.session()
result = session.run("MATCH (s)-[r]-(o) RETURN s,r,o limit 10")

# Assuming can handle full dump of triples!
# Assuming everything can be done with short_forms and edge types (can't yet).

class node2cypher:
    def __init__(self, node):
        self.node = node
        self.short_form = node.properties['short_form']
        self.id = node.id
    
    def gen_node_ref_statement(self, node_key):
        try:
            return "(%s:%s { short_form : '%s'}) " % (node_key, ':'.join(self.node.labels), 
                                                     self.node.properties['short_form'])
        except:
            pass  # Work on this!
        
    def get_node_merge_statement(self, node_key):
        return "MERGE %s " % self.gen_node_ref_statement(node_key)
    
    def get_node_match_statement(self, node_key):
        return "MATCH %s " % self.gen_node_ref_statement(node_key)
    
    def get_property_set_statements(self, node_key):
        if self.node.properties:
            out = self.get_node_match_statement(node_key)
            for k,v in self.node.properties.items():
                out += "SET node_key.%s = '%s" % (k,v)
            return out
        else:
            return False
            
class triple2cypher():
    def __init__(self, triple):
        self.triple = triple
        self.obj = node2cypher(triple['o'])
        self.subj = node2cypher(triple['o'])
        self.edge_type = triple['r'].type
        

    def gen_triple_merge_statement(self):
        return "%s, %s MERGE (s)-[%s]-(o)" % (self.subj.get_node_match_statement('s'),
                                                self.subj.gen_node_ref_statement('o'),
                                                self.edge_type)
        
      
    
    


statements = []
while result:
    t2c = triple2cypher(result.single())
    nodes = [t2c.subj, t2c.obj]
    for n in nodes:
        statements.append(n.get_node_merge_statement('n'))
        if 'Class' not in n.labels:
            statements.append(n.get_property_set_statements())
    # Add conditional here to avoid adding triples when Individual is anatomical and relation type is INSTANCEOF
    
    statements.append(t2c.gen_triple_merge_statement())
    
    
nc.commit_list_in_chunks()         


result.peek()

Out[74]: <Record s=<Node id=284 labels={'Class'}
properties={'short_form': 'FBbi_00000224', 'label': 'computer
graphic'}> r=<Relationship id=212035 start=121660 end=284
type='Related' properties={'short_form': 'OBI_0000312', 'label': 'has
specified output', 'uri':
'http://purl.obolibrary.org/obo/OBI_0000312'}> o=<Node id=121660
labels={'Individual', 'VFB'} properties={'short_form':
'VFBc_00030868', 'label': 'INP - painted domain JFRC2_c', 'iri':
'http://www.virtualflybrain.org/owl/VFBc_00030868', 'ontology_name':
'vfb'}>>

peek = result.peek()

peek['s'].properties
Out[79]: {'label': 'computer graphic', 'short_form': 'FBbi_00000224'}
