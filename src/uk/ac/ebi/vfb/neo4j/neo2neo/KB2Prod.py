'''
Created on Apr 28, 2017

@author: davidos
'''


from ..neo4j_tools import neo4j_connect, results_2_dict_list, dict_2_mapString
import sys

"""
A simple script to take all non OWL nodes (not :Class, :Property, or :Individual)
and all non-owl edges (not :Related, :INSTANCEOF, :SUBCLASSOF)
and move them from KB to prod.
"""

kb = neo4j_connect(sys.argv(1), sys.argv(2), sys.argv(3))
prod = neo4j_connect(sys.argv(4), sys.argv(5), sys.argv(6))



def move_nodes(f, t, match, key):
    """match = any match statement in which a node to move is specfied with variable n
    f = neo4j_connect object for KB that content is being moved from
    t = neo4j_connect object for KB that content is being move to.
    """
    ret = " RETURN labels(n) AS labels , " \
            "properties(n) as properties"
    results = f.commit_list([match + ret])                                            
    nodes = results_2_dict_list(results)
    s = []
    for n in nodes:
    
        attribute_map = dict_2_mapString(n[0]['properties'])
        label_string = ':'.join(n[0]['labels'])
        # Hmmm - would be better with unique attribute for n. iri ?
        s.append("MERGE (n:%s) WHERE n.%s = %s SET n = %s" % (label_string, key, n[key], attribute_map)) 
        t.commit_list(s)
        
def move_edges(f, t, match, node_key, edge_key = ''):
    """
    match = any match statement in which an edge is specified with variables s,r,o
    key = key used to add new content
    f = neo4j_connect object for KB that content is being moved from
    t = neo4j_connect object for KB that content is being move to."""
    
    ret = "RETURN n.IRI AS subject, type(r) AS reltype, " \
            "properties(r) AS relprops, m.IRI AS object "       
    results = f.commit_list([match + ret])                                            
    edges = results_2_dict_list(results)
    s = []
    for e in edges:
        attribute_map = dict_2_mapString(e['relprops'])
        rel = e['reltype']
        if edge_key:
            edge_restriction = "{ %s : '%s' }" % ()
        s.append("MERGE (s { %s : '%s'})-[r:%s %s]->(o { %s : '%s'})) SET r = %s"  # Getting too complicated.  Use format string!
                  % (node_key, e['subject'], rel, edge_restriction, node_key, e['object'], attribute_map))
    t.commit_list(s)                         
                              
    

## move non-OWL content
node_match = "MATCH (n) " \
        "WHERE not('Class' IN labels(n)) " \
        "AND not('Individual' IN labels(n)) " \
        "AND not('Property' IN labels(n))  "
        
move_nodes(f = kb, t = prod, match = node_match, key = 'iri',)

edge_match = "MATCH (s)-[r]->(o) " \
             "WHERE not(type(r) IN ['INSTANCEOF', 'Related']) " 

move_edges(f = kb, t = prod, match = edge_match, node_key = 'iri')

## move channels
 
node_match = "MATCH (:Class { label: 'channel'})-[:INSTANCEOF]-(n:Individual) "
move_nodes(f = kb, t = prod, match = node_match, key = 'iri')
edge_match = "MATCH (:Class { label: 'channel'})-[:INSTANCEOF]-(s:Individual) " \
             "WITH n MATCH (s)-[r]-(o) "
move_edges(f = kb, t = prod, match = edge_match, node_key = 'iri')
