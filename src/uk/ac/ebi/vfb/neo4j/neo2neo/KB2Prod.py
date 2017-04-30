'''
Created on Apr 28, 2017

@author: davidos
'''


from ..neo4j_tools import neo4j_connect, neo4jContentMover
import sys

"""
A simple script to take all non OWL nodes (not :Class, :Property, or :Individual)
and all non-owl edges (not :Related, :INSTANCEOF, :SUBCLASSOF)
and move them from KB to prod.
"""

kb = neo4j_connect(sys.argv(1), sys.argv(2), sys.argv(3))
prod = neo4j_connect(sys.argv(4), sys.argv(5), sys.argv(6))

ncm = neo4jContentMover(kb,prod)
    

## move non-OWL content
non_owl_node_match = "MATCH (n) " \
        "WHERE not('Class' IN labels(n)) " \
        "AND not('Individual' IN labels(n)) " \
        "AND not('Property' IN labels(n))  "        
ncm.move_nodes(match = non_owl_node_match, key = 'iri')

non_owl_edge_match = "MATCH (s)-[r]->(o) " \
             "WHERE not(type(r) IN ['INSTANCEOF', 'Related', :SUBCLASSOF]) " 
ncm.move_edges(match = non_owl_edge_match, node_key = 'iri')

## move channels and directly associated edges
channel_match = "MATCH (:Class { label: 'channel'})-[:INSTANCEOF]-(n:Individual) "
ncm.move_nodes(match = channel_match, key = 'iri')

edge_match = "MATCH (:Class { label: 'channel'})-[:INSTANCEOF]-(s:Individual) " \
             "WITH n MATCH (s)-[r]-(o) "
ncm.move_edges( match = edge_match, node_key = 'iri')
