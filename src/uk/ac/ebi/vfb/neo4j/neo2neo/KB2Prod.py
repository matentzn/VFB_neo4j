'''
Created on Apr 28, 2017

@author: davidos
'''


from ..neo4j_tools import neo4j_connect, neo4jContentMover
import sys
from ..KB_tools import node_importer

"""
A simple script to take all non OWL nodes (not :Class, :Property, or :Individual)
and all non-owl edges (not :Related, :INSTANCEOF, :SUBCLASSOF)
and move them from KB to prod.
"""

kb = neo4j_connect(sys.argv[1], sys.argv[2], sys.argv[3])
prod = neo4j_connect(sys.argv[4], sys.argv[5], sys.argv[6])

ni = node_importer(sys.argv[4], sys.argv[5], sys.argv[6])
ni.add_default_constraint_set(['DataSet', 'Site', 'License' ])

ncm = neo4jContentMover(kb,prod)

## move channels and directly associated edges
### TESTED  ## Requires all IRI -> iri in KB
channel_match = "MATCH (:Class { label: 'channel'})-[:INSTANCEOF]-(n:Individual) " 
ncm.move_nodes(match = channel_match, key = 'iri', test_mode=False)

edge_match = "MATCH (:Class { label: 'channel'})-[:INSTANCEOF]-(s:Individual) " \
             "WITH s MATCH (s)-[r]-(o) "
ncm.move_edges( match = edge_match, node_key = 'iri', test_mode=False)

## move non-OWL content
## Requires: IRIs for everything to be moved (as iri)
non_owl_node_match = "MATCH (n) " \
        "WHERE not('Class' IN labels(n)) " \
        "AND not('Individual' IN labels(n)) " \
        "AND not('Property' IN labels(n))"     
           
ncm.move_nodes(match = non_owl_node_match, key = 'iri', test_mode=False)
 
non_owl_edge_match = "MATCH (s)-[r]->(o) " \
             "WHERE not(type(r) IN ['INSTANCEOF', 'Related', 'SUBCLASSOF']) " 
ncm.move_edges(match = non_owl_edge_match, node_key = 'iri', test_mode=False)


