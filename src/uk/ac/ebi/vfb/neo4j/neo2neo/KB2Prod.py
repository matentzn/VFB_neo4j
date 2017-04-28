'''
Created on Apr 28, 2017

@author: davidos
'''


from ..neo4j_tools import neo4j_connect, results_2_dict, dict_2_mapString
import sys

"""
A simple script to take all non OWL nodes (not :Class, :Property, or :Individual)
and all non-owl edges (not :Related, :INSTANCEOF, :SUBCLASSOF)
and move them from KB to prod.
"""

kb = neo4j_connect(sys.argv(1), sys.argv(2), sys.argv(3))
prod = neo4j_connect(sys.argv(4), sys.argv(5), sys.argv(6))

results = kb.commit_list(["MATCH (n) " \
                    "WHERE not('Class' IN labels(n)) " \
                    "AND not('Individual' IN labels(n)) " \
                    "AND not('Property' IN labels(n))  " \
                    "RETURN  labels(n) AS labels , " \
                    "properties(n) as properties;"])

nodes = results_2_dict(results)
s = []
for n in nodes:
    
    attribute_map = dict_2_mapString(n[0]['properties'])
    label_string = ':'.join(n[0]['labels'])
    # Hmmm - would be better with unique attribute for n.:
    s.append("CREATE (n:%s) SET n = %s" % (label_string, attribute_map)) 

prod.commit_list(s)

results = kb.commit_list(["MATCH (n)-[r]->(m) " \
                          "WHERE not(type(r) IN ['INSTANCEOF', 'Related']) " \
                          "RETURN n.IRI AS subject, type(r) AS reltype, " \
                          "properties(r) AS relprops, m.IRI AS object "])

edges = results_2_dict(results)

for e in edges:
    attribute_map = dict_2_mapString(e['relprops'])
    rel = e['reltype']
    s.append(["MERGE (s { iri : '%s'})-[r:%s]-(o { iri : '%s'})) SET r = %s"
              % (e['subject'], rel, e['object'], attribute_map)])
    
prod.commit_list(s)

