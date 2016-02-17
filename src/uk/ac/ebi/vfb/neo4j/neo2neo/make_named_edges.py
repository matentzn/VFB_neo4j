#!/usr/bin/env python3

import sys
import re
from uk.ac.ebi.vfb.neo4j.tools import neo4j_connect

"""A simple script to make edges named (typed) for relations from all edges of of type :Related.
Arg1 = base_uri or neo4J server
Arg2 = usr
Arg2 = pwd

This script relies on a uniqueness constraint being in place for OBO ids.

Created on 4 Feb 2016

@author: davidos"""

# Current version makes all edges.  Might want to limit the types of edges made to those needed for graphing purposes.

# TODO: add in check of uniqueness constraint
# Use REST calls to /db/data/schema/


nc = neo4j_connect(base_uri = sys.argv[1], usr = sys.argv[2], pwd = sys.argv[3])

def make_name_edges(typ):
    statements = ["MATCH (n)-[r:%s]->(m) RETURN n.short_form, r.label, m.short_form" % typ]
    r = nc.commit_list(statements)        
    triples = [x['row'] for x in r[0]['data']]
    statements = []
    # Iterate over, making named edges for labels (sub space for _)
    for t in triples:
        subj = t[0]
        rel = re.sub(' ', '_', t[1]) # In case any labels have spaces
        obj = t[2]
        # Merge ensures this doesn't lead to duplicated edges if already present:
        statements.append("MATCH (n:Class),(m:Class) " \
                          "WHERE n.short_form = '%s' and m.short_form = '%s' " \
                          "MERGE (n)-[r:%s { type: '%s' }]->(m)" % (subj, obj, rel, typ)) 
    print("processing %s %s statements" % (len(statements), typ))    
    nc.commit_list_in_chunks(statements, verbose = True, chunk_length = 10000)

make_name_edges(typ='Related')
