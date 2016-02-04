#!/usr/bin/env python3

import sys
import re
from ..tools import neo4j_connect

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

base_uri = sys.argv[1]
usr = sys.argv[2]
pwd = sys.argv[3]
nc = neo4j_connect(base_uri, usr, pwd)

statements = ["MATCH (n)-[r:Related]->(m) RETURN n.obo_id, r.label, m.obo_id"]
try:
    r = nc.commit_list(statements)
except: Exception()
    
rj= r.json()
triples = rj['results'][0]['data']
statements = []
# Iterate over, making named edges for labels (sub space for _)
for t in triples:
    subj = t['row'][0]
    rel = re.sub(' ', '_', t['row'][1]) # In case any labels have spaces
    obj = t['row'][2]
    statements.append("MATCH (n:Class),(m:Class) WHERE n.obo_id = '%s' and m.obo_id = '%s' MERGE (n)-[r:%s]->(m)" % (subj, obj, rel))


print("processing %s statements" % len(statements))

nc.commit_list_in_chunks(statements, verbose = True, chunk_length = 1000)


