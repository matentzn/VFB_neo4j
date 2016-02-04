#!/usr/bin/env python

import sys
import re
from neo4j_tools import commit_list, commit_list_in_chunks

"""A simple script to make edges named (typed) for relations from all edges of of type :Related.
Arg1 = base_uri or neo4J server
Arg2 = usr
Arg2 = pwd

This script relies on a uniqueness constraint being in place for OBO ids."""

# Current version makes all edges.  Might want to limit the types of edges made to those needed for graphing purposes.

# TODO: add in check of uniqueness constraint
# Use REST calls to /db/data/schema/

# TODO: Add checks for sucess of query
## Is return code = 200?  If not, fail and return code
## Does the json indicate an error?  If so, fail and return error message

base_uri = sys.argv[1]
usr = sys.argv[2]
pwd = sys.argv[3]

statements = ["MATCH (n)-[r:Related]->(m) RETURN n.obo_id, r.label, m.obo_id"]
r = commit_list(statements, base_uri, usr, pwd)

if not r:
    raise Exception('REST query error')
    
rj= r.json()
# die if no query fails
triples = rj['results'][0]['data']
statements = []
# Iterate over, making named edges for labels (sub space for _)
for t in triples:
    subj = t['row'][0]
    rel = re.sub(' ', '_', t['row'][1]) # In case any labels have spaces
    obj = t['row'][2]
    statements.append("MATCH (n:Class),(m:Class) WHERE n.obo_id = '%s' and m.obo_id = '%s' MERGE (n)-[r:%s]->(m)" % (subj, obj, rel))


print("processing %s statements" % len(statements))

commit_list_in_chunks(statements, base_uri, usr, pwd, chunk_length=1000)


