import requests
import sys
import re

"""A simple script to make edges named (typed) for relations from all edges of of type :Related.
Arg1 = base_uri or neo4J server
Arg2 = usr
Arg2 = pwd

This script relies on a uniqueness constraint being in place for OBO ids."""

# Current version makes all edges.  Might want to limit the types of edges made to those needed for graphing purposes.


base_uri = sys.argv[1]
usr = sys.argv[2]
pwd = sys.argv[3]

def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in xrange(0, len(l), n):
        yield l[i:i+n]

payload = {'statements': [{'statement': 'MATCH (n)-[r:Related]->(m) RETURN n.obo_id, r.label, m.obo_id'}]}
rels = requests.post(url = "%s/db/data/transaction/commit" % base_url, auth = (usr, pwd) , data = json.dumps(payload))
rj= rels.json()
triples = rj['results'][0]['data']
chunked_triples = chunks(triples, 100) # Executing in chunks of 100, just 'cos.
for c in chunks:
    statements = []
    for t in c:
        subj = t['row'][0]
        rel = re.sub(' ', '_', t['row'][1]) # In case any labels have spaces
        obj = t['row'][2]
        statement.append({ 'statement': "MATCH (n:Class),(m:Class) WHERE n.obo_id = '%s' and m.obo_id = '%s' CREATE (n)-[r:%s]->(m)" % (subj, obj, rel)})
    payload = {'statements': statements}
    new_rel = requests.post(url = "%s/db/data/transaction/commit" % base_uri, auth = (usr, pwd) , data = json.dumps(payload))
    print new_rel.json()

