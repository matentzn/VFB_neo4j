from owl2pdm_tools import ont_manager
from uk.ac.ebi.brain.core import Brain
import requests
import sys
import json
import re
import time

"""Add typing via anonymous class expressions from OWL file.
Requires uniqueness constraint on individual & class short_form_id."""

base_uri = sys.argv[1]
usr = sys.argv[2]
pwd = sys.argv[3]

vfb = Brain()
vfb.learn("/repos/VFB_owl/src/owl/fbbt_vfb_ind_prc_nr.owl") # Make this non-local
vom = ont_manager(vfb.getOntology())

def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i+n]

vom.typeAxioms2pdm(sfid = 'VFB_00005000')

# example = [{'isAnonymous': False, 'objectId': u'FBbt_00100247'},
#            {'relId': u'BFO_0000050', 'isAnonymous': True, 'objectId': u'FBbt_00003624'},
#            {'relId': u'BFO_0000050', 'isAnonymous': True, 'objectId': u'FBbt_00007011'},
#            {'relId': u'RO_0002292', 'isAnonymous': True, 'objectId': u'FBtp0014830'}]

# Simple to use. Only issue is resolution of short_form_ids.  This can be done as long as these are stored as attributes on relations.  These should be added in the process of adding named relations.  Check proposed schema on ticket...


payload = {'statements': [{'statement': 'MATCH (i:Individual) RETURN i'}]}
rels = requests.post(url = "%s/db/data/transaction/commit" % base_uri, auth = (usr, pwd) , data = json.dumps(payload))

# Get all inds by query

inds = vfb.getInstances("Thing", 0) # Could grab from

inds_chunked = chunks(l = inds, n = 100) # Chunks of 100, just 'cos

# Iterate over individuals, looking up types and adding them
for chunk in inds_chunked:
    statements = []
    for i in chunk:
        types = vom.typeAxioms2pdm(sfid = i)
        for t in types:
            if t['isAnonymous']:
                rel = re.sub(' ', '_', vfb.getLabel(t['relId']))
                # Using related link. Can denormalise with generic edge naming script.
                s = "MATCH (I:Individual), (C:Class) WHERE I.short_form = '%s'" \
                    "AND C.short_form = '%s' MERGE (I)-[r:Related {label: '%s' }]->(C)" \
                    % (i, t['objectId'], rel) # 
                statements.append({ 'statement': s }) 
    payload = {'statements': statements}
    new_rel = requests.post(url = "%s/db/data/transaction/commit" % base_uri, auth = (usr, pwd) , data = json.dumps(payload))
    time.sleep(0.01) # Add a brief pause to avoid hammering server too much.  Possibly not needed...

    
# Inds from graph (probably don't need this)
# payload = {'statements': [{'statement': 'MATCH (i:Individual) RETURN i.short_form'}]}
# ind_q_res = requests.post(url = "%s/db/data/transaction/commit" % base_uri, auth = (usr, pwd) , data = json.dumps(payload))
# rj= rels.json()
# inds = rj['results'][0]['data']










