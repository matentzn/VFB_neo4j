from owl2pdm_tools import ont_manager
from uk.ac.ebi.brain.core import Brain
import requests
import sys

"""Add typing via anonymous class expressions from OWL file.
Requires short_form ID attribute on rels."""

base_uri = sys.argv[1]
usr = sys.argv[2]
pwd = sys.argv[3]

vfb = Brain("/repos/VFB_owl/src/owl/fbbt_vfb_ind_prc_nr.owl")
vom = ont_manager(vfb.getOntology())

# vom.typeAxioms2pdm(sfid = 'VFB_00005000')

# example = [{'isAnonymous': False, 'objectId': u'FBbt_00100247'},
#            {'relId': u'BFO_0000050', 'isAnonymous': True, 'objectId': u'FBbt_00003624'},
#            {'relId': u'BFO_0000050', 'isAnonymous': True, 'objectId': u'FBbt_00007011'},
#            {'relId': u'RO_0002292', 'isAnonymous': True, 'objectId': u'FBtp0014830'}]

# Simple to use. Only issue is resolution of short_form_ids.  This can be done as long as these are stored as attributes on relations.  These should be added in the process of adding named relations.  Check proposed schema on ticket...


payload = {'statements': [{'statement': 'MATCH (i:Individual) RETURN i'}]}
rels = requests.post(url = "%s/db/data/transaction/commit" % base_uri, auth = (usr, pwd) , data = json.dumps(payload))

# Get all inds by query

inds = vfb.getInstances("Thing", 0)

# Iterate over individuals, looking up types and adding them

for i in ind:
    types = vom.typeAxioms2pdm(sfid = i)
    for t in types:
        if t['isAnonymous']:
            statements.append({ 'statement': "MATCH (I:Individual), [r], (C:Class) " /
                                "WHERE I.short_form = '%s' AND C.short_form = '%s' AND r.short_form = '%s' " /
                                "CREATE (I)-[r]->(C)"
                                 % (i, t['relId'], t['objectId']) # Need to do some testing of this.  Relation match may not make sense
                                    payload = {'statements': statements}
    new_rel = requests.post(url = "%s/db/data/transaction/commit" % base_uri, auth = (usr, pwd) , data = json.dumps(payload))

    
# Inds from graph (probably don't need this)
# payload = {'statements': [{'statement': 'MATCH (i:Individual) RETURN i.short_form'}]}
# ind_q_res = requests.post(url = "%s/db/data/transaction/commit" % base_uri, auth = (usr, pwd) , data = json.dumps(payload))
# rj= rels.json()
# inds = rj['results'][0]['data']






