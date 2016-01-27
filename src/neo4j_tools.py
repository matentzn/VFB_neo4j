import requests
import json
import warnings

"""Add typing via anonymous class expressions from OWL file.
Requires uniqueness constraint on individual & class short_form_id."""


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in xrange(0, len(l), n):
        yield l[i:i+n]
        
def commit_list(cypher_statments, base_uri, usr, pwd):
    statements = []
    for s in cypher_statments:
        statements.append({'statement': s})
    payload = {'statements': statements}
    response = requests.post(url = "%s/db/data/transaction/commit" % base_uri, auth = (usr, pwd) , data = json.dumps(payload))
    if rest_return_check(response):
        return response.json()['results']
    else:
        return False
    
def rest_return_check(response):
    if not (response.status_code == 200):
        warnings.warn("Connection error: %s (%s)" % (response.status_code, response.reason))
    else:
        j = response.json()
        if j['errors']:
            for e in j['errors']:
                warnings.warn(str(e))
            return False
        else:
            return True
        
                
        
    
    