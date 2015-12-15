import sys
import re

"""Add typing via anonymous class expressions from OWL file.
Requires uniqueness constraint on individual & class short_form_id."""

base_uri = sys.argv[1]
usr = sys.argv[2]
pwd = sys.argv[3]

def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in xrange(0, len(l), n):
        yield l[i:i+n]