from .dbtools import FeatureMover
import warnings

# General strategy:
# 1. Merge on short_form
# 2. For each feature_expression - get features & pubs & FBex
# 3. Generate expression pattern or localization pattern class for each feature
# - depending on if protein or transcript.
# 4. Merge feature & pub + add typing and details.
# 5. Instantiate (merge) related types -> gene for features, adding details


endpoint = sys.argv[1]
usr = sys.argv[2]
pwd = sys.argv[3]

fm = FeatureMover(endpoint, usr, pwd)
def exp_gen(): return # code for generating and wiring up expression patterns


# Query feature_expression => pub feature and fbex
feps = fm.query_fb("SELECT ...")

# How to structure for batch

for fep in feps:
    # Split feps into batches for reasonable in clauses.
    # List comp => set X to add & fill
    pub = fep['pub'] 
    # -> merge pub + set details (if new)
    feat = fep['feature'] 
    names = 
    typ = type(feat)
    if typ == '':
        x = 1 # Create
    elif typ == '':
        x = 2
    else:
        warnings.warn()
        
    # merge feature + set details (if new)
    # Generate expression pattern/localization pattern
    exp_gen(fep)
    


    
    


