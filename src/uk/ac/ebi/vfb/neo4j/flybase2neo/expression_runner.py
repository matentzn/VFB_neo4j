from .dbtools import FeatureRelationship, NameFeatures, FeatureType
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

relater = FeatureRelationship(endpoint, usr, pwd)
namer = NameFeatures(endpoint, usr, pwd)
typer = FeatureType(endpoint, usr, pwd)
def exp_gen(): return # code for generating and wiring up expression patterns


# Query feature_expression => pub feature and fbex
feps = relater.run_query("SELECT ...")

for fep in feps:
    pub = fep['pub'] 
    # -> merge pub + set details (if new)
    feat = fep['feature'] 
    names = namer(feat)
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
    


    
    


