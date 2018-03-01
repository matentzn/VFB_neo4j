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
temp_csv_filepath = sys.argv[4]

fm = FeatureMover(endpoint, usr, pwd, temp_csv_filepath)
def exp_gen(): return # code for generating and wiring up expression patterns

def add_pubs(): return
# Query feature_expression => pub feature and fbex
feps = fm.query_fb("SELECT pub.uniquename as fbrf, "
                   "f.uniquename as fbid, e.uniquename as fbex "
                   "FROM feature_expression fe "
                   "JOIN pub ON fe.pub_id = pub.pub_id"
                   "JOIN feature f ON fe.feature_id = f.feature_id"
                   "JOIN expression e ON fe.expression_id = e.expression_id")

gene_products = [f['fbid'] for f in feps]
pubs = [f['fbrf'] for f in feps]
taps = [f['fbex'] for f in feps]

# TODO - check paths through feature_relations table
fm.add_features(gene_products)
fm.addTypes2Neo(gene_products)
genes = fm.gp2Gene(gene_products)
transgenes = fm.gp2Transgene(gene_products)

fm.add_feature_relations(genes)
fm.add_feature_relations(transgenes)

# Construct gene expression pattern
# Construc transgene expression pattern

add_pubs(pubs)





    
    


