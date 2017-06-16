'''
Created on Mar 6, 2017

@author: davidos

'''

from uk.ac.ebi.vfb.neo4j.lmb2neoKB.lmb_query_tools import get_conn
from uk.ac.ebi.vfb.neo4j.KB_tools import kb_owl_edge_writer
from uk.ac.ebi.vfb.curie_tools import map_iri
from uk.ac.ebi.vfb.neo4j.neo4j_tools import results_2_dict_list, chunks
import sys
import warnings
import re



"""Script to add voxel overlap to channel-channel edges"""

# Sketch:
## Based on OWL writing script, but without setting cut-off.
## SQL query needs to navigate one more edge: anat to channel.
## Or - the lookup could be done via Cypher.
# SQL query pulls back class:ind mapping
# Cypher query uses a path from the class through painted domain to the template 
# => correct painted domain ind (channel)
# shorter cypher query jumps over depcits to => individual channel for 




class map_gen():
    
    def __init__(self, cursor, edge_writer):
        self.cursor = cursor
        self.edge_writer = edge_writer
        self.nc = edge_writer.nc
        self.cursor.execute("SELECT a.shortFormID, sj.* " \
                       "FROM spatdist_jfrc sj " \
                       "JOIN neuron n ON (sj.idid=n.idid) " \
                       "JOIN owl_individual a ON (n.uuid=a.uuid)")
        self.neuron_neuropil_overlaps = self.cursor.fetchall() 


    def gen_BrainName_mapping(self):
        """Returns a dict mapping BrainName_abbv to channel."""
                
        self.cursor.execute("SELECT b2o.BrainName_abbv, oe.shortFormID, o.baseURI " \
                       "FROM BrainName_to_owl b2o " \
                       "JOIN owl_class oe ON (oe.id=b2o.owl_class_id) " \
                       "JOIN ontology o ON (o.id=oe.ontology_id)")
        
        BN_dict = {}
        dc = self.cursor.fetchall()
        for d in dc:
            BN = d["BrainName_abbv"]
            r = self.nc.commit_list(["MATCH (neuropil:Class)<-[:INSTANCEOF]-(a:Individual) " \
                    "<-[:Related { short_form : 'depicts'}]-(neuropil_channel:Individual)" \
                    "-[:in_register_with]->(bc) " \
                    "-[:Related { short_form : 'depicts'}]->(t:Individual)" \
                    "WHERE neuropil.short_form = '%s' " \
                    "AND t.label = 'JFRC2_template'" \
                    "RETURN neuropil_channel.iri " % d["shortFormID"]])
            nci = results_2_dict_list(r)
            if not nci:
                warnings.warn("No channel mapping for %s" % BN)
            elif len(nci) > 1:
                warnings.warn("Multiple channel mappings for %s" % BN)
            else:
                BN_dict[BN] = nci[0]["neuropil_channel.iri"]
        return BN_dict
     
    

    def gen_neuron2channel(self):
        """Function to add assertions of overlap to BrainName domains.  Currently works with a simple cutoff, but there is scope to modify this to at least specify a proportion of voxel size of domain."""
    
        # Map neuron 2 neuropil overlap 
    
        # Give this a better name
        # => anatomical individual -> overlaps by abbv.  Now we need channels!
        neurons = [d['shortFormID'] for d in self.neuron_neuropil_overlaps]
        chunked_neurons = chunks(l = neurons, n = 500)
        s = []
        rdl = []
        for c in chunked_neurons:
            s.append("MATCH (neuron_ind:Individual)<-[r:Related]-(neuron_channel:Individual) " \
                     "WHERE r.short_form = 'depicts' AND neuron_ind.short_form IN %s " \
                     "RETURN  neuron_ind.short_form, neuron_channel.iri" % str(c))  
            result = self.nc.commit_list(s)
        # Check if query returns
            if result:
                rdl.extend(results_2_dict_list(result))
        # Neuron to channel lookup
        neuron2channel = {}            
        for r in rdl:
            neuron2channel[r['neuron_ind.short_form']] = r['neuron_channel.iri']
            
        return neuron2channel
    

    
def main():
    
    c = get_conn(usr = sys.argv[4], pwd = sys.argv[4])
    cursor = c.cursor()
#    vfb = map_iri('vfb')
    obo = map_iri('obo')
    edge_writer = kb_owl_edge_writer(endpoint=sys.argv[1], usr=sys.argv[2], pwd=sys.argv[3])
    mg = map_gen(cursor, edge_writer)
    print("BrainName abbv channel mapping")
    BN_dict = mg.gen_BrainName_mapping() # {'AL_L': 'http://virtualflybrain.org/reports/VFBc_00030629', 'short_form': 'VFB_00011604' ...
    print("gen neuron neuropil mapping")
    neuron_neuropil_overlaps = mg.neuron_neuropil_overlaps # {'SMP_L': 11670, 
    print("Gen neuron to channel mapping")
    neuron2channel = mg.gen_neuron2channel() # neuron2channel[r['neuron_ind.short_form']] = r['neuron_channel.iri]']
    cursor.close()
    c.close() 
    
    # Iterate over neuron_neuropil_overlaps.  
    # Map neuron to a channel via neuron2channel
    # Map each key (apart from short_form and idid) to a channel via BN_dict.
    # If key ends in _L -> add overlap to attribute voxel_ccoverlap_left
    # If key ends in _R -> add overlap to attribute voxel_overlap_right
    # If key has no _L/R ending map to voxel_overlap_center.
    # (This may not be perfect, but will do for now).
    print ("Processing facts for addition.")
    for o in neuron_neuropil_overlaps:
        neuron_channel = neuron2channel[o['shortFormID']]
        for k,v in o.items():
            ad = {} # attribute for edge
            if k == 'shortFormID': continue # check keyword!
            elif k == 'idid': continue
            elif k == 'Exterior': continue
            else:
                neuropil_channel = BN_dict[k] # => channel iri
                # Only add edge if some overlap (But not setting cuttoff)
                if v:
                    if re.match('.+_L', k): ad['voxel_overlap_left'] = v
                    elif re.match('.+_R', k): ad['voxel_overlap_right'] = v
                    else: 
                        ad['voxel_overlap_center'] = v
                    edge_writer.add_fact(s = neuron_channel, 
                                     r = obo + 'RO_0002131', 
                                     o = neuropil_channel,
                                     edge_annotations = ad) # Extra properties are added if edge is already present
    
    print("Processing %d statements" % len(edge_writer.statements))
    edge_writer.commit(verbose = True, chunk_length = 2000)
    
           
                                 

if __name__ == "__main__":
    main()    
