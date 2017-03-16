'''
Created on Mar 8, 2017

@author: davidos
'''
import unittest
from ..KB_tools import kb_owl_edge_writer, node_importer


class TestEdgeWriter(unittest.TestCase):


    def setUp(self):
        self.edge_writer = kb_owl_edge_writer('http://localhost:7474', 'neo4j', 'neo4j')
        s = []
        s.append("MERGE (i1:Individual { IRI : 'Aya' })" \
            "MERGE (r1:Property { IRI : 'loves' }) " \
            "MERGE (i2:Individual { IRI: 'Freddy' }) ")
        s.append("MERGE (i1:Individual { IRI : 'Aya' })" \
            "MERGE (r1:Property { IRI : 'daughter_of' }) " \
            "MERGE (i2:Individual { IRI: 'David' }) ")
        self.edge_writer.nc.commit_list([s])
        pass


    def tearDown(self):
        # TODO - add some deletions here
        s = "MATCH (i1:Individual { IRI : 'Aya' })-" \
        "[r1:Property { IRI : 'loves' }]->" \
        "(i2:Individual { IRI: 'Freddy' }) DELETE i1, r1, i2"
        self.edge_writer.nc.commit_list([s])
        pass


    def test_add_fact(self):

        self.edge_writer.add_fact(s = 'Aya', r = 'loves', o = 'Freddy', edge_annotations = { 'fu' : "b\a'r", 
                                                                                            'bin': ['bash', "b\a'sh"],
                                                                                            'i' : 1,                                                                                            'x' : True })  # TBA - put something in edge annotation
        self.edge_writer.commit() 
        assert self.edge_writer.test_edge_addition() == True  
        self.edge_writer.add_fact(s = 'Aya', r = 'loved', o = 'Freddy', edge_annotations = {} )
        self.edge_writer.commit()        
        assert self.edge_writer.test_edge_addition() == False  
        # Add test of added content?
        
    def test_add_anon_type_ax(self):
        pass
    
    def test_add_named_type_ax(self):
        pass

class TestNodeImporter(unittest.TestCase):

    def setUp(self):
        self.ni = node_importer('http://localhost:7474', 'neo4j', 'neo4j')
    
    def test_update_from_obograph(self):
        self.ni.update_from_obograph(self, url = 'https://raw.githubusercontent.com/VirtualFlyBrain/VFB_owl/master/src/owl/vfb_ext.owl')
        self.ni.commit()
        

if __name__ == "__main__":
    unittest.main()