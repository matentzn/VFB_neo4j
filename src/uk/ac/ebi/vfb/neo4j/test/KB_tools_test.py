'''
Created on Mar 8, 2017

@author: davidos
'''
import unittest
from ..KB_tools import kb_owl_edge_writer


class Test(unittest.TestCase):


    def setUp(self):
        self.edge_writer = kb_owl_edge_writer('http://localhost:7474', 'neo4j', 'neo4j')
        s = "MERGE (i1:Individual { IRI : 'Aya' })" \
            "MERGE (r1:Property { IRI : 'loves' }) " \
            "MERGE (i2:Individual { IRI: 'Freddy' }) "
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
        # Check that edge write
        self.edge_writer.add_fact(s = 'Aya', r = 'loves', o = 'Freddy', edge_annotations = { 'fu' : 'bar', 
                                                                                            'bin': ['bash', 'bash'],
                                                                                            'i' : 1,
                                                                                            'x' : True })  # TBA - put something in edge annotation
        self.edge_writer.commit() 
        assert self.edge_writer.test_edge_addition() == True  
        self.edge_writer.add_fact(s = 'Aya', r = 'loved', o = 'Freddy', edge_annotations = {} )
        self.edge_writer.commit()        
        assert self.edge_writer.test_edge_addition() == False  
        
    def test_add_anon_type_ax(self):
        pass
    
    def test_add_named_type_ax(self):
        pass


if __name__ == "__main__":
    unittest.main()