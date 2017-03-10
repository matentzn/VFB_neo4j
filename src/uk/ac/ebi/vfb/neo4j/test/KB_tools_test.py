'''
Created on Mar 8, 2017

@author: davidos
'''
import unittest
from ..KB_tools import kb_owl_edge_writer
from ..tools import neo4j_connect


class Test(unittest.TestCase):


    def setUp(self):
        self.edge_writer = kb_owl_edge_writer('http://localhost:7474', 'neo4j', 'neo4j')
        statements = ["MERGE (i1:Individual { 'IRI' : 'Aya' }), " \
                      "(r1:Relation: { 'IRI' : 'loves' }), (i2:Individual { 'IRI', 'Freddy' })"]
        self.edge_writer.nc.commit_list(statements)
        pass


    def tearDown(self):
        pass


    def test_add_owl(self):
        self.edge_writer.add_fact(s = 'Aya', r = 'loves', o = 'Freddy', edge_annotations = {} )
        self.edge_writer.add_fact(s = 'Aya', r = 'hates', o = 'mushrooms', edge_annotations = {})
        self.edge_writer.commit()
        pass


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()