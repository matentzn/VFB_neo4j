'''
Created on Oct 24, 2016

@author: davidos
'''
import unittest
from uk.ac.ebi.vfb.neo4j.neo4j_tools import neo4j_connect

class Test(unittest.TestCase):
    
    def setUp(self):
        self.nc = neo4j_connect('http://localhost:7474', 'neo4j', 'neo4j')

    def test_neo_create_node(self): 
        # Quite a minimal test...
        self.assertTrue(expr = self.nc.commit_list(["CREATE (t:Test { fu: 'bar' })"]),
                        msg = "fu")
        
    def tearDown(self):
        self.nc.commit_list(["MATCH (t:Test { fu: 'bar' }) DELETE t"])

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()