'''
Created on Oct 24, 2016

@author: davidos
'''
import unittest
from uk.ac.ebi.vfb.neo4j.neo4j_tools import neo4j_connect, neo4jContentMover, results_2_dict_list

class Test_commit(unittest.TestCase):
    
    def setUp(self):
        self.nc = neo4j_connect('http://localhost:7474', 'neo4j', 'neo4j')

    def test_neo_create_node(self): 
        # Quite a minimal test...
        self.assertTrue(expr = self.nc.commit_list(["CREATE (t:Test { fu: 'bar' })"]),
                        msg = "fu")
        
    def tearDown(self):
        self.nc.commit_list(["MATCH (t:Test { fu: 'bar' }) DELETE t"])

class TestContentMover(unittest.TestCase):

    def setUp(self):
        To = neo4j_connect('http://localhost:7474', 'neo4j', 'neo4j')
        From = neo4j_connect('http://kb.virtualflybrain.org', 'neo4j', 'neo4j')
        self.ncm = neo4jContentMover(From=From, To=To)

    def testMoveNode(self):
        match = "MATCH (n:Class { label : 'adult brain' }) "
        self.ncm.move_nodes(match,
                            key='iri')

    def testMoveEdges(self):
        self.ncm.move_nodes(match="MATCH (n:Individual { short_form : 'VFB_00000001' })",
                            key='iri')
        self.ncm.move_nodes(match="MATCH (n:Class { label : 'neuron' }) ",
                            key='iri')
        self.ncm.move_edges(match="MATCH (s:Individual { short_form : 'VFB_00000001'})"
                                  "-[r]-(o:Class { label : 'neuron' })",
                            node_key='iri')
        query = self.ncm.To.commit_list(["MATCH (s:Individual { short_form : 'VFB_00000001' })"
                                        "-[r]-(o:Class { label : 'neuron' }) "
                                        "RETURN type(r) as rtype"])
        query_results = results_2_dict_list(query)
        for q in query_results:
            assert q['rtype'] == 'INSTANCEOF'

    def testMoveNodeLabels(self):
        self.ncm.To.commit_list(["CREATE (n { short_form : 'VFB_00000002' })"])
        self.ncm.move_node_labels(match="MATCH (n { short_form : 'VFB_00000002' })",
                                  node_key='short_form')
        query = self.ncm.To.commit_list(["MATCH (n { short_form : 'VFB_00000002' })"
                                         "RETURN labels(n) as nlab"])
        query_results = results_2_dict_list(query)

        assert 'Individual' in query_results[0]['nlab']

    def tearDown(self):
        #self.ncm.To.commit_list(["MATCH (x)-[r]-(y) DELETE r", "MATCH (n) delete (n)"])
        return





if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
