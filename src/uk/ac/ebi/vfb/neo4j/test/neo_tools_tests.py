'''
Created on Oct 24, 2016

@author: davidos
'''
import unittest
from ..tools import neo4j_connect

class Test(unittest.TestCase):

    def testName(self):
        nc = neo4j_connect('http://localhost:7474', 'neo4j', 'neo4j')
        self.assertTrue(expr = nc.commit_list("CREATE (t:Test { fu: 'bar' })"),
                        msg = "fu")

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()