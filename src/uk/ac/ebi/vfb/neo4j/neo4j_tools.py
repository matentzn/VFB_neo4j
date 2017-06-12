#!/usr/bin/env python3

import requests
import json
import warnings
import re
#import token


'''
Created on 4 Feb 2016

Tools for connecting to the neo4j REST API

@author: davidos
'''

#Could also use py2neo, but this is a bit heavy duty for some uses



def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i+n]
 

        
class neo4j_connect():
    """Thin layer over REST API to hold connection details, 
    handle multi-statement POST queries."""
    # In order to support returning graphs, we need 
    
    def __init__(self, base_uri, usr, pwd):
        self.base_uri=base_uri
        self.usr = usr
        self.pwd = pwd
        self.test_connection()
       
    def commit_list(self, statements, return_graphs = False):
        """Commit a list of statements to neo4J DB via REST API.
        Prints requests status and warnings if any problems with commit.
            - statements = list of cypher statements as strings
            - return_graphs, optionally specify graphs to be returned in JSON results.
        Errors prompt warnings, not exceptions, and cause return  = FALSE.
        Returns results list or False if any errors are encountered."""
        cstatements = []
        if return_graphs:
            for s in statements:
                cstatements.append({'statement': s, "resultDataContents" : [ "row", "graph" ]})
        else:        
            for s in statements:
                cstatements.append({'statement': s}) # rows an columns are returned by default.
        payload = {'statements': cstatements}
        response = requests.post(url = "%s/db/data/transaction/commit" 
                                 % self.base_uri, auth = (self.usr, self.pwd) ,
                                  data = json.dumps(payload))
        if self.rest_return_check(response):
            return response.json()['results']
        else:
            return False
        
        
    def commit_list_in_chunks(self, statements, verbose=False, chunk_length=1000):
        """Commit a list of statements to neo4J DB via REST API, split into chunks.
        cypher_statments = list of cypher statements as strings
        base_uri = base URL for neo4J DB
        Default chunk size = 100 statements. This can be overridden by KWARG chunk_length.
        Returns a list of results.
        """
        chunked_statements = chunks(l = statements, n=chunk_length)
        chunk_results = []
        for c in chunked_statements:
            if verbose:
                print("Processing chunk starting with: %s" % c[0])
            r = self.commit_list(c)
            if type(r) == list:
                chunk_results.extend(r)
            else:
                chunk_results.append(r)
        return chunk_results
        
    def rest_return_check(self, response):
        """Checks status response to post. Prints warnings to STDERR if not OK.
        If OK, checks for errors in response. Prints any present as warnings to STDERR.
        Returns True STATUS OK and no errors, otherwise returns False.
        """
        if not (response.status_code == 200):
            warnings.warn("Connection error: %s (%s)" % (response.status_code, response.reason))
        else:
            j = response.json()
            if j['errors']:
                for e in j['errors']:
                    warnings.warn(str(e))
                return False
            else:
                return True
            
    def test_connection(self):
        statements = ["MATCH (n) RETURN n LIMIT 1"]
        if self.commit_list(statements):
            return True
        else:
            return False
        
    def list_all_node_props(self):
        r = self.commit_list(['MATCH (n) with keys(n) AS kl UNWIND kl as k RETURN DISTINCT k'])
        d = results_2_dict_list(r)
        return [x['k'] for x in d]
    
    def list_all_edge_props(self):
        r = self.commit_list(['MATCH ()-[r]-() with keys(r) AS kl UNWIND kl as k RETURN DISTINCT k'])
        d = results_2_dict_list(r)
        return [x['k'] for x in d]
        
def results_2_dict_list(results):
    """Takes JSON results from a neo4J query and turns them into a list of dicts.
    """
    dc = []
    for n in results:
        if n:
            for d in n['data']:
                dc.append(dict(zip(n['columns'], d['row'])))
    return dc

def escape_string(strng):
    if type(strng) == str:
        strng = re.sub(r'\\', r'\\\\', strng)
        strng = re.sub("'", "\\'", strng)
        strng = re.sub('"', '\\"', strng)        
    return strng

def dict_2_mapString(d):
    """Converts a Python dict into a cypher map string.
    Only supports values of type: int, float, list, bool, string."""
    # Surely one of the fancier libraries comes with this built in!
    map_pairs = []
    for k,v in d.items():  
        if type(v) == (int):
            map_pairs.append("%s : %d" % (k,v))
        elif type(v) == float:   
            map_pairs.append("%s : %f " % (k,v))                   
        elif type(v) == str:
            map_pairs.append('%s : "%s"' % (k, escape_string(v)))           
        elif type(v) == list:                        
            map_pairs.append('%s : %s' % (k, str([escape_string(i) for i in v])))
        elif type(v) == bool:
            map_pairs.append("%s : %s" % (k, str(v)))                
        else: 
            warnings.warn("Can't use a %s as an attribute value in Cypher. Key %s Value :%s" 
                          % (type(v), k, (str(v))))
    
    return "{ " + ' , '.join(map_pairs) + " }"

class neo4jContentMover:
    """A wrapper for methods that move content between two neo4J databases.
    Limitation:  The database being pulled from must be Neo4j 3.n + (2.n lacks
    the properties function used here)."""
    
    def __init__(self, From, To):
        """From: a neo4jConnect object for interacting with a neo4j DB to pull content from (neo 3.n+)
        To: a neo4jConnect object for interacting with a neo4j DB to load content (2.n+)"""
        self.From = From
        self.To = To
        
    def move_nodes(self, match, key, chunk_length = 2000, verbose = True, test_mode = False):
        """match = any match statement in which a node to move is specified with variable n.
        key = attribute used in merge statements to non-redundantly add content. must be present
        in matched nodes.
        Optionally set commit chunk length, verbosity, test mode (limit 100)
        """
        
        ret = " RETURN n.%s AS key, labels(n) AS labels , " \
                "properties(n) as properties" % key
        

        if test_mode:
            ret += " limit 100"   
        results = self.From.commit_list([match + ret])                                            
        nodes = results_2_dict_list(results)
        s = []
        for n in nodes:
            attribute_map = dict_2_mapString(n['properties'])
            label_string = ':'.join(n['labels'])
            s.append('MERGE (n:%s { %s : "%s" }) SET n = %s' % (label_string, 
                                                                  key, n['properties'][key], 
                                                                  attribute_map)) 
        self.To.commit_list_in_chunks(statements = s,
                                       verbose = verbose, 
                                       chunk_length = chunk_length)
                    
    def move_edges(self, match, node_key, edge_key = '', chunk_length = 2000, 
                   verbose = True, test_mode = False):
        """
        match = any match statement in which an edge (triple) is specified with variables s,r,o
        node_key = key used to match/merge to add new content
        f = neo4j_connect object for KB that content is being moved from
        t = neo4j_connect object for KB that content is being move to.
        Optionally set commit chunk length
        """
        
        ret = "RETURN s.%s AS subject, labels(s) as slab, type(r) AS reltype, " \
                "properties(r) AS relprops, o.%s AS object, labels(o) AS olab " % (node_key, node_key)
        if test_mode:
            ret += " limit 100"
        results = self.From.commit_list([match + ret])                                            
        edges = results_2_dict_list(results)
        s = []
        for e in edges:
            attribute_map = dict_2_mapString(e['relprops'])
            rel = e['reltype']
            slab_string = ':'+':'.join(e['slab'])
            olab_string = ':'+':'.join(e['olab'])
            if edge_key:
                if edge_key in e['relprops'].keys():
                    edge_restriction = "{ %s : '%s' }" % (edge_key, e['relprops'][edge_key])
                else :
                    warnings.warn("Matched edge lacks specified edge_key (%s)"  % (edge_key))
            else:
                edge_restriction = ""
            s.append("MERGE (s%s { %s : '%s'}) " \
                     "MERGE (o%s { %s : '%s'}) " \
                     "MERGE (s)-[r:%s %s]->(o) " \
                     "SET r = %s" % (slab_string, node_key, e['subject'], 
                                     olab_string, node_key, e['object'], 
                                     rel, edge_restriction,
                                     attribute_map))
        self.To.commit_list_in_chunks(statements = s,
                                       verbose = verbose, 
                                       chunk_length = chunk_length)  
                      
                       
    
    
    

        
            
            


        
            
                
        
    
    