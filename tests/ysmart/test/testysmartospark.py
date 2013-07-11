#!/usr/bin/env python

"""
   Copyright (c) 2013 The Ohio State University.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

"""
import os.path
from ysmart.backend.ystree import GroupByNode, TwoJoinNode, SelectProjectNode, \
    TableNode, OrderByNode
from types import NoneType

'''
Testcases for YSmart front end
Created on May 7, 2013

@author: fathi
'''

from ysmart.backend import correlation, ystree, config

from ysmart.frontend.YSmartLexer import *  # import all the tokens
from ysmart.frontend.YSmartParser import *
from ysmart.frontend.sql2xml import toXml

from ysmart.frontend import sqltokens

import antlr3
import unittest

base_tests_path = './tests/ysmart/test'

class Test(unittest.TestCase):


    def setUp(self):
        pass


    def tearDown(self):
        pass
 

    def test_select(self):
        self.sql2sparkScaffold("unit_tests/countries_select.sql")

    def test_countries_quilified_select(self):
        self.sql2sparkScaffold("unit_tests/countries_quilified_select.sql")

    def test_keywords_size(self):
        self.sql2sparkScaffold("unit_tests/keywords_size.sql")

    def test_keywords_start(self):
        self.sql2sparkScaffold("unit_tests/keywords_start.sql")

    def test_keywords_bracketed_start(self):
        self.sql2sparkScaffold("unit_tests/keywords_bracketed_start.sql")

    def test_ssb_q2_3(self) :
        self.sql2sparkScaffold("ssb_test/q2_3.sql")

    def test_ssb_q1_2(self) :
        self.sql2sparkScaffold("ssb_test/q1_2.sql")

    def test_ssb_q1_1(self) :
        self.sql2sparkScaffold("ssb_test/q1_1.sql")

    def test_ssb_q4_2(self) :
        self.sql2sparkScaffold("ssb_test/q4_2.sql")

    def test_ssb_q2_2(self) :
        self.sql2sparkScaffold("ssb_test/q2_2.sql")

    def test_ssb_q3_3(self) :
        self.sql2sparkScaffold("ssb_test/q3_3.sql")

    def test_ssb_q1_3(self) :
        self.sql2sparkScaffold("ssb_test/q1_3.sql")

    def test_ssb_q3_2(self) :
        self.sql2sparkScaffold("ssb_test/q3_2.sql")

    def test_ssb_q2_1(self) :
        self.sql2sparkScaffold("ssb_test/q2_1.sql")

    def test_ssb_q3_1(self) :
        self.sql2sparkScaffold("ssb_test/q3_1.sql")

    def test_ssb_q3_4(self) :
        self.sql2sparkScaffold("ssb_test/q3_4.sql")

    def test_ssb_q4_1(self) :
        self.sql2sparkScaffold("ssb_test/q4_1.sql")

    def test_ssb_q4_3(self) :
        self.sql2sparkScaffold("ssb_test/q4_3.sql")

    def test_tpch3query(self) :
        self.sql2sparkScaffold("tpch_test/tpch3query.sql")

    def test_tpch21query(self) :
        self.sql2sparkScaffold("tpch_test/tpch21query.sql")

    def test_tpch6query(self) :
        self.sql2sparkScaffold("tpch_test/tpch6query.sql")

    def test_tpch18query(self) :
        self.sql2sparkScaffold("tpch_test/tpch18query.sql")

    def test_tpch17query(self) :
        self.sql2sparkScaffold("tpch_test/tpch17query.sql")

    def test_tpch1query(self) :
        self.sql2sparkScaffold("tpch_test/tpch1query.sql")

    def test_tpch10query(self) :
        self.sql2sparkScaffold("tpch_test/tpch10query.sql")

    def test_tpch5query(self) :
        self.sql2sparkScaffold("tpch_test/tpch5query.sql")

    #----------------------------------------------------
    # Tests for YStree nodes
    @unittest.skip("skipping until the bug in parser is fixed")
    def test_select_ystree(self) :
        self.ystree2graph("unit_tests/countries_select.sql", "unit_tests/countries.schema")

    def test_countries_quilified_select_ystree(self):
        self.ystree2graph("unit_tests/countries_quilified_select.sql", "unit_tests/countries.schema")

    @unittest.skip("skipping until the bug in parser is fixed")
    def test_keywords_size_ystree(self):
        self.ystree2graph("unit_tests/keywords_size.sql", "unit_tests/countries.schema")

    @unittest.skip("skipping until the bug in parser is fixed")
    def test_keywords_start_ystree(self):
        self.ystree2graph("unit_tests/keywords_start.sql", "unit_tests/countries.schema")

    @unittest.skip("skipping until the bug in parser is fixed")
    def test_keywords_bracketed_start_ystree(self):
        self.ystree2graph("unit_tests/keywords_bracketed_start.sql", "unit_tests/countries.schema")

    def test_ssb_q2_3ystree(self) :
        self.ystree2graph("ssb_test/q2_3.sql", "ssb_test/ssb.schema")

    def test_ssb_q1_2ystree(self) :
        self.ystree2graph("ssb_test/q1_2.sql", "ssb_test/ssb.schema")

    def test_ssb_q1_1ystree(self) :
        self.ystree2graph("ssb_test/q1_1.sql", "ssb_test/ssb.schema")

    def test_ssb_q4_2ystree(self) :
        self.ystree2graph("ssb_test/q4_2.sql", "ssb_test/ssb.schema")

    def test_ssb_q2_2ystree(self) :
        self.ystree2graph("ssb_test/q2_2.sql", "ssb_test/ssb.schema")

    def test_ssb_q3_3ystree(self) :
        self.ystree2graph("ssb_test/q3_3.sql", "ssb_test/ssb.schema")

    def test_ssb_q1_3ystree(self) :
        self.ystree2graph("ssb_test/q1_3.sql", "ssb_test/ssb.schema")

    def test_ssb_q3_2ystree(self) :
        self.ystree2graph("ssb_test/q3_2.sql", "ssb_test/ssb.schema")

    def test_ssb_q2_1ystree(self) :
        self.ystree2graph("ssb_test/q2_1.sql", "ssb_test/ssb.schema")

    def test_ssb_q3_1ystree(self) :
        self.ystree2graph("ssb_test/q3_1.sql", "ssb_test/ssb.schema")

    def test_ssb_q3_4ystree(self) :
        self.ystree2graph("ssb_test/q3_4.sql", "ssb_test/ssb.schema")

    def test_ssb_q4_1ystree(self) :
        self.ystree2graph("ssb_test/q4_1.sql", "ssb_test/ssb.schema")

    def test_ssb_q4_3ystree(self) :
        self.ystree2graph("ssb_test/q4_3.sql", "ssb_test/ssb.schema")

    def test_tpch3queryystree(self) :
        self.ystree2graph("tpch_test/tpch3query.sql", "tpch_test/tpch.schema")

    def test_tpch21queryystree(self) :
        self.ystree2graph("tpch_test/tpch21query.sql", "tpch_test/tpch.schema")

    def test_tpch6queryystree(self) :
        self.ystree2graph("tpch_test/tpch6query.sql", "tpch_test/tpch.schema")

    def test_tpch18queryystree(self) :
        self.ystree2graph("tpch_test/tpch18query.sql", "tpch_test/tpch.schema")

    def test_tpch17queryystree(self) :
        self.ystree2graph("tpch_test/tpch17query.sql", "tpch_test/tpch.schema")

    def test_tpch1queryystree(self) :
        self.ystree2graph("tpch_test/tpch1query.sql", "tpch_test/tpch.schema")

    def test_tpch10queryystree(self) :
        self.ystree2graph("tpch_test/tpch10query.sql", "tpch_test/tpch.schema")

    def test_tpch5queryystree(self) :
        self.ystree2graph("tpch_test/tpch5query.sql", "tpch_test/tpch.schema")
    #################################
    
    def sql2sparkScaffold(self, relative_path):
        '''
        Parses the given SQL file and compares the parse tree with the expected parse tree
        '''
        errorMsg = """Expected output and produced output do not match for %s:
                         Expected output: %s
                         ---------------------------------------------------------------
                         Produced output: %s
                         ---------------------------------------------------------------
                         Diff: %s
                         ==============================================================="""
        path = os.path.join(base_tests_path, relative_path)
        with open(path) as sqlFile:
            # TODO Meisam: Make the grammar case insensitive?
            query = sqlFile.read().upper()
            stringStream = antlr3.StringStream(query)
            lexer = YSmartLexer(stringStream)
            
            tokenStream = antlr3.CommonTokenStream(lexer)
            
            parser = YSmartParser(tokenStream)
            
            parse_tree = parser.start_rule()
            
            graphviz = visit_tree(parse_tree.tree)
            with open(path + ".dot", 'w') as dot_file:
                dot_file.write(graphviz)
                dot_file.flush()
                dot_file.close()
                

    def ystree2graph(self, query_path, schema_path):
        query_file_name = os.path.join(base_tests_path, query_path)
        schema_file_name = os.path.join(base_tests_path, schema_path)
        with open(query_file_name) as quey_file, open(schema_file_name) as schema_file:
            xml_str = toXml(quey_file)
            schema_str = schema_file.read()

        tree_node = ystree.ysmart_tree_gen(xml_str, schema_str)
        
        with open(query_file_name + ".ystree.dot", "w") as ystree_file:
            ystree_file.write(visit_ystree(tree_node))
            ystree_file.flush()

        optimized_tree_node = correlation.ysmart_correlation(tree_node)
        with open(query_file_name + ".ystree.opt.dot", "w") as ystree_opt_file:
            ystree_opt_file.write(visit_ystree(optimized_tree_node))
            ystree_opt_file.flush()

def print_tree(node, indent):
    print "TRAVERSE:", " " * (indent * 4), node , "\t", type(node)
    for child in node.children:
        print_tree(child, indent + 1)
    pass

TEMPLATE_STRING = '''
/*
 * This is auto-generated code
 */
 
import spark._
import SparkContext._

object SparkPi {

  def main(args:Array[String] {
    %s
  }
}
'''

def my_repr(object, str=None):
    if str == None:
        str = object
    return "%s<%s>" % (str, repr(object).split(' ')[-1][:-1])

def visit_tree(node):
    childs_strings = ''
    for child in node.children:
        childs_strings += '  "%s" -> "%s"\n%s' % (
                            my_repr(node), my_repr(child) , visit_tree(child))

    if node.type == 0:
        return "digraph finite_state_machine {\n  rankdir=LR\n  node [shape=plaintext]\n%s}" % childs_strings
    
    return childs_strings

def visit_ystree(node):
    childs_strings = ''
    
    if isinstance(node, GroupByNode) or isinstance(node, SelectProjectNode) or isinstance(node, OrderByNode):
        childs_strings += '  "%s" -> "%s"\n%s' % (
                        my_repr(node, node.__class__.__name__)
                        , my_repr(node.child, node.child.__class__.__name__)
                        , visit_ystree(node.child))
    elif isinstance(node, TwoJoinNode):
        childs_strings += '  "%s" -> "%s"\n%s' % (
                        my_repr(node, node.__class__.__name__)
                        , my_repr(node.left_child, node.left_child.__class__.__name__)
                        , visit_ystree(node.left_child))
        childs_strings += '  "%s" -> "%s"\n%s' % (
                        my_repr(node, node.__class__.__name__)
                        , my_repr(node.right_child, node.right_child.__class__.__name__)
                        , visit_ystree(node.right_child))
    elif isinstance(node, TableNode):
        childs_strings += '  "%s" -> "%s"\n' % (
                        my_repr(node, node.__class__.__name__)
                        , node.table_name)
    elif isinstance(node, NoneType):
        childs_strings += 'NONETYPE!!'
    else:
        raise RuntimeError(node.__class__.__name__)

    if node.parent is None:
        return "digraph finite_state_machine {\n  rankdir=LR\n  node [shape=box]\n%s}" % childs_strings
    
    return childs_strings

if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testParser']
    unittest.main()
