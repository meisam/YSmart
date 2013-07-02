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
from mako.exceptions import RuntimeException

'''
Testcases for YSmart front end
Created on May 7, 2013

@author: fathi
'''
import sys
import difflib
sys.path.append("..")
sys.path.append("../sql2spark")
sys.path.append("../SQL2XML")
 
from YSmartLexer import *  # import all the tokens
from YSmartParser import *
from antlr3.tokens import CommonToken

import sqltokens

import antlr3
import unittest

class Test(unittest.TestCase):


    def setUp(self):
        pass


    def tearDown(self):
        pass
 

    def test_select(self):
        self.sql2sparkScaffold("./unit_tests/countries_select.sql")

    def test_countries_quilified_select(self):
        self.sql2sparkScaffold("./unit_tests/countries_quilified_select.sql")

    def test_keywords_size(self):
        self.sql2sparkScaffold("./unit_tests/keywords_size.sql")

    def test_keywords_start(self):
        self.sql2sparkScaffold("./unit_tests/keywords_start.sql")

    def test_keywords_bracketed_start(self):
        self.sql2sparkScaffold("./unit_tests/keywords_bracketed_start.sql")

    #################################
    
    def sql2sparkScaffold(self, input_file_name):
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
        print("="*40)
        print(input_file_name)
        
        with open(input_file_name) as sqlFile:
            # TODO Meisam: Make the grammar case insensitive?
            query = sqlFile.read().upper()
            stringStream = antlr3.StringStream(query)
            lexer = YSmartLexer(stringStream)
            
            tokenStream = antlr3.CommonTokenStream(lexer)
            
            parser = YSmartParser(tokenStream)
            
            parse_tree = parser.start_rule()
            
            print_tree(parse_tree.tree, 0)
            
            visit_tree(parse_tree.tree)

def print_tree(node, indent):
    print "TRAVERSE:", " " * (indent * 4), node , "\t", type(node)
    for child in node.children:
        print_tree(child, indent + 1)
    pass

TEMPLATE_STRING='''
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


def visit_tree(node):
    childs_strings = ''
    for child in node.children:
        childs_strings += visit_tree(child)
    
    if node.type == sqltokens.T_SELECT:
        str = visit_t_select(node)
        return  TEMPLATE_STRING % childs_strings
    elif node.type == sqltokens.T_RESERVED:
        return "|||RESERVED WORD|||"
    else:
        raise RuntimeException("Unknown type %s for node %s"    % (node.type, node))
    
    pass

def visit_t_select(node):
    print()
    pass


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testParser']
    unittest.main()
