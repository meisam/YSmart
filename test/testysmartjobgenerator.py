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
from sql2xml import toXml
import os

'''
Testcases for YSmart front end
Created on May 7, 2013

@author: fathi
'''
import sys
import difflib
sys.path.append("..")
sys.path.append("../SQL2XML")
sys.path.append("../XML2MapReduce")

import config
from YSmartLexer import * # import all the tokens
from YSmartParser import *
from antlr3.tokens import CommonToken

import antlr3
import unittest
import ystree
from sql2xml import toXml
from code_gen import JobWriter, generate_code

import unittest

class TestJobGenerator(unittest.TestCase):

    def setUp(self):
        pass


    def tearDown(self):
        pass

    def testPrint(self):
        f = JobWriter("file1")

        lines = ["This is a text", "Another text"]
        for line in lines:
            print >> f, line

        self.assertEqual(f.content, "\n".join(lines) + "\n")

    def test_select_country(self):
        self._job_generator_scaffold('countries_select.sql', 'countries.schema', './test/unit_tests')

    def test_select_country_order_by(self):
        self._job_generator_scaffold('countries_select_order_by.sql', 'countries.schema', './test/unit_tests')

    def test_ssb_q1_1(self):
        self._job_generator_scaffold('q1_1.sql', 'ssb.schema', './test/ssb_test')

    def test_ssb_q1_2(self):
        self._job_generator_scaffold('q1_2.sql', 'ssb.schema', './test/ssb_test')

    def test_ssb_q1_3(self):
        self._job_generator_scaffold('q1_3.sql', 'ssb.schema', './test/ssb_test')

    def test_ssb_q2_1(self):
        self._job_generator_scaffold('q2_1.sql', 'ssb.schema', './test/ssb_test')

    def _job_generator_scaffold(self, query_file_name, schema_file_name, path):
        errorMsg ="""
Expected output and produced output do not match for %s:
------------------------------------------------------
Expected output:
%s
------------------------------------------------------
Produced output:
%s
------------------------------------------------------
Diff: %s
===============================================================
"""

        # Needed to match results with the YSmart online version
        config.turn_on_correlation = True
        config.advanced_agg = True
        
        inputFileName = os.path.join(path , query_file_name)
        schema_file_name = os.path.join(path, schema_file_name)
        with open(inputFileName) as quey_file, open(schema_file_name) as schema_file:
            xml_str = toXml(quey_file)
            schema_str = schema_file.read()

        tree_node = ystree.ysmart_tree_gen(xml_str, schema_str)

        job_name = query_file_name[:-len('.sql')]
        jobs = generate_code(tree_node, job_name)


        hadoop_job_files = _hadoop_job_files(path, job_name)

        self.assertEqual(len(hadoop_job_files), len(jobs),
                          "A different number of hadoop jab files generated for %s! Expected=%d, generated=%d"
                           % (job_name, len(hadoop_job_files), len(jobs)))

        for job in jobs:
            self.assertTrue(job.name in hadoop_job_files,
                             "Hadoop job file %s generated but not expected to be generated!" % job.name)

        for job in jobs:
            output_path = os.path.join(path, job.name)
            with open(output_path) as expected_output:
                expected_output_str = expected_output.read()
                diff = difflib.ndiff(expected_output_str.splitlines(1), job.content.splitlines(1))
                self.assertEqual(expected_output_str, job.content, errorMsg % (job.name, expected_output_str, job.content, ''.join(diff)))

def _hadoop_job_files(path, job_name):
    '''
    returns a list of all files in the given path that are translated hadoop jobs of the given job.
    This means that the file name should meet these 3 conditions:
        1. It start with 'job_name'.
        2. It should end with '.java'.
        3. Ignoring the staring part of the file and its suffix, the rest should be a number
     For example if the given job_name is SelectQuery, this function returns
     files whose names are SelectQuery1.java, SelectQuery2.java, SelectQuery01.java
     if they exist in the given directory.
    '''
    return filter(lambda f: f.startswith(job_name) and f.endswith('java') and f[len(job_name):][:-len('.java')].isdigit(), os.listdir(path))

if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testParser']
    unittest.main()
