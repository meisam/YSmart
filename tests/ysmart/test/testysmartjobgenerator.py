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

'''
Testcases for YSmart front end
Created on May 7, 2013

@author: fathi
'''

from ysmart.backend import correlation, ystree, config
from ysmart.backend.code_gen import JobWriter, generate_code, base_name, \
    INITIAL_CLASSNAME_SUFFIX
from ysmart.frontend.sql2xml import toXml

import difflib
import os
import unittest

base_tests_path = './tests/ysmart/test'

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

    @unittest.skip("skipping until the bug in parser is fixed")
    def test_select_country(self):
        self._job_generator_scaffold('countries_select.sql', 'countries.schema', './unit_tests')

    def test_select_country_order_by(self):
        self._job_generator_scaffold('countries_select_order_by.sql', 'countries.schema', './unit_tests')

    def test_ssb_q1_1(self):
        self._job_generator_scaffold('q1_1.sql', 'ssb.schema', './ssb_test')

    def test_ssb_q1_2(self):
        self._job_generator_scaffold('q1_2.sql', 'ssb.schema', './ssb_test')

    def test_ssb_q1_3(self):
        self._job_generator_scaffold('q1_3.sql', 'ssb.schema', './ssb_test')

    def test_ssb_q2_1(self):
        self._job_generator_scaffold('q2_1.sql', 'ssb.schema', './ssb_test')

    def test_ssb_q2_2(self):
        self._job_generator_scaffold('q2_2.sql', 'ssb.schema', './ssb_test')

    def test_ssb_q2_3(self):
        self._job_generator_scaffold('q2_3.sql', 'ssb.schema', './ssb_test')

    def test_ssb_q3_1(self):
        self._job_generator_scaffold('q3_1.sql', 'ssb.schema', './ssb_test')

    def test_ssb_q3_2(self):
        self._job_generator_scaffold('q3_2.sql', 'ssb.schema', './ssb_test')

    def test_ssb_q3_3(self):
        self._job_generator_scaffold('q3_3.sql', 'ssb.schema', './ssb_test')

    @unittest.skip("skipping until the bug in parser is fixed")
    def test_ssb_q3_4(self):
        self._job_generator_scaffold('q3_4.sql', 'ssb.schema', './ssb_test')

    def test_ssb_q4_1(self):
        self._job_generator_scaffold('q4_1.sql', 'ssb.schema', './ssb_test')

    def test_ssb_q4_2(self):
        self._job_generator_scaffold('q4_2.sql', 'ssb.schema', './ssb_test')

    def test_ssb_q4_3(self):
        self._job_generator_scaffold('q4_3.sql', 'ssb.schema', './ssb_test')

    def test_tpch_1(self):
        self._job_generator_scaffold('tpch1query.sql', 'tpch.schema', './tpch_test')

    @unittest.skip("skipping until the bug in parser is fixed")
    def test_tpch_3(self):
        self._job_generator_scaffold('tpch3query.sql', 'tpch.schema', './tpch_test')

    def test_tpch_5(self):
        self._job_generator_scaffold('tpch5query.sql', 'tpch.schema', './tpch_test')

    def test_tpch_6(self):
        self._job_generator_scaffold('tpch6query.sql', 'tpch.schema', './tpch_test')

    def test_tpch_10(self):
        self._job_generator_scaffold('tpch10query.sql', 'tpch.schema', './tpch_test')

    @unittest.skip("skipping until the bug in parser is fixed")
    def test_tpch_17(self):
        self._job_generator_scaffold('tpch17query.sql', 'tpch.schema', './tpch_test')

    @unittest.skip("skipping until the bug in parser is fixed")
    def test_tpch_18(self):
        self._job_generator_scaffold('tpch18query.sql', 'tpch.schema', './tpch_test')

    @unittest.skip("skipping until the bug in parser is fixed")
    def test_tpch_21(self):
        self._job_generator_scaffold('tpch21query.sql', 'tpch.schema', './tpch_test')

    def _job_generator_scaffold(self, query_file_name, schema_file_name, relative_path):
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
        
        path = os.path.join(base_tests_path, relative_path)
        inputFileName = os.path.join(path , query_file_name)
        schema_file_name = os.path.join(path, schema_file_name)
        with open(inputFileName) as quey_file, open(schema_file_name) as schema_file:
            xml_str = toXml(quey_file)
            schema_str = schema_file.read()

        tree_node = ystree.ysmart_tree_gen(xml_str, schema_str)
        tree_node = correlation.ysmart_correlation(tree_node)

        job_name = query_file_name[:-len('.sql')] + INITIAL_CLASSNAME_SUFFIX
        jobs = generate_code(tree_node, job_name)


        expected_job_files = [j for j in _hadoop_job_files(path, job_name)]

        self.assertEqual(len(expected_job_files), len(jobs),
                          "A different number of hadoop jab files generated for %s! Expected=%d, generated=%d,\n"
                          "generated file names=%s\n"
                          "expected file names=%s"
                           % (job_name, len(expected_job_files), len(jobs)
                              , [job.name for job in jobs]
                              , expected_job_files))

        for job in jobs:
            self.assertTrue(job.name in expected_job_files,
                             "Hadoop job file %s generated but not expected to be generated!" % job.name)

        for job in jobs:
            output_path = os.path.join(path, job.name)
            with open(output_path) as expected_output:
                expected_output_str = expected_output.read()
                diff = difflib.ndiff(expected_output_str.splitlines(1), job.content.splitlines(1))
                self.assertEqual(expected_output_str, job.content, errorMsg 
                                 % (job.name, "[Omitted...]", "[Omitted...]", ''.join(diff)))

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
    for f in os.listdir(path):
        if f.endswith('.java'):
            class_name = base_name(f[:-len('.java')])
            if base_name(job_name)  == class_name:
                yield f

if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testParser']
    unittest.main()
