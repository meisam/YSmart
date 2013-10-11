'''
Created on Jul 15, 2013

@author: fathi
'''

from ysmart.backend import correlation, ystree, config
from ysmart.frontend.sql2xml import toXml
from ysmart.spark import translator
import os.path
import unittest

class BaseTestCase(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

class Test(BaseTestCase):

    def test_select_nation(self):
        self._test_scaffold('tests/ysmart/test/unit_tests/tpch_select_nation.sql'
                                  , 'tests/ysmart/test/tpch_test/tpch.schema', 'tests/ysmart/test/unit_tests')

    def test_join(self):
        self._test_scaffold('tests/ysmart/test/unit_tests/tpch_join.sql'
                                  , 'tests/ysmart/test/tpch_test/tpch.schema'
                               , 'tests/ysmart/test/tpch_test')

    def test_ssb1_1(self):
        self._test_scaffold('tests/ysmart/test/ssb_test/q1_1.sql'
                                  , 'tests/ysmart/test/ssb_test/ssb.schema', 'tests/ysmart/test/ssb_test')

    def test_ssb1_2(self):
        self._test_scaffold('tests/ysmart/test/ssb_test/q1_2.sql'
                                  , 'tests/ysmart/test/ssb_test/ssb.schema', 'tests/ysmart/test/ssb_test')

    def test_ssb1_3(self):
        self._test_scaffold('tests/ysmart/test/ssb_test/q1_3.sql'
                                  , 'tests/ysmart/test/ssb_test/ssb.schema', 'tests/ysmart/test/ssb_test')

    def test_ssb2_1(self):
        self._test_scaffold('tests/ysmart/test/ssb_test/q2_1.sql'
                                  , 'tests/ysmart/test/ssb_test/ssb.schema', 'tests/ysmart/test/ssb_test')

    def test_ssb2_2(self):
        self._test_scaffold('tests/ysmart/test/ssb_test/q2_2.sql'
                                  , 'tests/ysmart/test/ssb_test/ssb.schema', 'tests/ysmart/test/ssb_test')

    def test_ssb2_3(self):
        self._test_scaffold('tests/ysmart/test/ssb_test/q2_3.sql'
                                  , 'tests/ysmart/test/ssb_test/ssb.schema', 'tests/ysmart/test/ssb_test')

    def test_ssb3_1(self):
        self._test_scaffold('tests/ysmart/test/ssb_test/q3_1.sql'
                                  , 'tests/ysmart/test/ssb_test/ssb.schema', 'tests/ysmart/test/ssb_test')

    def test_ssb3_2(self):
        self._test_scaffold('tests/ysmart/test/ssb_test/q3_2.sql'
                                  , 'tests/ysmart/test/ssb_test/ssb.schema', 'tests/ysmart/test/ssb_test')

    def test_ssb3_3(self):
        self._test_scaffold('tests/ysmart/test/ssb_test/q3_3.sql'
                                  , 'tests/ysmart/test/ssb_test/ssb.schema', 'tests/ysmart/test/ssb_test')

    def test_ssb3_4(self):
        self._test_scaffold('tests/ysmart/test/ssb_test/q3_4.sql'
                                  , 'tests/ysmart/test/ssb_test/ssb.schema', 'tests/ysmart/test/ssb_test')

    def test_ssb4_1(self):
        self._test_scaffold('tests/ysmart/test/ssb_test/q4_1.sql'
                                  , 'tests/ysmart/test/ssb_test/ssb.schema', 'tests/ysmart/test/ssb_test')

    def test_ssb4_2(self):
        self._test_scaffold('tests/ysmart/test/ssb_test/q4_2.sql'
                                  , 'tests/ysmart/test/ssb_test/ssb.schema', 'tests/ysmart/test/ssb_test')

    def test_ssb4_3(self):
        self._test_scaffold('tests/ysmart/test/ssb_test/q4_3.sql'
                                  , 'tests/ysmart/test/ssb_test/ssb.schema', 'tests/ysmart/test/ssb_test')

    def test_tpch1query(self):
        self._test_scaffold('tests/ysmart/test/tpch_test/tpch1query.sql'
                                  , 'tests/ysmart/test/tpch_test/tpch.schema'
                               , 'tests/ysmart/test/tpch_test')

    def test_tpch3query(self):
        self._test_scaffold('tests/ysmart/test/tpch_test/tpch3query.sql'
                                  , 'tests/ysmart/test/tpch_test/tpch.schema'
                               , 'tests/ysmart/test/tpch_test')

    def test_tpch5query(self):
        self._test_scaffold('tests/ysmart/test/tpch_test/tpch5query.sql'
                                  , 'tests/ysmart/test/tpch_test/tpch.schema'
                               , 'tests/ysmart/test/tpch_test')

    def test_tpch6query(self):
        self._test_scaffold('tests/ysmart/test/tpch_test/tpch6query.sql'
                                  , 'tests/ysmart/test/tpch_test/tpch.schema'
                               , 'tests/ysmart/test/tpch_test')

    def test_tpch10query(self):
        self._test_scaffold('tests/ysmart/test/tpch_test/tpch10query.sql'
                                  , 'tests/ysmart/test/tpch_test/tpch.schema'
                               , 'tests/ysmart/test/tpch_test')

    def test_tpch17query(self):
        self._test_scaffold('tests/ysmart/test/tpch_test/tpch17query.sql'
                                  , 'tests/ysmart/test/tpch_test/tpch.schema'
                               , 'tests/ysmart/test/tpch_test')

    def test_tpch18query(self):
        self._test_scaffold('tests/ysmart/test/tpch_test/tpch18query.sql'
                                  , 'tests/ysmart/test/tpch_test/tpch.schema'
                               , 'tests/ysmart/test/tpch_test')

    def test_tpch21query(self):
        self._test_scaffold('tests/ysmart/test/tpch_test/tpch21query.sql'
                                , 'tests/ysmart/test/tpch_test/tpch.schema'
                               , 'tests/ysmart/test/tpch_test')

    def _test_scaffold(self, input_file_path, schema_file_path, dest_dir):
        config.turn_on_correlation = True
        config.advanced_agg = True

        with open(input_file_path) as quey_file, open(schema_file_path) as schema_file:
            xml_str = toXml(quey_file)
            schema_str = schema_file.read()
        tree_node = ystree.ysmart_tree_gen(xml_str, schema_str)
        tree_node = correlation.ysmart_correlation(tree_node)
        
        file_name, ___ = os.path.splitext(os.path.basename(input_file_path))
        job_name = file_name.title()
        dest_file_path = os.path.join(dest_dir, job_name + '.scala')
        code = translator.spark_code(tree_node, job_name)
        with open(dest_file_path, 'w') as job_file:
            print(os.path.abspath(dest_file_path))
            job_file.write(code)
            job_file.flush()

if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
