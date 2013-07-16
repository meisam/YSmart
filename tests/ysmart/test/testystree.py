'''
Created on Jul 15, 2013

@author: fathi
'''
from ysmart.backend import config, correlation, ystree
import os.path
import unittest
from ysmart.frontend.sql2xml import toXml
from ysmart.spark import translator

class BaseTestCase(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

class Test(BaseTestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_1(self):

#         config.turn_on_correlation = True
#         config.advanced_agg = True
        
        inputFileName = 'tests/ysmart/test/unit_tests/tpch_select_nation.sql'
        print(os.path.abspath(inputFileName))
        schema_file_name = 'tests/ysmart/test/tpch_test/tpch.schema'

        with open(inputFileName) as quey_file, open(schema_file_name) as schema_file:
            xml_str = toXml(quey_file)
            schema_str = schema_file.read()

        tree_node = ystree.ysmart_tree_gen(xml_str, schema_str)
#         tree_node = correlation.ysmart_correlation(tree_node)

        code = translator.spark_code(tree_node)
        
        with open('/home/fathi/workspace/spark/examples/src/main/scala/spark/ysmart-examples/TestSpark.scala', 'w') as job_file:
            job_file.write(code)
            job_file.flush()

if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
