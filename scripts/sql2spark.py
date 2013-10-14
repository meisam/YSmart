#!/usr/bin/env python

"""
   Copyright (c) 2012 The Ohio State University.

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


import datetime

from ysmart.backend import code_gen, correlation, ystree, config
from ysmart.frontend import sql2xml
from ysmart.spark import translator

import os;
import subprocess;
import sys;

def print_usage():
    print 'usage: ./sql2spark.py <query-file>.sql <schema-file>.schema <query-name> <dest-path>'

def main():
    if (len(sys.argv) != 4):
        print_usage()
        sys.exit(0)

    sql_file_path = sys.argv[1]
    schema_file_path = sys.argv[2]
    dest_dir = sys.argv[3]
    
    config.advanced_agg = True
    config.turn_on_correlation = True

    with open(sql_file_path) as quey_file, open(schema_file_path) as schema_file:
        xml_str = sql2xml.toXml(quey_file)
        schema_str = schema_file.read()
    tree_node = ystree.ysmart_tree_gen(xml_str, schema_str)
    tree_node = correlation.ysmart_correlation(tree_node)

    file_name, ___ = os.path.splitext(os.path.basename(sql_file_path))
    job_name = file_name.title()
    dest_file_path = os.path.join(dest_dir, job_name + '.scala')
    code = translator.spark_code(tree_node, job_name)
    with open(dest_file_path, 'w') as job_file:
        print(os.path.abspath(dest_file_path))
        job_file.write(code)
        job_file.flush()

if __name__ == "__main__":
    main();
