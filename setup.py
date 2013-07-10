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
import os
import sys

from distutils.core import setup
from distutils.sysconfig import get_python_lib

version = '13.09'

setup(
    name='YSmart',
    version=version,
    url='http://ysmart.cse.ohio-state.edu/',
    author='Rubao Lee and others',
    author_email='liru@cse.ohio-state.edu',
    description=('YSmart is a correlation aware SQL-to-MapReduce translator'),
    license='http://www.apache.org/licenses/LICENSE-2.0',
    packages=['ysmart', 'ysmart.frontend', 'ysmart.backend', 'ysmart.test'],
    package_dir={'ysmart':'src/ysmart',
                  'ysmart.frontend': 'src/ysmart/frontend',
                  'ysmart.backend': 'src/ysmart/backend',
                  'ysmart.test': 'tests/ysmart/test'
                },
    data_files=[('ysmart.test', 
                 [
                    'tests/ysmart/test/ssb_test/q1_3.sql.xml',
                    'tests/ysmart/test/ssb_test/q2_2004.java',
                    'tests/ysmart/test/ssb_test/q2_3.sql.xml',
                    'tests/ysmart/test/ssb_test/q1_1002.java',
                    'tests/ysmart/test/ssb_test/q3_1005.java',
                    'tests/ysmart/test/ssb_test/q3_4002.java',
                    'tests/ysmart/test/ssb_test/q2_3.sql',
                    'tests/ysmart/test/ssb_test/q2_1004.java',
                    'tests/ysmart/test/ssb_test/q1_2.sql',
                    'tests/ysmart/test/ssb_test/q4_1.sql.xml',
                    'tests/ysmart/test/ssb_test/q1_2001.java',
                    'tests/ysmart/test/ssb_test/q1_3002.java',
                    'tests/ysmart/test/ssb_test/q3_3.sql.xml',
                    'tests/ysmart/test/ssb_test/q1_1.sql',
                    'tests/ysmart/test/ssb_test/q4_2006.java',
                    'tests/ysmart/test/ssb_test/q2_3003.java',
                    'tests/ysmart/test/ssb_test/q3_2.sql.xml',
                    'tests/ysmart/test/ssb_test/q4_2005.java',
                    'tests/ysmart/test/ssb_test/q4_2.sql',
                    'tests/ysmart/test/ssb_test/q2_2.sql',
                    'tests/ysmart/test/ssb_test/q4_2004.java',
                    'tests/ysmart/test/ssb_test/q3_3001.java',
                    'tests/ysmart/test/ssb_test/q3_4005.java',
                    'tests/ysmart/test/ssb_test/q3_4003.java',
                    'tests/ysmart/test/ssb_test/q4_3005.java',
                    'tests/ysmart/test/ssb_test/q3_2005.java',
                    'tests/ysmart/test/ssb_test/q2_3005.java',
                    'tests/ysmart/test/ssb_test/q3_3.sql',
                    'tests/ysmart/test/ssb_test/q2_3001.java',
                    'tests/ysmart/test/ssb_test/q3_1001.java',
                    'tests/ysmart/test/ssb_test/q2_1002.java',
                    'tests/ysmart/test/ssb_test/q2_2002.java',
                    'tests/ysmart/test/ssb_test/q4_1005.java',
                    'tests/ysmart/test/ssb_test/q3_4.sql.xml',
                    'tests/ysmart/test/ssb_test/q1_3.sql',
                    'tests/ysmart/test/ssb_test/q2_2.sql.xml',
                    'tests/ysmart/test/ssb_test/q3_2004.java',
                    'tests/ysmart/test/ssb_test/q2_1001.java',
                    'tests/ysmart/test/ssb_test/q3_1002.java',
                    'tests/ysmart/test/ssb_test/q3_4004.java',
                    'tests/ysmart/test/ssb_test/q1_3001.java',
                    'tests/ysmart/test/ssb_test/q4_3006.java',
                    'tests/ysmart/test/ssb_test/q2_2005.java',
                    'tests/ysmart/test/ssb_test/q4_3003.java',
                    'tests/ysmart/test/ssb_test/q1_2002.java',
                    'tests/ysmart/test/ssb_test/q2_2003.java',
                    'tests/ysmart/test/ssb_test/q4_3002.java',
                    'tests/ysmart/test/ssb_test/q3_3003.java',
                    'tests/ysmart/test/ssb_test/q1_2.sql.xml',
                    'tests/ysmart/test/ssb_test/q4_1002.java',
                    'tests/ysmart/test/ssb_test/q4_3.sql.xml',
                    'tests/ysmart/test/ssb_test/q4_2.sql.xml',
                    'tests/ysmart/test/ssb_test/q3_2.sql',
                    'tests/ysmart/test/ssb_test/q2_1.sql.xml',
                    'tests/ysmart/test/ssb_test/q3_1003.java',
                    'tests/ysmart/test/ssb_test/q4_1003.java',
                    'tests/ysmart/test/ssb_test/q2_2001.java',
                    'tests/ysmart/test/ssb_test/q3_3005.java',
                    'tests/ysmart/test/ssb_test/q2_1003.java',
                    'tests/ysmart/test/ssb_test/q4_2002.java',
                    'tests/ysmart/test/ssb_test/q4_3004.java',
                    'tests/ysmart/test/ssb_test/q2_3002.java',
                    'tests/ysmart/test/ssb_test/q2_1.sql',
                    'tests/ysmart/test/ssb_test/q3_1.sql',
                    'tests/ysmart/test/ssb_test/ssb.schema',
                    'tests/ysmart/test/ssb_test/q3_3002.java',
                    'tests/ysmart/test/ssb_test/q3_1.sql.xml',
                    'tests/ysmart/test/ssb_test/q3_2002.java',
                    'tests/ysmart/test/ssb_test/q2_1005.java',
                    'tests/ysmart/test/ssb_test/q4_1004.java',
                    'tests/ysmart/test/ssb_test/q3_3004.java',
                    'tests/ysmart/test/ssb_test/q1_1.sql.xml',
                    'tests/ysmart/test/ssb_test/q4_3001.java',
                    'tests/ysmart/test/ssb_test/q4_2001.java',
                    'tests/ysmart/test/ssb_test/q4_2003.java',
                    'tests/ysmart/test/ssb_test/q4_1006.java',
                    'tests/ysmart/test/ssb_test/q3_1004.java',
                    'tests/ysmart/test/ssb_test/q2_3004.java',
                    'tests/ysmart/test/ssb_test/q4_1001.java',
                    'tests/ysmart/test/ssb_test/q3_4.sql',
                    'tests/ysmart/test/ssb_test/q4_1.sql',
                    'tests/ysmart/test/ssb_test/q3_2001.java',
                    'tests/ysmart/test/ssb_test/q3_2003.java',
                    'tests/ysmart/test/ssb_test/q1_1001.java',
                    'tests/ysmart/test/ssb_test/q4_3.sql',
                    'tests/ysmart/test/ssb_test/q3_4001.java',
                    'tests/ysmart/test/unit_tests/countries.schema',
                    'tests/ysmart/test/unit_tests/countries_quilified_select.sql',
                    'tests/ysmart/test/unit_tests/countries_select_order_by001.java',
                    'tests/ysmart/test/unit_tests/countries_select.sql',
                    'tests/ysmart/test/unit_tests/keywords_uid.sql',
                    'tests/ysmart/test/unit_tests/keywords_size.sql',
                    'tests/ysmart/test/unit_tests/keywords_start.sql.xml',
                    'tests/ysmart/test/unit_tests/countries_select001.java',
                    'tests/ysmart/test/unit_tests/keywords_uid.sql.xml',
                    'tests/ysmart/test/unit_tests/keywords.schema',
                    'tests/ysmart/test/unit_tests/keywords_bracketed_start.sql',
                    'tests/ysmart/test/unit_tests/countries_select_order_by.sql',
                    'tests/ysmart/test/unit_tests/keywords_start.sql',
                    'tests/ysmart/test/unit_tests/keywords_size.sql.xml',
                    'tests/ysmart/test/tpch_test/tpch3query.sql',
                    'tests/ysmart/test/tpch_test/tpch21query005.java',
                    'tests/ysmart/test/tpch_test/tpch.schema',
                    'tests/ysmart/test/tpch_test/tpch1query001.java',
                    'tests/ysmart/test/tpch_test/tpch21query.sql',
                    'tests/ysmart/test/tpch_test/tpch6query.sql.xml',
                    'tests/ysmart/test/tpch_test/tpch6query.sql',
                    'tests/ysmart/test/tpch_test/tpch21query.sql.xml',
                    'tests/ysmart/test/tpch_test/tpch17query002.java',
                    'tests/ysmart/test/tpch_test/tpch18query.sql',
                    'tests/ysmart/test/tpch_test/tpch5query004.java',
                    'tests/ysmart/test/tpch_test/tpch10query005.java',
                    'tests/ysmart/test/tpch_test/tpch5query001.java',
                    'tests/ysmart/test/tpch_test/tpch21query001.java',
                    'tests/ysmart/test/tpch_test/tpch21query002.java',
                    'tests/ysmart/test/tpch_test/tpch1query.sql.xml',
                    'tests/ysmart/test/tpch_test/tpch10query.sql.xml',
                    'tests/ysmart/test/tpch_test/tpch17query.sql',
                    'tests/ysmart/test/tpch_test/tpch21query003.java',
                    'tests/ysmart/test/tpch_test/tpch3query003.java',
                    'tests/ysmart/test/tpch_test/tpch17query001.java',
                    'tests/ysmart/test/tpch_test/tpch10query002.java',
                    'tests/ysmart/test/tpch_test/tpch18query.sql.xml',
                    'tests/ysmart/test/tpch_test/tpch6query001.java',
                    'tests/ysmart/test/tpch_test/tpch10query004.java',
                    'tests/ysmart/test/tpch_test/tpch17query.sql.xml',
                    'tests/ysmart/test/tpch_test/tpch5query.sql.xml',
                    'tests/ysmart/test/tpch_test/tpch5query006.java',
                    'tests/ysmart/test/tpch_test/tpch5query002.java',
                    'tests/ysmart/test/tpch_test/tpch10query003.java',
                    'tests/ysmart/test/tpch_test/tpch1query.sql',
                    'tests/ysmart/test/tpch_test/tpch10query.sql',
                    'tests/ysmart/test/tpch_test/tpch5query.sql',
                    'tests/ysmart/test/tpch_test/tpch5query007.java',
                    'tests/ysmart/test/tpch_test/tpch18query002.java',
                    'tests/ysmart/test/tpch_test/tpch3query001.java',
                    'tests/ysmart/test/tpch_test/tpch3query002.java',
                    'tests/ysmart/test/tpch_test/tpch1query002.java',
                    'tests/ysmart/test/tpch_test/tpch5query005.java',
                    'tests/ysmart/test/tpch_test/tpch21query004.java',
                    'tests/ysmart/test/tpch_test/tpch3query.sql.xml',
                    'tests/ysmart/test/tpch_test/tpch18query003.java',
                    'tests/ysmart/test/tpch_test/tpch10query001.java',
                    'tests/ysmart/test/tpch_test/tpch5query003.java',
                    'tests/ysmart/test/tpch_test/tpch18query001.java',
                  ]
                 )],
    scripts=['scripts/sql2mapred.py'],
)
