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
import sys
import difflib
sys.path.append("..")
sys.path.append("../XML2MapRed")

from code_gen import JobWriter

import unittest

class TestJobGenerator(unittest.TestCase):

    def setUp(self):
        pass


    def tearDown(self):
        pass

    def testPrint(self):
        f = JobWriter("file1.java")

        lines = ["This is a text", "Another text"]
        for line in lines:
            print >> f, line

        self.assertEqual(f.content, "\n".join(lines) + "\n")

if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testParser']
    unittest.main()
