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
import os;
import subprocess;
import sys;

from ysmart.frontend import sql2xml

from ysmart.backend import code_gen

CURRENT_DIR = os.getcwd()
EXEC_DIR = 'bin';
TEMP_DIR = '.tmp';

def genXMLTree(queryFile, tmpFilePath):
    try:
        os.mkdir(TEMP_DIR)
    except:
      pass

    with open(queryFile) as inputFile:
        xmlStr = sql2xml.toXml(inputFile)
        with open(tmpFilePath, "w") as outputFile:
            outputFile.write(xmlStr)

def genHadoopJobs(schemaFile, xmlFilePath, queryName, queryInputPath, queryOutputPath):
# print 'TODO: call job generation program in ./bin/';
    os.chdir(CURRENT_DIR)
    cmd = 'python XML2MapReduce/main.py ' + schemaFile + ' ' + xmlFilePath + ' ' + queryName + ' ' + queryInputPath + ' ' + queryOutputPath
    print cmd
    
    xml_query_srt = open(xmlFilePath).read()
    schema_str = open(schemaFile).read()
    code_gen.ysmart_code_gen(xml_query_srt, schema_str, queryName, queryInputPath, queryOutputPath)

def print_usage():
    print 'usage 1: ./translate.py <query-file>.sql <schema-file>.schema'
    print 'usage 2: ./translate.py <query-file>.sql <schema-file>.schema <query-name> <query-input-path> <query-output-path>'

def main():
    if (len(sys.argv) != 6 and len(sys.argv) != 3):
        print_usage();
        sys.exit(0);

    queryFile = sys.argv[1]
    schemaFile = sys.argv[2]
    tmpFile = str(datetime.datetime.now()).replace(' ', '_') + '.xml'
    xmlFilePath = './' + TEMP_DIR + '/' + tmpFile

    if (len(sys.argv) == 3):
        queryName = "testquery"
        queryInputPath = "YSmartInput/"
        queryOutputPath = "YSmartOutput/"
    elif (len(sys.argv) == 6):
        queryName = sys.argv[3]
        queryInputPath = sys.argv[4]
        queryOutputPath = sys.argv[5]
    else:
        print_usage()
        raise 

    print '--------------------------------------------------------------------'
    print 'Generating XML tree ...'
    genXMLTree(queryFile, xmlFilePath)

    print 'Generating Hadoop jobs ...'
    genHadoopJobs(schemaFile, xmlFilePath, queryName, queryInputPath, queryOutputPath)

    print 'Done'
    print '--------------------------------------------------------------------'
    subprocess.check_call(['rm', '-rf', './' + TEMP_DIR])

if __name__ == "__main__":
    main();
