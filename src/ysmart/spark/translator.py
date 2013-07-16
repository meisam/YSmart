'''
Created on Jul 15, 2013

@author: fathi
'''
from types import NoneType
from ysmart.backend.code_gen import JobWriter
from ysmart.backend.ystree import SelectProjectNode, GroupByNode, OrderByNode, \
    TwoJoinNode, TableNode, global_table_dict


_job_template = """
package spark.examples

import spark._
import SparkContext._

{imports}

object {job_name} {{

  def main(args: Array[String]) {{

    val master = if (args.length == 0) "spark://perseus:7077"  else args(0)
    val sc = new SparkContext(master, "{job_name}",
      "/home/fathi/workspace/spark", Seq("/home/fathi/workspace/spark/examples/target/scala-2.9.3/spark-examples_2.9.3-0.8.0-SNAPSHOT.jar") )
    
    val dataset = sc.textFile("/home/fathi/hadoop-file-system/tpch-data/1gb-sample/{file_name}.tbl")
    
    val nations = dataset.map(line => {select_code})
    nations.saveAsTextFile("/home/fathi/Desktop/select-nation")

    Console.print("number of nations=" + nations.count)
  }}

}}
"""


def spark_code(node):
    """ returns a list of JobWriters for the given tree
    """
    
    global global_table_dict
    
        
    result = visit_ystree(node, global_table_dict)
    
    code = _job_template.format(select_code=result, imports=""
                                , job_name="TestSpark", file_name="nation")
    
    return code

def visit_ystree(node, schema_dictionary):

    print("visiting {node} with type {type}".format(node=node, type=node.__class__))
    
    if isinstance(node, GroupByNode):
        visit_ystree(node.child)
    elif isinstance(node, SelectProjectNode):
        for table in node.in_table_list:
            print ("TABLE = {tbl}".format(tbl=table))
        else:
            print("No table found")
    elif isinstance(node, OrderByNode):
        visit_ystree(node.child)
    elif isinstance(node, TwoJoinNode):
        visit_ystree(node.left_child)
        visit_ystree(node.right_child)
    elif isinstance(node, TableNode):
        select_columns = []
        for column in node.select_list.tmp_exp_list:
            table_name = node.table_name
            column_name = node.select_list.dict_exp_and_alias[column]
            column_index = schema_dictionary[table_name].column_name_list.index(column_name)
            select_columns.append(r'line.split("\\|")({column_index})'.format(column_index=column_index))
            
        result = ' + "\" +'.join(select_columns)
        return result
                              
    elif isinstance(node, NoneType):
        print("ROOT")
    else:
        raise RuntimeError(node.__class__.__name__)

