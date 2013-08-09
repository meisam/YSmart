'''
Created on Jul 15, 2013

@author: fathi
'''
from types import NoneType
from ysmart.backend.ystree import SelectProjectNode, GroupByNode, OrderByNode, \
    TwoJoinNode, TableNode, global_table_dict


_job_template = """
object {job_name} {{
  def main(args: Array[String]) {{

    if (args.length != 3) {{
        Console.print("USAGE: {job_name} <spark_cluster address> <db home dir> <output dir>")
        throw new RuntimeException("Not enough number of parameters")
    }}
    val master =  args(0)
    val dbDir =  args(1)
    val outputDir = args(2)
    
    val sc = new SparkContext(master, "{job_name}",
      "{spark_home}", Seq(), Map())
"""

class SparkCodeEmiter(object):
    _table_scheme = None
    _spark_home = ''
    _rdd_cntr = 0
    _code = ''
    _indent_dept = 0
    _indent_width = 2

    def __init__(self, table_schema, spark_home):
        self._table_scheme = table_schema
        self._spark_home = spark_home

    def get_code(self):
        return self._code

    def _new_rdd_name(self):
        self._rdd_cntr += 1
        return 'rdd{counter:03d}'.format(counter=self._rdd_cntr) 

    def _emit(self, instructions):
        self._code += ' ' * (self._indent_dept * self._indent_width)
        self._code += instructions
        self._code += '\n'
        

    def _emit_package_name(self):
        self._emit('package spark.examples')

    def _emit_package_imports(self):
        self._emit('import spark._')
        self._emit('import SparkContext._')

    def _emit_object_def(self):
        self._emit(_job_template.format(job_name='YSmartSparkJob', spark_home=self._spark_home))

    def emit_header(self):
        self._emit_package_name()
        self._emit_package_imports()
        self._emit_object_def()
        self._indent_dept += 2

    def emit_footer(self):
        self._indent_dept -= 1
        self._emit('}')
        self._indent_dept -= 1
        self._emit('}')

    def emit_table_read(self, table_name, column_indices):
        rdd_name = self._new_rdd_name()
        select_columns = [r'line.split("\\|")({column_index})'.format(column_index=column_index) for column_index in column_indices]
        select_code = 'Seq(' + ', '.join(select_columns) + ')'
        self._emit('val {rdd_name} = sc.textFile(dbDir + "/{table_name}.tbl").map(line => {select_code})'.format(
                rdd_name=rdd_name, table_name=table_name.lower(), select_code=select_code))
        return rdd_name
    
    def emit_join(self, rdd_name_left, rdd_name_right, join_condition_filter, join_project_flat_map):
        """
        This method emits Scala code that joins two RDDs. It does the join in 3 steps.
        1. First it Cartesian products the RDDs.
        2. Filters the results that should be included in the final result by calling the filter() method
        3. Eliminates unneeded columns from the result by calling the map() method.
        
        Proper arguments to the filter() and the map() functions should be given as parameters to this method
        """
        # FIXME use join columns
        cartesian_rdd = self._new_rdd_name()
        
        self._emit('val {cartesian_rdd} = {left_rdd}.cartesian({right_rdd})'.format(
                cartesian_rdd=cartesian_rdd, left_rdd=rdd_name_left, right_rdd=rdd_name_right))
        join_rdd = self._new_rdd_name()
        self._emit('val {join_rdd} = {cartesian_rdd}.filter({condition}).map({join_project_flat_map})'
                   .format(join_rdd=join_rdd , cartesian_rdd=cartesian_rdd, condition=join_condition_filter, join_project_flat_map=join_project_flat_map))
        return join_rdd

    def emit_save_to_file(self, rdd_name):
        self._emit('{rdd}.saveAsTextFile(outputDir)'.format(rdd=rdd_name))

def spark_code(node, job_name, spark_home):
    # /home/fathi/workspace/spark/examples/target/scala-2.9.3/spark-examples_2.9.3-0.8.0-SNAPSHOT.jar
    
    global global_table_dict
    
    code_emitter = SparkCodeEmiter(global_table_dict, spark_home)
    code_emitter.emit_header();
    rdd_name = visit_ystree(node, code_emitter)
    code_emitter.emit_save_to_file(rdd_name)
    code_emitter.emit_footer()
    code = code_emitter.get_code()
    print ('rdd name = {0}'.format(rdd_name))
    print ('code= ================= =\n{0}'.format(code))
    return code


def _scala_join_condition(join_node):
    """
    returns a string that can be used as a lambda function as a parameter to
    dd.filter(). Only tuples that pass return true for this lambda expression 
    should be included in the result of the join.
    
    This method assumes that the current join node is the result of the 
    Cartesian product of its left child and its right child.
    """
    
    condition_exp = join_node.where_condition.where_condition_exp
    func_name = condition_exp.func_name
    parameter_list = condition_exp.parameter_list
    
    if func_name == "EQ":
        parameter_list[0]
        assert len(parameter_list) == 2
        if parameter_list[0].table_name == "LEFT":
            assert parameter_list[1].table_name == "RIGHT"
            left_param = parameter_list[0]
            right_param = parameter_list[1]
        elif parameter_list[0].table_name == "RIGHT":
            assert parameter_list[1].table_name == "LEFT"
            left_param = parameter_list[1]
            right_param = parameter_list[0]
        else:
            raise
        
        assert left_param.column_type == right_param.column_type
        left_columns_index = left_param.column_name
        right_columns_index = right_param.column_name
        return 'x => x._1({left_columns_index}) == x._2({right_columns_index})'.format(
            left_columns_index=left_columns_index, right_columns_index=right_columns_index)
    else:
        raise


def lookup_column_index(column_exp, child, scheme):
    column_name = scheme[column_exp]
    print("DEBUG: column name: {0}".format(column_name))

    for column in child.select_list.tmp_exp_list:
        if child.select_list.dict_exp_and_alias[column] == column_name:
            return child.select_list.tmp_exp_list.index(column)
    raise # not found

def _scala_join_flat_map(join_node):
    """
    This method should be used to remove columns that should not appear in the result of the join.
    These columns are columns that are used in join but are not selected as part of the map.
    """
    left_child_indices = []
    right_child_indices = []
    scheme = join_node.select_list.dict_exp_and_alias
    left_child = join_node.left_child
    right_child = join_node.right_child
    
    for column_exp in join_node.select_list.tmp_exp_list:
        if column_exp.table_name == "LEFT":
            column_index = lookup_column_index(column_exp, left_child, scheme)
            left_child_indices.append(column_index)
        elif column_exp.table_name == "RIGHT":
            column_index = lookup_column_index(column_exp, right_child, scheme)
            right_child_indices.append(column_index)
        else:
            raise
        
    print("DEBUG: Left column indices = {0}; right column indices = {1}".format(left_child_indices , right_child_indices))
    left_child_filter = ['x._1({index})'.format(index=column_index) for column_index in left_child_indices]
    right_child_filter = ['x._2({index})'.format(index=column_index) for column_index in right_child_indices]
    all_columns = left_child_filter + right_child_filter
    return 'x => (' + ', '.join(all_columns) + ')'

def visit_ystree(node, code_emitter):

    global global_table_dict
    db_scheme = global_table_dict
    
    if isinstance(node, GroupByNode):
        return visit_ystree_groupby(node, code_emitter)
    elif isinstance(node, SelectProjectNode):
        for table in node.in_table_list:
            print ("TABLE = {tbl}".format(tbl=table))
        else:
            print("No table found")
    elif isinstance(node, OrderByNode):
        return visit_ystree_orderby(node.child, code_emitter)
    elif isinstance(node, TwoJoinNode):
        rdd_name_left = visit_ystree(node.left_child, code_emitter)
        rdd_name_right = visit_ystree(node.right_child, code_emitter)
        condition = node.where_condition.where_condition_exp
#         parameters_list = condition.parameters_list
        join_condition_filter = _scala_join_condition(node)
        join_project_flat_map = _scala_join_flat_map(node) 
        return code_emitter.emit_join(rdd_name_left, rdd_name_right, join_condition_filter, join_project_flat_map)
    elif isinstance(node, TableNode):
        table_name = node.table_name
        column_indices = [column.column_name for column in node.select_list.tmp_exp_list]
        print('columns {0}'.format(str(column_indices)))
        return code_emitter.emit_table_read(table_name, column_indices)
    elif isinstance(node, NoneType):
        print("ROOT")
    else:
        raise RuntimeError(node.__class__.__name__)

def visit_ystree_groupby(node, code_emitter):
    print("-" * 80)
    print("NODE:{value}".format(value=repr(node)))
    print("child:{value}".format(value=repr(node.child)))
    print("composite:{value}".format(value=repr(node.composite)))
    print("table_list:{value}".format(value=node.table_list))
    print("where_condition:{value}".format(value=node.where_condition))
    print("select_list:{value}".format(value=node.select_list))
    print("group_by_clause:{value}".format(value=node.group_by_clause))
    print("node.group_by_clause.source:{value}".format(value=node.group_by_clause.source))
    print("node.group_by_clause.realstructure:{value}".format(value=node.group_by_clause.realstructure))
    print("node.group_by_clause.groupby_exp_list:{value}".format(value=node.group_by_clause.groupby_exp_list))
    print(":{value}".format(value=''))

    print("node.group_by_clause: ")
    for property_name, value in vars(node.group_by_clause).iteritems():
        print("\t\t{property_name}:{value}".format(property_name=property_name, value=value))
    
    print('global table dic = {tables}'.format(tables=global_table_dict))

    print("node.group_by_clause.groupby_exp_list: ")
    for exp in node.group_by_clause.groupby_exp_list:
        print("\t{exp}:".format(exp=exp))
        for property_name, value in vars(exp).iteritems():
            print("\t\t{property_name}:{value} [{type}]".format(property_name=property_name, value=value, type=type(value).__module__))
        print('\tcolumn name= {colname}'.format(colname=global_table_dict[exp.table_name].column_list[exp.column_name].column_name))

    print("." * 20)
    for atr in dir(node):
        print("\t{key} = {value}".format(key=atr, value='?'))
#     raise RuntimeError('Not implemented yet')

def visit_ystree_orderby(node, code_emitter):
    raise RuntimeError('Not implemented yet')

