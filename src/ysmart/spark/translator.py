'''
Created on Jul 15, 2013

@author: fathi
'''
from types import NoneType
from ysmart.backend.code_gen import math_func_dict, agg_func_list
from ysmart.backend.ystree import SelectProjectNode, GroupByNode, OrderByNode, \
    TwoJoinNode, TableNode, global_table_dict, YRawColExp, YConsExp, YFuncExp, \
    FirstStepWhereCondition, FirstStepOnCondition


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

def _condition_to_scala(expr, node):
    logical_funtions = {"AND":" && ", "OR":" || ", "EQ":" == ", "GTH":" > ", "LTH":" < ", "NOT_EQ":" != ", "GEQ":" >= ", "LEQ":" <= "}
    
    if isinstance(expr, YRawColExp):
        column_index = expr.column_name
        return 'x._{0}'.format(column_index + 1)
    elif isinstance(expr, YConsExp):
        return expr.cons_value
    elif isinstance(expr, YFuncExp):
        condition_str = [_condition_to_scala(param, node) for param in expr.parameter_list]
        return '(' + logical_funtions[expr.func_name].join(condition_str) + ')'
    else:
        raise RuntimeError(repr(expr))


def _select_list_to_scala(column_expr, node):
    if isinstance(column_expr, YRawColExp):
        return 'x._{index}'.format(index=column_expr.column_name + 1)
    elif isinstance(column_expr, YConsExp):
        if column_expr.cons_type in ["DECIMAL", "INTEGER"]:
            return str(column_expr.cons_value)
        else:
            return column_expr.cons_value
    elif isinstance(column_expr, YFuncExp):
        func_name = column_expr.func_name
        params = [_select_list_to_scala(parameter_expr, node) for parameter_expr in column_expr.parameter_list]
        if func_name in agg_func_list:
            if len(params) == 1:
                return params[0]
            elif len(params) > 1:
                return ', '.join(params)
            else:
                raise 
        elif func_name in math_func_dict:
            operation = math_func_dict[column_expr.func_name]
            return '(' + operation.join(params) + ')'
        else:
            raise
    else:
        raise

def _aggregate_to_scala(column_expr, node):
    assert len(column_expr.parameter_list) == 1
    param = column_expr.parameter_list[0]
    param_str = _select_list_to_scala(param, node)
    
    if column_expr.func_name == "SUM":
        return 'x._2.map(x => {0}).sum'.format(param_str)
    elif column_expr.func_name == "AVG":
        return 'x._2.map(x => {0}).sum / x._2.map(x => {0}).length'.format(param_str)
    elif column_expr.func_name == "COUNT":
        return 'x._2.map(x => {0}).length'.format(param_str)
    elif column_expr.func_name == "COUNT_DISTINCT":
        return 'x._2.map(x => {0}).distinct.length'.format(param_str)
    elif column_expr.func_name == "MAX":
        return 'x._2.map(x => {0}).max'.format(param_str)
    else:
        raise
    return param_str

class SparkCodeEmiter(object):
    _job_name = 'YSmartSparkJob'
    _table_scheme = None
    _spark_home = ''
    _rdd_cntr = 0
    _code = ''
    _indent_dept = 0
    _indent_width = 2

    def __init__(self, table_schema, spark_home, job_name='YSmartSparkJob'):
        self._table_scheme = table_schema
        self._spark_home = spark_home
        self._job_name = job_name

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
        self._emit(_job_template.format(job_name=self._job_name, spark_home=self._spark_home))

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

    def emit_table_read(self, node):
        
        # Scan the table
        table_name = node.table_name
        global global_table_dict
        scan_rdd = self._new_rdd_name()
        all_columns = global_table_dict[table_name].column_list
        columns_count = len(all_columns)
        
        # FIXME Meisam: add code to convert date ad text types
        type_function_map = {"INTEGER":".toInt", "DECIMAL":".toFloat", "TEXT":"", "DATE":""}
        projected_columns = []
        for (column, index) in zip (all_columns, range(0, columns_count)):
            column_expr = r'columns({column_index})'.format(column_index=index)
            column_expr += type_function_map[column.column_type]
            projected_columns.append(column_expr)

        if columns_count > 1:
            line_split = '(' + ', '.join(projected_columns) + ')'
        elif columns_count == 1:
            line_split = 'Tuple1(' + ', '.join(projected_columns) + ')'
        else:
            raise

        self._emit(r'val {scan_rdd} = sc.textFile(dbDir + "/{table_name}.tbl").map(line => {{ val columns = line.split("\\|"); {line_split} }})'.format(
                scan_rdd=scan_rdd, table_name=table_name.lower(), line_split=line_split))

        # Select needed rows
        if node.where_condition:
            expr = node.where_condition.where_condition_exp
            select_code = _condition_to_scala(expr, node)

            select_rdd = self._new_rdd_name()
            self._emit('val {select_rdd} = {scan_rdd}.filter(x => {select_code})'.format(
                select_rdd=select_rdd, scan_rdd=scan_rdd, select_code=select_code))
        else:
            select_rdd = scan_rdd

        # Project needed columns
        projected_columns = []
        for column in node.select_list.tmp_exp_list:
            if isinstance(column, YRawColExp):
                column_index = column.column_name + 1  # + 1 for 1-based tuples in Scala
                column_expr = r'x._{column_index}'.format(column_index=column_index)
            elif isinstance(column, YConsExp):
                column_expr = str(column.cons_value)
            elif isinstance(column, YFuncExp):
                raise
            else:
                raise
            projected_columns.append(column_expr)
        
        if (len(projected_columns) > 1):
            project_code = '({columns})'.format(columns=', '.join(projected_columns))
        elif (len(projected_columns) == 1):
            project_code = 'Tuple1({column})'.format(column=projected_columns[0])
        else:
            raise RuntimeError()
        
        project_rdd = self._new_rdd_name()
        self._emit('val {project_rdd} = {select_rdd}.map(x => {project_code})'.format(
                project_rdd=project_rdd, select_rdd=select_rdd, project_code=project_code))
        
        return project_rdd
    
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

    def emit_group_by(self, node, child_rdd):
        group_by_rdd = self._new_rdd_name()
        
        group_by_columns = []
        for column in node.group_by_clause.groupby_exp_list:
            if isinstance(column, YRawColExp):
                group_by_columns.append('x._{0}'.format(column.column_name+1))
            elif isinstance(column, YConsExp):
                group_by_columns.append(str(column.cons_value))
            else:
                raise
        
        if len(group_by_columns) == 1:
            group_by_filter = ' x => Tuple1({column})'.format(column=group_by_columns[0])
        elif len(group_by_columns) > 1:
            group_by_filter = ' x => ({column})'.format(column=', '.join(group_by_columns))
        else:
            raise RuntimeError('Unknown group by clause: {0}'.format(node.group_by_clause))

        self._emit('val {rdd_name} = {child_rdd}.groupBy({group_by_filter})'
                   .format(rdd_name=group_by_rdd, child_rdd=child_rdd, group_by_filter=group_by_filter))
        
        aggregate_rdd = self._new_rdd_name()
        
        grouped_columns = []
        for column in node.group_by_clause.groupby_exp_list:
            if isinstance(column, YRawColExp):
                grouped_columns.append('x._1._{0}'.format(column.column_name+1))
            elif isinstance(column, YConsExp):
                grouped_columns.append(str(column.cons_value))
            else:
                raise
            
        if len(grouped_columns) == 1:
            grouped_columns_str = grouped_columns[0]
        elif len(grouped_columns) > 1:
             grouped_columns_str = ', '.join(grouped_columns)
        else:
            raise RuntimeError("Unknown number of grouped columns")
        
        aggregated_columns = []
        for column_expr in node.select_list.tmp_exp_list:
            if isinstance(column_expr, YRawColExp):
                continue # raise # continue but really
            else:
                aggregated_columns.append(column_expr)
                
        agg_str = [_aggregate_to_scala(column, node) for column in aggregated_columns]
        
        print("DEBUG: AGG COLUMNS {0}".format(agg_str))
        if len(agg_str) == 1:
            aggregated_columns_str = agg_str[0]
        elif len(aggregated_columns) > 1:
             aggregated_columns_str = ', '.join(agg_str)
        else:
            raise RuntimeError("Unknown number of grouped columns")


        if len(grouped_columns) + len(aggregated_columns) == 1:
            if grouped_columns:
                all_columns_scala = 'Tuple({0})'.format(grouped_columns_str)
            elif aggregated_columns:
                all_columns_scala = 'Tuple({0})'.format(aggregated_columns_str)
            else:
                raise
        elif len(grouped_columns) + len(aggregated_columns) > 1:
            all_columns_scala = '({grouped}, {aggregated})'.format(grouped=grouped_columns_str, aggregated=aggregated_columns_str)
        else:
            raise
#         for column_expr in aggregated_columns:
#             column_str = _select_list_to_scala(column_expr, node)
#             grouped_columns.append(column_str)
# 
#         if len(grouped_columns) == 1:
#             all_columns_scala = grouped_columns[0]
#         elif len(grouped_columns) > 1:
#             all_columns_scala = ', '.join(grouped_columns)
#         else:
#             raise

        self._emit('val {aggregate_rdd} = {group_by_rdd}.map(x => {all_columns_scala})'.
                   format(aggregate_rdd=aggregate_rdd, group_by_rdd=group_by_rdd,
                          all_columns_scala=all_columns_scala))
        return aggregate_rdd
        
    def emit_save_to_file(self, rdd_name):
        self._emit('{rdd}.saveAsTextFile(outputDir + "/{sub_path}")'.format(rdd=rdd_name, sub_path=self._job_name))
        
    def emit_order_by(self, node, child_rdd, ascending):
        rdd = self._new_rdd_name()
        # FIXME: there is no sort support in Spark
#         self._emit('val {rdd} = {child_rdd}.orderBy({order}, {partition_count})'
#                    .format(rdd=rdd, child_rdd=child_rdd, 
#                     order= 'true' if ascending else 'false', partition_count=1))
        return child_rdd

def spark_code(node, job_name, spark_home):
    # /home/fathi/workspace/spark/examples/target/scala-2.9.3/spark-examples_2.9.3-0.8.0-SNAPSHOT.jar
    
    global global_table_dict
    
    code_emitter = SparkCodeEmiter(global_table_dict, spark_home, job_name)
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
    
    assert join_node.join_condition
    
    if isinstance(join_node.join_condition, FirstStepWhereCondition):
        condition_exp = join_node.join_condition.where_condition_exp
    elif isinstance(join_node.join_condition, FirstStepOnCondition):
        condition_exp = join_node.join_condition.on_condition_exp
    else:
        raise

    func_name = condition_exp.func_name
    parameter_list = condition_exp.parameter_list
    
    if func_name == "EQ":
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
        column_index = lookup_column_index(left_param, join_node.left_child)
        left_column = 'x._1._{index}'.format(index=column_index + 1)  # Scala starts tuple indexes from 1 and not from 0

        column_index = lookup_column_index(right_param, join_node.right_child)
        right_column = 'x._2._{index}'.format(index=column_index + 1)  # Scala starts tuple indexes from 1 and not from 0

        return 'x => {left_column} {operation} {right_column}'.format(
            left_column=left_column, right_column=right_column, operation='==')
    else:
        raise


def lookup_column_index(column_exp, node):
    
    if isinstance(node, TwoJoinNode):
        return column_exp.column_name
    elif isinstance(node, GroupByNode):
        return column_exp.column_name
    elif isinstance(node, TableNode):
        columns_list = node.select_list.tmp_exp_list
        for (column, index) in zip (columns_list, range(0, len(columns_list))):
            if isinstance(column, YFuncExp):
                continue # Meisam: means no join condition on aggregated columns
            elif isinstance(column, YRawColExp):
                if column.column_name == column_exp.column_name:
                    return index
    else:
        raise RuntimeError('Unknow node type is used in join')
    raise RuntimeError('{0} not found in {1}'.format(column_exp, node))

def _expr_to_scala(node, exp):
    '''
    Converts the given SQL expression to Scala expression. 
    '''
    left_child = node.left_child
    right_child = node.right_child
    
    assert left_child
    assert right_child

    if isinstance(exp, YRawColExp):
        if exp.table_name == "LEFT":
            column_index = exp.column_name
            return 'x._1._{index}'.format(index=column_index + 1)  # Scala starts tuple indexes from 1 and not from 0
        elif exp.table_name == "RIGHT":
            column_index = exp.column_name
            return 'x._2._{index}'.format(index=column_index + 1)  # Scala starts tuple indexes from 1 and not from 0
        else:
            raise
    elif isinstance(exp, YConsExp):
        return str(exp.cons_value)
    elif isinstance(exp, YFuncExp):
        params = exp.parameter_list
        assert len(params) == 2
        assert math_func_dict[exp.func_name]
        operation = math_func_dict[exp.func_name]
        expr1 = _expr_to_scala(node, params[0])
        expr2 = _expr_to_scala(node, params[1])
        return '({param1}{operation}{param2})'.format(
                 param1=expr1, operation=operation, param2=expr2)
    else:
        raise RuntimeError(repr(exp))


def _scala_join_project(join_node):
    """
    This method should be used to remove columns that should not appear in the result of the join.
    These columns are columns that are used in join but are not selected as part of the map.
    """
    expr_list = join_node.select_list.tmp_exp_list
    all_columns = [_expr_to_scala(join_node, exp) for exp in expr_list]
    if len(all_columns) > 1:
        return 'x => (' + ', '.join(all_columns) + ')'
    elif len(all_columns) == 1:
        return 'x => ' + all_columns[0]
    else:
        raise RuntimeError(repr(join_node))

def visit_ystree(node, code_emitter):

    global global_table_dict
    
    if isinstance(node, GroupByNode):
        child_rdd = visit_ystree(node.child, code_emitter)
        return code_emitter.emit_group_by(node, child_rdd)
    elif isinstance(node, SelectProjectNode):
        raise RuntimeError("Not implemented")
    elif isinstance(node, OrderByNode):
        return visit_ystree_orderby(node, code_emitter)
    elif isinstance(node, TwoJoinNode):
        left_rdd = visit_ystree(node.left_child, code_emitter)
        right_rdd = visit_ystree(node.right_child, code_emitter)
        join_condition = _scala_join_condition(node)
        join_project = _scala_join_project(node) 
        return code_emitter.emit_join(left_rdd, right_rdd, join_condition, join_project)
    elif isinstance(node, TableNode):
        return code_emitter.emit_table_read(node)
    elif isinstance(node, NoneType):
        raise RuntimeError("Not implemented")
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
    child_rdd = visit_ystree(node.child, code_emitter)
    order_list = node.order_by_clause.order_indicator_list
    
    # not all order by clauses are implemented
    if order_list:
        ascending = order_list[0] == 'ASC'
        return code_emitter.emit_order_by(node, child_rdd, ascending)
    raise RuntimeError('Not implemented yet')

