select 
    l_linestatus,
    sum(l_quantity) as sum_qty
from
    lineitem
group by
    l_linestatus;