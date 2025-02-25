from hammer.logical_plan import DAG

# Example usage
dag = DAG()

# Add data IO node
dag.add_data_node("input_csv", "io", "data.csv")

# Add operation node (read CSV and store in memory)
dag.add_operation_node("read_csv", {"sep": ","}, "input_csv", "df1")

# Add memory data node
dag.add_data_node("df1", "memory")

# Add operation node (groupby operation)
dag.add_operation_node("groupby", {"by": "a"}, "df1", "select")
dag.add_operation_node("select", {"b"}, "groupby", "sum")
dag.add_operation_node("sum", {}, "select", "df2")

dag.add_data_node("df2", "memory")

# to pyspark
# dag.to_pyspark()
print(dag["read_csv"])
# Visualize the DAG
dag.visualize()
