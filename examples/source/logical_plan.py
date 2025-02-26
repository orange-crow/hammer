from hammer.logical_plan import LogicalPlan

# Example usage
dag = LogicalPlan()

# Add data IO node
dag.add_data_node("input_csv", "io", "data.csv")

# Add memory data node
dag.add_data_node("df1", "memory")

# Add operation node (read CSV and store in memory)
dag.add_operation_node("read_csv_0", "pd.read_csv", ["input_csv"], {"sep": ","}, "input_csv", "df1")

dag.add_data_node("df2", "memory")
# Add operation node (groupby operation)
dag.add_operation_node("groupby_0", "groupby", [], {"by": "a"}, "df1", "select_0")
dag.add_operation_node("select_0", "select", ["b"], {}, "groupby_0", "sum_0")
dag.add_operation_node("sum_0", "sum", [], {}, "select_0", "df2")

# to pyspark
print(dag.to_pyspark("input_csv", "df2"))

# Visualize the DAG
# dag.visualize()
