from hammer.logical_plan.pandas_ast.parser import PandasParser

if __name__ == "__main__":
    code = """
    import pandas as pd

    def myfunc(df):
        return df

    input_csv = "./context/data/sample_data.csv"
    df1 = pd.read_csv(input_csv)
    df2 = df1.groupby("category")["value"].sum()
    df2 = df2["value"]
    df3 = myfunc(df2)
    """

    parser = PandasParser("input_csv", "df3")
    parser.parse(code, verbose=False)
    parser.dag.visualize()
    print(parser.dag.to_pyspark("input_csv", parser.end_node_name))
