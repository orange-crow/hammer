from hammer.logical_plan.pandas_ast.parser import PandasParser

if __name__ == "__main__":
    code = """
    import pandas as pd
    input_csv = "./context/data/sample_data.csv"
    df1 = pd.read_csv(input_csv)
    df2 = df1.groupby("category")["value"].sum()
    # df3 = df2["value"]
    """

    parser = PandasParser("input_csv", "df1")
    parser.parse(code)
    parser.dag.visualize()
