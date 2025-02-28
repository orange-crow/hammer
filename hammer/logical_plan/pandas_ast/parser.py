import ast
from typing import List, Literal, Tuple

from ...dataset.source import SUPPORT_FIEL_TYPES
from ..logical_plan import LogicalPlan
from ..operations.build_ops import _DATA_METHOD_OPERATION


class PandasParser(ast.NodeVisitor):
    def __init__(self, start_node_name: List[str], end_node_name: str = None):
        self.dag = LogicalPlan()
        self.start_node_name = [start_node_name] if isinstance(start_node_name, str) else start_node_name
        self.end_node_name = end_node_name

    def get_main_nodes(
        self, node_type: Literal["data", "op"] = "data", end_node_name: str = None, prefix: str = None
    ) -> List:
        """获取当前dag中在输入和输出最短路径上的数据或者算子节点"""
        end_node_name = end_node_name or self.end_node_name
        if node_type == "data":
            nodes = [
                n
                for start_node_name in self.start_node_name
                for n in self.dag._get_datanodes(start_node_name, end_node_name)
            ]
        elif node_type == "op":
            nodes = [
                n
                for start_node_name in self.start_node_name
                for n in self.dag._get_operation_nodes(start_node_name, end_node_name)
            ]
        else:
            raise ValueError

        if prefix:
            nodes = [n for n in nodes if n.startswith(prefix)]
        return list(set(nodes))

    def update_node_name(self, node_name: str) -> str:
        # 记录数据流: 例如 read_csv 产生 df1
        if node_name and node_name.startswith("read_"):
            node_name = f"pd.{node_name}"
        return node_name

    def visit_Assign(self, node) -> Tuple:
        """解析变量赋值，记录变量名及其来源"""
        if isinstance(node.targets[0], ast.Name):
            var_name = node.targets[0].id  # 变量名
            source = self._get_source(node.value)
            return var_name, source, node

    def visit_Call(self, node) -> List[str]:
        """解析函数调用，例如 pd.read_csv(input_csv)"""
        func_name = self._get_func_name(node.func)
        func_name = self.update_node_name(func_name)
        func_args = [self._get_arg_value(arg) for arg in node.args]
        func_keywords = {kw.arg: self._get_arg_value(kw.value) for kw in node.keywords}

        # dag
        func_args = [func_args] if isinstance(func_args, str) else func_args
        input_nodes = [n for n in func_args if self.dag.has_node(n)]
        self.dag.add_operation_node(func_name, func_name, func_args, func_keywords, input_nodes=input_nodes)
        # 返回入参中的数据节点
        return [n for n in input_nodes if self.dag[n]["type"] == "data"]

    def visit_Subscript(self, node) -> List[str]:
        """解析 DataFrame 列选择，例如 df["value"]"""
        col_name = self._get_arg_value(node.slice)
        #
        obj = self._get_source(node.value)
        # dag
        self.dag.add_operation_node("select", "select", col_name, input_nodes=obj)
        # 返回obj[x]中的obj
        return [obj]

    def _get_func_name(self, node):
        """获取函数名"""
        if isinstance(node, ast.Attribute):
            # obj.x 模式表示 df.count(), df.sum()之类的`对象.属性`代码。obj == node.value, x == node.attr；
            # df.x(): obj.x 中，当obj是 Dataframe 之类的python变量，而.x是是方法链函数时
            if node.attr in _DATA_METHOD_OPERATION and isinstance(node.value, ast.Name):
                self.dag.add_edge(node.value.id, node.attr)
            # ["col"].x(): 当obj是Subscript类型，而.x是是方法链函数，默认obj为 select算子
            elif node.attr in _DATA_METHOD_OPERATION and isinstance(node.value, ast.Subscript):
                self.dag.add_edge("select", node.attr)
            return node.attr
        # node为变量名
        elif isinstance(node, ast.Name):
            return node.id
        return None

    def _get_arg_value(self, node):
        """获取参数值，可能是字符串、数字或变量"""
        if isinstance(node, ast.Constant):
            return node.value
        elif isinstance(node, ast.Name):
            return node.id
        return None

    def _get_source(self, node):
        """获取赋值来源"""
        if isinstance(node, ast.Call):
            return self._get_func_name(node.func)
        elif isinstance(node, ast.Name):
            return node.id
        elif isinstance(node, ast.Constant):
            return node.value
        elif isinstance(node, ast.Subscript):
            return "select"
        return None

    def _get_parent_var(self, node):
        """获取赋值变量名称"""
        parent = node.parent if hasattr(node, "parent") else None
        if isinstance(parent, ast.Assign) and isinstance(parent.targets[0], ast.Name):
            return parent.targets[0].id
        return None

    def parse_line(self, line):
        try:
            tree = ast.parse(line)
            var_name, source, assign_node, right_var_names = None, None, None, []
            for node in ast.walk(tree):
                for child in ast.iter_child_nodes(node):
                    child.parent = node  # 给AST节点添加父节点信息
                if isinstance(node, ast.Assign):
                    var_name, source, assign_node = self.visit_Assign(node)
                elif isinstance(node, ast.Call):
                    right_var_names.extend(self.visit_Call(node))
                elif isinstance(node, ast.Subscript):
                    right_var_names.extend(self.visit_Subscript(node))
            return var_name, source, right_var_names, assign_node
        except SyntaxError:
            print(f"语法错误: {line}")
            return var_name, source, right_var_names, assign_node

    def add_data_node(self, var_name, source, assign_node: ast.Assign):
        source = self.update_node_name(source)
        if source:
            if not isinstance(assign_node.value, ast.Constant):
                self.dag.add_data_node(var_name, data_type="memory")
            elif source.endswith(SUPPORT_FIEL_TYPES):
                self.dag.add_data_node(var_name, data_type="io", source=source)

    def parse(self, code: str):
        # TODO: 解析自定义函数为udf节点
        lines = code.strip().split("\n")
        for line in lines:
            if line.strip():  # 忽略空行
                var_name, source, right_var_names, assign_node = self.parse_line(line.strip())
                if var_name:
                    # create right var_name as data node
                    self.add_data_node(var_name, source, assign_node)
                    self.dag.add_edge(
                        self.update_node_name(source), self.update_node_name(var_name), var_name in right_var_names
                    )
                    self.end_node_name = self.dag.get_last_node(var_name)
                    self.dag.visualize()
