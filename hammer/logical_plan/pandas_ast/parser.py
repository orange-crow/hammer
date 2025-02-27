import ast
import textwrap

from ...dataset.source import SUPPORT_FIEL_TYPES
from ..logical_plan import LogicalPlan
from ..operations.build_ops import _DATA_METHOD_OPERATION


class PandasParser(ast.NodeVisitor):
    def __init__(self):
        self.dag = LogicalPlan()

    def visit_Assign(self, node):
        """解析变量赋值，记录变量名及其来源"""
        if isinstance(node.targets[0], ast.Name):
            var_name = node.targets[0].id  # 变量名
            source = self._get_source(node.value)
            # dag
            if source and source.endswith(SUPPORT_FIEL_TYPES):
                self.dag.add_data_node(var_name, data_type="io", source=source)
            else:
                self.dag.add_data_node(var_name, data_type="memory")
        self.generic_visit(node)

    def visit_Call(self, node):
        """解析函数调用，例如 pd.read_csv(input_csv)"""
        func_name = self._get_func_name(node.func)
        func_args = [self._get_arg_value(arg) for arg in node.args]
        func_keywords = {kw.arg: self._get_arg_value(kw.value) for kw in node.keywords}

        # 记录数据流: 例如 read_csv 产生 df1
        if func_name.startswith("read_"):
            func_name = f"pd.{func_name}"

        output_node = self._get_parent_var(node)

        # dag
        # TODO: 更新 node_name
        func_args = [func_args] if isinstance(func_args, str) else func_args
        input_nodes = [n for n in func_args if self.dag.has_node(n)]
        self.dag.add_operation_node(
            func_name, func_name, func_args, func_keywords, input_nodes=input_nodes, output_node=output_node
        )
        self.generic_visit(node)

    def visit_Subscript(self, node):
        """解析 DataFrame 列选择，例如 df["value"]"""
        col_name = self._get_arg_value(node.slice)
        source = self._get_source(node.value)
        # dag
        # FIXME: Subscript 之后的 output_node 无法确定，或者说 Attribute 的输入节点为 Subscript 时, 无法将 Subscript作为 input_nodes 赋给 Attribute
        # FIXME: 如何将 Subscript 定死为 Dataframe的select？
        self.dag.add_operation_node("select", "select", col_name, input_nodes=source)
        self.generic_visit(node)

    def visit_Attribute(self, node):
        """解析方法调用，例如 df.sum()"""
        self.generic_visit(node)

    def _get_func_name(self, node):
        """获取函数名"""
        if isinstance(node, ast.Attribute):
            if node.attr in _DATA_METHOD_OPERATION and hasattr(node.value, "id"):
                self.dag.graph.add_edge(node.value.id, node.attr)
            # FIXME: 如果 select 是多个的话，这里hard code就很难自适应修改。
            elif node.attr in _DATA_METHOD_OPERATION and isinstance(node.value, ast.Subscript):
                self.dag.graph.add_edge("select", node.attr)
            return node.attr
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
        return None

    def _get_parent_var(self, node):
        """获取赋值变量名称"""
        parent = node.parent if hasattr(node, "parent") else None
        if isinstance(parent, ast.Assign) and isinstance(parent.targets[0], ast.Name):
            return parent.targets[0].id
        return None

    def parse(self, code):
        tree = ast.parse(textwrap.dedent((code)))
        for node in ast.walk(tree):
            for child in ast.iter_child_nodes(node):
                child.parent = node  # 给AST节点添加父节点信息
        self.visit(tree)
