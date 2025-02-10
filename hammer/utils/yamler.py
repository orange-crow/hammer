import yaml


def read(file_path: str):
    """
    读取 YAML 文件并返回其内容。

    Args:
        file_path (str): YAML 文件的路径。

    Returns:
        dict or list or None: YAML 文件的内容，通常是字典或列表。如果文件不存在或解析出错，返回 None。

    Raises:
        FileNotFoundError: 如果文件不存在，抛出此异常。
        yaml.YAMLError: 如果 YAML 文件格式不正确，抛出此异常。
    """
    try:
        with open(file_path, "r", encoding="utf-8") as file:
            data = yaml.safe_load(file)
        return data
    except FileNotFoundError:
        print(f"文件未找到: {file_path}")
        return None
    except yaml.YAMLError as e:
        print(f"解析 YAML 文件时出错: {e}")
        return None
