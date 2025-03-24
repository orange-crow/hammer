import pytest
import typeguard

from hammer.table.entity import Entity  # 替换为实际的模块名


# 测试初始化
def test_entity_initialization():
    entity = Entity(name="test_entity", join_keys=["key1", "key2"])
    assert entity.name == "test_entity"
    assert entity.join_keys == ["key1", "key2"]


# 测试相等性比较
def test_entity_equality():
    entity1 = Entity(name="test_entity", join_keys=["key1", "key2"])
    entity2 = Entity(name="test_entity", join_keys=["key1", "key2"])
    entity3 = Entity(name="test_entity", join_keys=["key3", "key4"])

    # 相同属性值的实例应该相等
    assert entity1 == entity2

    # 不同属性值的实例应该不相等
    assert entity1 != entity3


def test_entity_hash():
    entity1 = Entity(name="test_entity", join_keys=["key1", "key2"])
    entity2 = Entity(name="test_entity", join_keys=["key1", "key2"])
    entity3 = Entity(name="test_entity", join_keys=["key3", "key4"])

    # 相同属性值的实例应该有相同的哈希值
    assert hash(entity1) == hash(entity2)

    # 不同属性值的实例应该有不同的哈希值
    assert hash(entity1) != hash(entity3)


def test_entity_immutability():
    entity = Entity(name="test_entity", join_keys=["key1", "key2"])

    # 尝试修改属性值，应该抛出异常
    with pytest.raises(AttributeError):
        entity.name = "new_name"

    with pytest.raises(AttributeError):
        entity.join_keys = ["new_key"]


def test_entity_type_validation():
    with pytest.raises(typeguard.TypeCheckError):
        # name 应该是 str 类型
        Entity(name=123, join_keys=["key1", "key2"])

    with pytest.raises(typeguard.TypeCheckError):
        # join_keys 应该是 List[str] 类型
        Entity(name="test_entity", join_keys="not_a_list")
