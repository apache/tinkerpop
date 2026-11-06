"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
"""
import pytest

from gremlin_python.structure.graph import Tree
from gremlin_python.structure.io.graphbinaryV4 import GraphBinaryWriter, GraphBinaryReader

writer = GraphBinaryWriter()
reader = GraphBinaryReader()


def build_tree():
    """
    Builds the following tree in-memory (mirrors Go's TestTreeReadAPI fixture):

        a
        |-- b
        |   |-- d
        |   |-- e
        |-- c
        |   |-- f
    """
    tree = Tree()
    a = tree.get_or_create_child("a")
    b = a.get_or_create_child("b")
    b.get_or_create_child("d")
    b.get_or_create_child("e")
    c = a.get_or_create_child("c")
    c.get_or_create_child("f")
    return tree


def test_root_nodes():
    tree = build_tree()
    assert tree.root_nodes() == ["a"]
    assert tree.child_at("a").root_nodes() == ["b", "c"]


def test_child_at():
    tree = build_tree()
    a = tree.child_at("a")
    assert isinstance(a, Tree)
    assert a.root_nodes() == ["b", "c"]


def test_child_at_missing_key_raises():
    tree = build_tree()
    with pytest.raises(KeyError):
        tree.child_at("does-not-exist")


def test_has_child():
    tree = build_tree()
    assert tree.has_child("a") is True
    assert tree.has_child("b") is False
    assert tree.child_at("a").has_child("b") is True
    assert tree.child_at("a").has_child("z") is False


def test_contains():
    tree = build_tree()
    assert tree.contains("a") is True
    assert tree.contains("d") is True
    assert tree.contains("f") is True
    assert tree.contains("z") is False


def test_find_subtree():
    tree = build_tree()
    b = tree.find_subtree("b")
    assert b is not None
    assert b.root_nodes() == ["d", "e"]
    assert tree.find_subtree("f").is_leaf()
    assert tree.find_subtree("z") is None


def test_get_or_create_child():
    tree = Tree()
    first = tree.get_or_create_child("k")
    assert isinstance(first, Tree)
    # returns the existing child on re-request rather than replacing it
    first.get_or_create_child("nested")
    second = tree.get_or_create_child("k")
    assert second is first
    assert second.has_child("nested")
    assert tree.root_nodes() == ["k"]


def test_is_leaf():
    tree = build_tree()
    assert tree.is_leaf() is False
    assert tree.find_subtree("d").is_leaf() is True
    assert Tree().is_leaf() is True


def test_node_count():
    tree = build_tree()
    # a, b, c, d, e, f
    assert tree.node_count() == 6
    assert Tree().node_count() == 0


def test_get_nodes_at_depth():
    tree = build_tree()
    assert tree.get_nodes_at_depth(0) == ["a"]
    assert tree.get_nodes_at_depth(1) == ["b", "c"]
    assert tree.get_nodes_at_depth(2) == ["d", "e", "f"]
    # out of range
    assert tree.get_nodes_at_depth(3) == []
    # negative
    assert tree.get_nodes_at_depth(-1) == []


def test_get_trees_at_depth():
    tree = build_tree()
    depth0 = tree.get_trees_at_depth(0)
    assert depth0 == [tree]
    depth1 = tree.get_trees_at_depth(1)
    assert len(depth1) == 1
    assert depth1[0].root_nodes() == ["b", "c"]
    depth2 = tree.get_trees_at_depth(2)
    roots = []
    for t in depth2:
        roots.extend(t.root_nodes())
    assert roots == ["d", "e", "f"]
    # depth 3 is the (empty) subtrees hanging off the leaf nodes d, e, f: they
    # exist as empty trees, so no nodes but three empty trees are returned.
    depth3 = tree.get_trees_at_depth(3)
    assert len(depth3) == 3
    assert all(t.is_leaf() for t in depth3)
    # depth 4 is genuinely beyond the tree's height -> empty list
    assert tree.get_trees_at_depth(4) == []
    assert tree.get_trees_at_depth(-1) == []


def test_get_leaf_nodes():
    tree = build_tree()
    assert tree.get_leaf_nodes() == ["d", "e", "f"]


def test_get_leaf_trees():
    tree = build_tree()
    leaf_trees = tree.get_leaf_trees()
    roots = [t.root_nodes()[0] for t in leaf_trees]
    assert roots == ["d", "e", "f"]
    for t in leaf_trees:
        assert len(t.root_nodes()) == 1
        assert t.child_at(t.root_nodes()[0]).is_leaf()


def test_add_tree_merge():
    base = Tree()
    a = base.get_or_create_child("a")
    a.get_or_create_child("b")

    other = Tree()
    oa = other.get_or_create_child("a")
    oa.get_or_create_child("c")
    other.get_or_create_child("x")

    base.add_tree(other)

    assert base.root_nodes() == ["a", "x"]
    # overlapping key 'a' has both 'b' (original) and 'c' (merged in)
    assert base.child_at("a").root_nodes() == ["b", "c"]
    # key present only in other adopted directly
    assert base.has_child("x")


def test_split_parents_multiple():
    tree = Tree()
    tree.get_or_create_child("a").get_or_create_child("a1")
    tree.get_or_create_child("b").get_or_create_child("b1")

    parts = tree.split_parents()
    assert len(parts) == 2
    assert parts[0].root_nodes() == ["a"]
    assert parts[0].child_at("a").root_nodes() == ["a1"]
    assert parts[1].root_nodes() == ["b"]


def test_split_parents_single():
    tree = build_tree()
    parts = tree.split_parents()
    assert parts == [tree]


def test_add_tree_none_raises():
    # Merging a None tree is invalid: add_tree dereferences other._entries,
    # which raises on a None argument (AttributeError/TypeError).
    tree = build_tree()
    with pytest.raises((AttributeError, TypeError)):
        tree.add_tree(None)


def test_element_keys_dedup_by_id_and_type():
    from gremlin_python.structure.graph import Vertex, Edge

    # Same id, differing label/properties -> same Element (Element.__eq__ is
    # isinstance(self.__class__) and id == id), so the key collapses.
    same_id = Tree()
    same_id.get_or_create_child(Vertex(1, "person"))
    same_id.get_or_create_child(Vertex(1, "software"))
    assert len(same_id.root_nodes()) == 1

    # Differing ids -> two distinct keys.
    diff_id = Tree()
    diff_id.get_or_create_child(Vertex(1))
    diff_id.get_or_create_child(Vertex(2))
    assert len(diff_id.root_nodes()) == 2

    # Same id but different concrete type (Vertex vs Edge) -> two distinct keys.
    diff_type = Tree()
    diff_type.get_or_create_child(Vertex(1))
    diff_type.get_or_create_child(Edge(1, Vertex(2), "knows", Vertex(3)))
    assert len(diff_type.root_nodes()) == 2


def test_hash_consistent_with_eq():
    # Equal trees must have equal hashes. Python's Tree.__hash__ is based on the
    # root entry count, which structurally-equal trees share.
    t1 = build_tree()
    t2 = build_tree()
    assert t1 == t2
    assert hash(t1) == hash(t2)


def test_pretty_print():
    tree = build_tree()
    expected = "\n".join([
        "|--a",
        "   |--b",
        "      |--d",
        "      |--e",
        "   |--c",
        "      |--f",
    ])
    assert tree.pretty_print() == expected
    # no trailing newline
    assert not tree.pretty_print().endswith("\n")


def test_eq_order_insensitivity():
    t1 = Tree()
    r1 = t1.get_or_create_child("root")
    r1.get_or_create_child("a")
    r1.get_or_create_child("b")

    t2 = Tree()
    r2 = t2.get_or_create_child("root")
    # insert in reversed sibling order
    r2.get_or_create_child("b")
    r2.get_or_create_child("a")

    assert t1 == t2
    assert not (t1 != t2)


def test_eq_differing_structure():
    t1 = build_tree()
    t2 = build_tree()
    assert t1 == t2
    t2.child_at("a").get_or_create_child("extra")
    assert t1 != t2


def test_null_key_round_trip():
    tree = Tree()
    tree.get_or_create_child(None).get_or_create_child("child")

    round_tripped = reader.read_object(writer.write_object(tree))

    assert isinstance(round_tripped, Tree)
    assert round_tripped.has_child(None)
    assert round_tripped.child_at(None).has_child("child")
    assert round_tripped == tree


# ---- Deserialization tests (duplicate-sibling-key dedup) ----

import io

from gremlin_python.structure.io.graphbinaryV4 import DataType, TreeIO, int32_pack


def _fq_string(s):
    """Fully-qualified GraphBinary string: {type-id}{null flag}{int32 len}{bytes}."""
    b = bytearray([DataType.string.value, 0x00])
    encoded = s.encode("utf-8")
    b.extend(int32_pack(len(encoded)))
    b.extend(encoded)
    return b


def test_deserializer_duplicate_sibling_key():
    # length=2; key='dup' appears twice with distinct children -> one merged entry.
    b = bytearray(int32_pack(2))
    # entry 1: key='dup', child={ 'a' -> {} }
    b.extend(_fq_string("dup"))
    b.extend(int32_pack(1))
    b.extend(_fq_string("a"))
    b.extend(int32_pack(0))
    # entry 2: key='dup', child={ 'b' -> {} }
    b.extend(_fq_string("dup"))
    b.extend(int32_pack(1))
    b.extend(_fq_string("b"))
    b.extend(int32_pack(0))

    tree = TreeIO._read_tree(io.BytesIO(bytes(b)), reader)
    assert tree.root_nodes() == ["dup"]
    merged = tree.child_at("dup")
    assert merged.has_child("a")
    assert merged.has_child("b")
