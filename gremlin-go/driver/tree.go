/*
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
*/

package gremlingo

import (
	"fmt"
	"reflect"
	"strings"
)

// treeEntry is a single key/subtree pair within a Tree. The value is the child
// subtree rooted at key.
type treeEntry struct {
	key   interface{}
	value *Tree
}

// Tree is a tree data structure with a tree-shaped public API.
//
// Children are kept in an ordered slice of treeEntry, preserving insertion
// order and allowing arbitrary (including non-comparable) keys. Graph element
// keys compare by id and concrete type; all others compare by value.
type Tree struct {
	entries []treeEntry
}

// keyEquals compares two tree keys by value. Graph element keys compare by id
// and concrete type; all other keys, including nil and composite types,
// compare with reflect.DeepEqual.
func keyEquals(a, b interface{}) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	// Graph elements (Vertex, Edge, VertexProperty) compare by id and concrete
	// type, rather than by every struct field as reflect.DeepEqual would (which
	// would treat two elements with the same id but different property snapshots
	// as unequal).
	if aId, ok := elementId(a); ok {
		bId, ok2 := elementId(b)
		if !ok2 || reflect.TypeOf(a) != reflect.TypeOf(b) {
			return false
		}
		return reflect.DeepEqual(aId, bId)
	}
	// Non-element keys (scalars, composite types, nil) compare by value.
	return reflect.DeepEqual(a, b)
}

// elementId returns the id of a graph element key and true, or (nil, false) if
// the key is not a graph element. Both pointer and value forms are handled. A
// nil element pointer is treated as a non-element (nil, false) to avoid a
// typed-nil dereference panic.
func elementId(v interface{}) (interface{}, bool) {
	switch e := v.(type) {
	case *Vertex:
		if e == nil {
			return nil, false
		}
		return e.Id, true
	case *Edge:
		if e == nil {
			return nil, false
		}
		return e.Id, true
	case *VertexProperty:
		if e == nil {
			return nil, false
		}
		return e.Id, true
	case Vertex:
		return e.Id, true
	case Edge:
		return e.Id, true
	case VertexProperty:
		return e.Id, true
	}
	return nil, false
}

// RootNodes returns the keys at the root of this tree, in insertion order.
func (t *Tree) RootNodes() []interface{} {
	keys := make([]interface{}, 0, len(t.entries))
	for _, e := range t.entries {
		keys = append(keys, e.key)
	}
	return keys
}

// ChildAt returns the child subtree for the given key. It returns an error if
// no child exists for the given key.
func (t *Tree) ChildAt(key interface{}) (*Tree, error) {
	for _, e := range t.entries {
		if keyEquals(e.key, key) {
			return e.value, nil
		}
	}
	return nil, fmt.Errorf("tree has no child for key: %v", key)
}

// HasChild returns true if the given key is an immediate child of this tree.
func (t *Tree) HasChild(key interface{}) bool {
	for _, e := range t.entries {
		if keyEquals(e.key, key) {
			return true
		}
	}
	return false
}

// Equals reports whether two trees are structurally equal, ignoring the order
// in which sibling children were inserted. Two nil trees are equal; a nil tree
// is never equal to a non-nil tree. Trees are equal when they have the same
// number of entries and, for every entry in t, there is a key-matching entry in
// other (compared with keyEquals) whose subtree is recursively Equals.
func (t *Tree) Equals(other *Tree) bool {
	if t == nil || other == nil {
		return t == nil && other == nil
	}
	if len(t.entries) != len(other.entries) {
		return false
	}
	for _, e := range t.entries {
		matched := false
		for _, o := range other.entries {
			if keyEquals(e.key, o.key) {
				if e.value.Equals(o.value) {
					matched = true
				}
				break
			}
		}
		if !matched {
			return false
		}
	}
	return true
}

// Contains returns true if the given value appears as a key anywhere in this
// tree (recursive).
func (t *Tree) Contains(value interface{}) bool {
	for _, e := range t.entries {
		if keyEquals(e.key, value) {
			return true
		}
	}
	for _, e := range t.entries {
		if e.value != nil && e.value.Contains(value) {
			return true
		}
	}
	return false
}

// FindSubtree recursively searches the tree for the first subtree rooted at key
// and returns it, or nil if absent. Direct children are visited before
// recursing.
func (t *Tree) FindSubtree(key interface{}) *Tree {
	for _, e := range t.entries {
		if keyEquals(e.key, key) {
			return e.value
		}
	}
	for _, e := range t.entries {
		if e.value != nil {
			if found := e.value.FindSubtree(key); found != nil {
				return found
			}
		}
	}
	return nil
}

// GetOrCreateChild returns the existing child for key, or inserts and returns a
// new empty Tree if absent.
func (t *Tree) GetOrCreateChild(key interface{}) *Tree {
	for _, e := range t.entries {
		if keyEquals(e.key, key) {
			return e.value
		}
	}
	child := &Tree{}
	t.entries = append(t.entries, treeEntry{key: key, value: child})
	return child
}

// IsLeaf returns true if this tree has no children. An empty tree is considered
// a leaf.
func (t *Tree) IsLeaf() bool {
	return len(t.entries) == 0
}

// NodeCount returns the total number of nodes (keys) in the tree, counted
// recursively.
func (t *Tree) NodeCount() int {
	count := len(t.entries)
	for _, e := range t.entries {
		if e.value != nil {
			count += e.value.NodeCount()
		}
	}
	return count
}

// GetNodesAtDepth returns the keys at the given depth. Depth 0 returns the root
// keys. Negative depths or depths beyond the tree's height return an empty
// slice.
func (t *Tree) GetNodesAtDepth(depth int) []interface{} {
	list := make([]interface{}, 0)
	for _, tree := range t.GetTreesAtDepth(depth) {
		list = append(list, tree.RootNodes()...)
	}
	return list
}

// GetTreesAtDepth returns the trees at the given depth. Depth 0 returns a
// singleton slice containing this tree. Negative depths or depths beyond the
// tree's height return an empty slice.
func (t *Tree) GetTreesAtDepth(depth int) []*Tree {
	if depth < 0 {
		return []*Tree{}
	}
	currentDepth := []*Tree{t}
	for i := 0; i < depth; i++ {
		next := make([]*Tree, 0)
		for _, tree := range currentDepth {
			for _, e := range tree.entries {
				next = append(next, e.value)
			}
		}
		if len(next) == 0 {
			return []*Tree{}
		}
		currentDepth = next
	}
	return currentDepth
}

// GetLeafNodes returns all keys whose subtrees are leaves.
func (t *Tree) GetLeafNodes() []interface{} {
	leaves := make([]interface{}, 0)
	t.collectLeafKeys(&leaves)
	return leaves
}

func (t *Tree) collectLeafKeys(out *[]interface{}) {
	for _, e := range t.entries {
		if e.value == nil || e.value.IsLeaf() {
			*out = append(*out, e.key)
		} else {
			e.value.collectLeafKeys(out)
		}
	}
}

// GetLeafTrees returns single-key trees representing each leaf key in this tree,
// preserving the original key-to-empty-subtree mapping.
func (t *Tree) GetLeafTrees() []*Tree {
	leaves := make([]*Tree, 0)
	t.collectLeafTrees(&leaves)
	return leaves
}

func (t *Tree) collectLeafTrees(out *[]*Tree) {
	for _, e := range t.entries {
		if e.value == nil || e.value.IsLeaf() {
			leaf := &Tree{}
			leaf.entries = append(leaf.entries, treeEntry{key: e.key, value: e.value})
			*out = append(*out, leaf)
		} else {
			e.value.collectLeafTrees(out)
		}
	}
}

// AddTree recursively merges other into this tree. For overlapping keys, child
// subtrees are merged in turn. For keys present only in other, the corresponding
// subtree reference is adopted directly. A nil argument is invalid and causes a
// nil-pointer panic.
func (t *Tree) AddTree(other *Tree) {
	for _, e := range other.entries {
		if t.HasChild(e.key) {
			child, _ := t.ChildAt(e.key)
			child.AddTree(e.value)
		} else {
			t.entries = append(t.entries, treeEntry{key: e.key, value: e.value})
		}
	}
}

// SplitParents splits this tree into one tree per root key. If the tree has a
// single root, returns a singleton slice containing this tree.
func (t *Tree) SplitParents() []*Tree {
	if len(t.entries) == 1 {
		return []*Tree{t}
	}
	parents := make([]*Tree, 0, len(t.entries))
	for _, e := range t.entries {
		parentTree := &Tree{}
		parentTree.entries = append(parentTree.entries, treeEntry{key: e.key, value: e.value})
		parents = append(parents, parentTree)
	}
	return parents
}

// PrettyPrint produces a formatted string representation of the tree structure
// using a |-- ASCII style, with a 3-space indent per level and no trailing
// newline.
func (t *Tree) PrettyPrint() string {
	var builder strings.Builder
	t.prettyPrint(&builder, "")
	pretty := builder.String()
	if pretty == "" {
		return pretty
	}
	return strings.TrimSuffix(pretty, "\n")
}

func (t *Tree) prettyPrint(builder *strings.Builder, prefix string) {
	for _, e := range t.entries {
		builder.WriteString(prefix)
		builder.WriteString("|--")
		builder.WriteString(fmt.Sprintf("%v", e.key))
		builder.WriteString("\n")
		if e.value != nil {
			e.value.prettyPrint(builder, prefix+"   ")
		}
	}
}

// String returns the string representation of the tree.
func (t *Tree) String() string {
	parts := make([]string, 0, len(t.entries))
	for _, e := range t.entries {
		var sub string
		if e.value != nil {
			sub = e.value.String()
		} else {
			sub = "{}"
		}
		parts = append(parts, fmt.Sprintf("%v=%s", e.key, sub))
	}
	return "{" + strings.Join(parts, ", ") + "}"
}
