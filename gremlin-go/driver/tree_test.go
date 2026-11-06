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
	"bytes"
	"encoding/binary"
	"reflect"
	"testing"

	"golang.org/x/text/language"
)

// buildSampleTree constructs:
//
//	a
//	|--b
//	   |--c
//	d
//	|--e
func buildSampleTree() *Tree {
	tree := &Tree{}
	a := tree.GetOrCreateChild("a")
	b := a.GetOrCreateChild("b")
	b.GetOrCreateChild("c")
	d := tree.GetOrCreateChild("d")
	d.GetOrCreateChild("e")
	return tree
}

func TestTreeReadAPI(t *testing.T) {
	tree := buildSampleTree()

	// RootNodes preserves insertion order.
	roots := tree.RootNodes()
	if !reflect.DeepEqual(roots, []interface{}{"a", "d"}) {
		t.Fatalf("RootNodes = %v, want [a d]", roots)
	}

	// NodeCount recursive: a,b,c,d,e = 5
	if c := tree.NodeCount(); c != 5 {
		t.Fatalf("NodeCount = %d, want 5", c)
	}

	// ChildAt present and absent.
	childA, err := tree.ChildAt("a")
	if err != nil {
		t.Fatalf("ChildAt(a) returned error: %v", err)
	}
	if !childA.HasChild("b") {
		t.Fatalf("expected a to have child b")
	}
	if _, err := tree.ChildAt("zzz"); err == nil {
		t.Fatalf("ChildAt(zzz) expected error, got nil")
	}

	// HasChild
	if !tree.HasChild("a") || tree.HasChild("c") {
		t.Fatalf("HasChild mismatch")
	}

	// Contains (recursive)
	if !tree.Contains("c") || !tree.Contains("e") {
		t.Fatalf("Contains should find deep keys")
	}
	if tree.Contains("missing") {
		t.Fatalf("Contains should not find missing key")
	}

	// FindSubtree
	if sub := tree.FindSubtree("b"); sub == nil || !sub.HasChild("c") {
		t.Fatalf("FindSubtree(b) failed")
	}
	if sub := tree.FindSubtree("missing"); sub != nil {
		t.Fatalf("FindSubtree(missing) should be nil")
	}

	// IsLeaf
	cTree := tree.FindSubtree("c")
	if cTree == nil || !cTree.IsLeaf() {
		t.Fatalf("c should be a leaf")
	}

	// GetNodesAtDepth
	if d0 := tree.GetNodesAtDepth(0); !reflect.DeepEqual(d0, []interface{}{"a", "d"}) {
		t.Fatalf("GetNodesAtDepth(0) = %v, want [a d]", d0)
	}
	if d1 := tree.GetNodesAtDepth(1); !reflect.DeepEqual(d1, []interface{}{"b", "e"}) {
		t.Fatalf("GetNodesAtDepth(1) = %v, want [b e]", d1)
	}
	if d2 := tree.GetNodesAtDepth(2); !reflect.DeepEqual(d2, []interface{}{"c"}) {
		t.Fatalf("GetNodesAtDepth(2) = %v, want [c]", d2)
	}
	if dn := tree.GetNodesAtDepth(-1); len(dn) != 0 {
		t.Fatalf("GetNodesAtDepth(-1) should be empty")
	}
	if dbeyond := tree.GetNodesAtDepth(5); len(dbeyond) != 0 {
		t.Fatalf("GetNodesAtDepth(beyond) should be empty")
	}

	// GetTreesAtDepth(0) -> [self]
	if ts := tree.GetTreesAtDepth(0); len(ts) != 1 || ts[0] != tree {
		t.Fatalf("GetTreesAtDepth(0) should return [self]")
	}

	// GetLeafNodes
	leaves := tree.GetLeafNodes()
	if !reflect.DeepEqual(leaves, []interface{}{"c", "e"}) {
		t.Fatalf("GetLeafNodes = %v, want [c e]", leaves)
	}

	// GetLeafTrees
	leafTrees := tree.GetLeafTrees()
	if len(leafTrees) != 2 {
		t.Fatalf("GetLeafTrees len = %d, want 2", len(leafTrees))
	}

	// SplitParents
	parents := tree.SplitParents()
	if len(parents) != 2 {
		t.Fatalf("SplitParents len = %d, want 2", len(parents))
	}
}

func TestTreePrettyPrint(t *testing.T) {
	tree := buildSampleTree()
	got := tree.PrettyPrint()
	want := "|--a\n   |--b\n      |--c\n|--d\n   |--e"
	if got != want {
		t.Fatalf("PrettyPrint =\n%q\nwant\n%q", got, want)
	}
}

func TestTreeAddTreeMerge(t *testing.T) {
	t1 := &Tree{}
	t1.GetOrCreateChild("a").GetOrCreateChild("b")

	t2 := &Tree{}
	t2.GetOrCreateChild("a").GetOrCreateChild("c")
	t2.GetOrCreateChild("d")

	t1.AddTree(t2)

	a, err := t1.ChildAt("a")
	if err != nil {
		t.Fatalf("ChildAt(a) error: %v", err)
	}
	if !a.HasChild("b") || !a.HasChild("c") {
		t.Fatalf("merged a should have both b and c")
	}
	if !t1.HasChild("d") {
		t.Fatalf("merged tree should have adopted d")
	}
}

// writeBareTree encodes a Tree as a bare GraphBinary tree value (int32 length +
// fully-qualified string keys + bare child trees), matching the wire format
// consumed by readTreeValue. Keys are written as fully-qualified strings.
func writeBareTree(buf *bytes.Buffer, tree *Tree) {
	lenBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBytes, uint32(len(tree.entries)))
	buf.Write(lenBytes)
	for _, e := range tree.entries {
		// fully-qualified string key: type-id, flag, length, bytes
		buf.WriteByte(byte(stringType))
		buf.WriteByte(0x00) // not null
		key := e.key.(string)
		klen := make([]byte, 4)
		binary.BigEndian.PutUint32(klen, uint32(len(key)))
		buf.Write(klen)
		buf.WriteString(key)
		// bare child tree
		writeBareTree(buf, e.value)
	}
}

func TestTreeDeserializerRoundTrip(t *testing.T) {
	original := buildSampleTree()

	var buf bytes.Buffer
	writeBareTree(&buf, original)

	d := NewGraphBinaryDeserializer(&buf)
	result, err := d.readTree()
	if err != nil {
		t.Fatalf("readTree error: %v", err)
	}
	got, ok := result.(*Tree)
	if !ok {
		t.Fatalf("readTree returned %T, want *Tree", result)
	}

	if got.NodeCount() != original.NodeCount() {
		t.Fatalf("round-trip NodeCount = %d, want %d", got.NodeCount(), original.NodeCount())
	}
	if got.PrettyPrint() != original.PrettyPrint() {
		t.Fatalf("round-trip PrettyPrint mismatch:\ngot:\n%s\nwant:\n%s", got.PrettyPrint(), original.PrettyPrint())
	}
	childA, err := got.ChildAt("a")
	if err != nil || !childA.HasChild("b") {
		t.Fatalf("round-trip ChildAt(a) failed: %v", err)
	}
}

// newTestTreeSerializer builds a graphBinaryTypeSerializer suitable for unit
// tests, mirroring the construction in graphBinarySerializer_test.go.
func newTestTreeSerializer() *graphBinaryTypeSerializer {
	return &graphBinaryTypeSerializer{newLogHandler(&defaultLogger{}, Error, language.English)}
}

// TestTreeWriterRoundTrip exercises the real write path: it serializes a *Tree
// fully-qualified via the type serializer (which dispatches through getType ->
// treeType -> treeWriter) and reads it back with ReadFullyQualified, asserting
// the round-trip preserves structure. This replaces reliance on the hand-rolled
// writeBareTree byte helper for verifying the writer.
func TestTreeWriterRoundTrip(t *testing.T) {
	original := buildSampleTree()

	serializer := newTestTreeSerializer()

	// getType must resolve *Tree to treeType and a registered writer.
	dt, err := serializer.getType(original)
	if err != nil {
		t.Fatalf("getType(*Tree) error: %v", err)
	}
	if dt != treeType {
		t.Fatalf("getType(*Tree) = %v, want treeType (0x2b)", dt)
	}

	var buf bytes.Buffer
	if err := serializer.write(original, &buf); err != nil {
		t.Fatalf("write(*Tree) error: %v", err)
	}

	// Read back fully-qualified: the leading type-id (0x2b) and value flag must
	// dispatch through the treeType case.
	d := NewGraphBinaryDeserializer(bytes.NewReader(buf.Bytes()))
	result, err := d.ReadFullyQualified()
	if err != nil {
		t.Fatalf("ReadFullyQualified error: %v", err)
	}
	got, ok := result.(*Tree)
	if !ok {
		t.Fatalf("ReadFullyQualified returned %T, want *Tree", result)
	}

	if !got.Equals(original) {
		t.Fatalf("write/read round-trip mismatch:\ngot:\n%s\nwant:\n%s", got.PrettyPrint(), original.PrettyPrint())
	}
	if got.NodeCount() != original.NodeCount() {
		t.Fatalf("round-trip NodeCount = %d, want %d", got.NodeCount(), original.NodeCount())
	}
}

// TestTreeWriterEmptyRoundTrip verifies an empty tree survives the real
// write/read path as a leaf.
func TestTreeWriterEmptyRoundTrip(t *testing.T) {
	original := &Tree{}
	serializer := newTestTreeSerializer()

	var buf bytes.Buffer
	if err := serializer.write(original, &buf); err != nil {
		t.Fatalf("write(empty *Tree) error: %v", err)
	}

	d := NewGraphBinaryDeserializer(bytes.NewReader(buf.Bytes()))
	result, err := d.ReadFullyQualified()
	if err != nil {
		t.Fatalf("ReadFullyQualified error: %v", err)
	}
	got, ok := result.(*Tree)
	if !ok {
		t.Fatalf("ReadFullyQualified returned %T, want *Tree", result)
	}
	if !got.IsLeaf() {
		t.Fatalf("empty tree write/read round-trip should be a leaf")
	}
	if !got.Equals(original) {
		t.Fatalf("empty tree write/read round-trip should Equals the original")
	}
}

// TestTreeWriterNullKeyRoundTrip verifies the writer emits a nil key as a
// fully-qualified null and that it survives the real write/read path with its
// subtree intact (keys may be null; children are never null).
func TestTreeWriterNullKeyRoundTrip(t *testing.T) {
	original := &Tree{}
	original.GetOrCreateChild(nil).GetOrCreateChild("child")

	serializer := newTestTreeSerializer()

	var buf bytes.Buffer
	if err := serializer.write(original, &buf); err != nil {
		t.Fatalf("write(*Tree with nil key) error: %v", err)
	}

	d := NewGraphBinaryDeserializer(bytes.NewReader(buf.Bytes()))
	result, err := d.ReadFullyQualified()
	if err != nil {
		t.Fatalf("ReadFullyQualified error: %v", err)
	}
	got, ok := result.(*Tree)
	if !ok {
		t.Fatalf("ReadFullyQualified returned %T, want *Tree", result)
	}
	if !got.HasChild(nil) {
		t.Fatalf("round-trip tree should retain the nil key")
	}
	sub, err := got.ChildAt(nil)
	if err != nil {
		t.Fatalf("ChildAt(nil) error: %v", err)
	}
	if !sub.HasChild("child") {
		t.Fatalf("nil key subtree should contain 'child'")
	}
	if !got.Equals(original) {
		t.Fatalf("nil key write/read round-trip should Equals the original")
	}
}

// TestTreeWriterMatchesBareHelper verifies the production writer produces the
// same child-tree byte layout as the hand-rolled writeBareTree helper (the
// writer additionally prefixes the fully-qualified Tree type-id and value flag).
// This pins the writer to the documented wire format consumed by readTreeValue.
func TestTreeWriterMatchesBareHelper(t *testing.T) {
	original := buildSampleTree()
	serializer := newTestTreeSerializer()

	var fq bytes.Buffer
	if err := serializer.write(original, &fq); err != nil {
		t.Fatalf("write(*Tree) error: %v", err)
	}

	// Expected: {type-id 0x2b}{value flag 0x00}{bare tree bytes}.
	var expected bytes.Buffer
	expected.Write(treeType.getCodeBytes())
	expected.WriteByte(0x00)
	writeBareTree(&expected, original)

	if !bytes.Equal(fq.Bytes(), expected.Bytes()) {
		t.Fatalf("writer bytes mismatch:\ngot:  %x\nwant: %x", fq.Bytes(), expected.Bytes())
	}
}

// TestTreeElementKeyEquality verifies that graph element keys are deduplicated
// by id and concrete type, independent of other element fields.
func TestTreeElementKeyEquality(t *testing.T) {
	// Same id, differing label/properties -> one node.
	tree := &Tree{}
	tree.GetOrCreateChild(&Vertex{Element: Element{Id: 1, Label: "a"}})
	tree.GetOrCreateChild(&Vertex{Element: Element{Id: 1, Label: "b", Properties: "different"}})
	if got := len(tree.RootNodes()); got != 1 {
		t.Fatalf("same-id keys: RootNodes len = %d, want 1", got)
	}
	if got := tree.NodeCount(); got != 1 {
		t.Fatalf("same-id keys: NodeCount = %d, want 1", got)
	}
	if !tree.HasChild(&Vertex{Element: Element{Id: 1}}) {
		t.Fatalf("same-id keys: HasChild(Vertex Id:1) should be true")
	}

	// Differing ids -> two nodes.
	tree2 := &Tree{}
	tree2.GetOrCreateChild(&Vertex{Element: Element{Id: 1, Label: "a"}})
	tree2.GetOrCreateChild(&Vertex{Element: Element{Id: 2, Label: "a"}})
	if got := len(tree2.RootNodes()); got != 2 {
		t.Fatalf("differing-id keys: RootNodes len = %d, want 2", got)
	}

	// Type mismatch (same id) -> two nodes; keys are not equal.
	tree3 := &Tree{}
	tree3.GetOrCreateChild(&Vertex{Element: Element{Id: 1}})
	tree3.GetOrCreateChild(&Edge{Element: Element{Id: 1}})
	if got := len(tree3.RootNodes()); got != 2 {
		t.Fatalf("type-mismatch keys: RootNodes len = %d, want 2", got)
	}
	if tree3.HasChild(&Edge{Element: Element{Id: 1}}) != true {
		t.Fatalf("type-mismatch keys: Edge Id:1 should be present")
	}
	if !keyEquals(&Vertex{Element: Element{Id: 1}}, &Vertex{Element: Element{Id: 1}}) {
		t.Fatalf("keyEquals(Vertex Id:1, Vertex Id:1) should be true")
	}
	if keyEquals(&Vertex{Element: Element{Id: 1}}, &Edge{Element: Element{Id: 1}}) {
		t.Fatalf("keyEquals(Vertex Id:1, Edge Id:1) should be false (type mismatch)")
	}
}

// TestTreeElementKeyNilPointer verifies that a typed-nil element pointer key
// does not panic and is treated as a non-element value key.
func TestTreeElementKeyNilPointer(t *testing.T) {
	var nilV *Vertex
	if _, ok := elementId(nilV); ok {
		t.Fatalf("elementId(nil *Vertex) should return ok=false")
	}
	// Must not panic.
	if keyEquals(nilV, nilV) != true {
		t.Fatalf("keyEquals(nil *Vertex, nil *Vertex) should be true")
	}
	if keyEquals(nilV, &Vertex{Element: Element{Id: 1}}) {
		t.Fatalf("keyEquals(nil *Vertex, Vertex Id:1) should be false")
	}
}

// TestTreeEquals verifies order-insensitive structural equality.
func TestTreeEquals(t *testing.T) {
	// nil handling.
	var n1, n2 *Tree
	if !n1.Equals(n2) {
		t.Fatalf("nil.Equals(nil) should be true")
	}
	if (&Tree{}).Equals(nil) {
		t.Fatalf("empty.Equals(nil) should be false")
	}
	if n1.Equals(&Tree{}) {
		t.Fatalf("nil.Equals(empty) should be false")
	}

	// Equal trees regardless of sibling insertion order.
	t1 := &Tree{}
	t1.GetOrCreateChild("a").GetOrCreateChild("b")
	t1.GetOrCreateChild("c")

	t2 := &Tree{}
	t2.GetOrCreateChild("c")
	t2.GetOrCreateChild("a").GetOrCreateChild("b")

	if !t1.Equals(t2) {
		t.Fatalf("trees with same structure (different sibling order) should be Equals")
	}
	if !t2.Equals(t1) {
		t.Fatalf("Equals should be symmetric")
	}

	// Structurally different trees are not equal.
	t3 := &Tree{}
	t3.GetOrCreateChild("a").GetOrCreateChild("x")
	t3.GetOrCreateChild("c")
	if t1.Equals(t3) {
		t.Fatalf("trees differing in a subtree key should not be Equals")
	}

	// Different number of entries.
	t4 := &Tree{}
	t4.GetOrCreateChild("a").GetOrCreateChild("b")
	if t1.Equals(t4) {
		t.Fatalf("trees with differing entry counts should not be Equals")
	}

	// Empty trees are equal.
	if !(&Tree{}).Equals(&Tree{}) {
		t.Fatalf("two empty trees should be Equals")
	}
}

// TestTreeDeserializerEmptyTree verifies an empty tree round-trips through the
// bare GraphBinary tree wire format.
func TestTreeDeserializerEmptyTree(t *testing.T) {
	original := &Tree{}

	var buf bytes.Buffer
	writeBareTree(&buf, original)

	d := NewGraphBinaryDeserializer(&buf)
	result, err := d.readTree()
	if err != nil {
		t.Fatalf("readTree error: %v", err)
	}
	got, ok := result.(*Tree)
	if !ok {
		t.Fatalf("readTree returned %T, want *Tree", result)
	}
	if !got.IsLeaf() {
		t.Fatalf("empty tree round-trip should be a leaf")
	}
	if got.NodeCount() != 0 {
		t.Fatalf("empty tree round-trip NodeCount = %d, want 0", got.NodeCount())
	}
	if !got.Equals(original) {
		t.Fatalf("empty tree round-trip should Equals the original")
	}
}

// ---- Deserialization tests (dedup, null-key round-trip) ----

// putInt32 appends a big-endian int32 to buf, matching the bare GraphBinary
// length encoding.
func putInt32(buf *bytes.Buffer, n int32) {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, uint32(n))
	buf.Write(b)
}

// writeFQString writes a fully-qualified GraphBinary string: {type-id}{null
// flag}{int32 length}{bytes}.
func writeFQString(buf *bytes.Buffer, s string) {
	buf.WriteByte(byte(stringType))
	buf.WriteByte(0x00) // not null
	putInt32(buf, int32(len(s)))
	buf.WriteString(s)
}

// writeFQNull writes a fully-qualified GraphBinary null: {type-id=nullType}
// {null flag=0x01}. Unlike the string-cast helpers it does not coerce the key
// to a string, so it round-trips an actual nil key.
func writeFQNull(buf *bytes.Buffer) {
	buf.WriteByte(byte(nullType))
	buf.WriteByte(0x01)
}

// TestTreeDeserializerDuplicateSiblingKey verifies that a bare stream whose root
// key appears twice with distinct children collapses into a single merged root
// entry containing both children, matching the JS/Python/.NET GLVs.
func TestTreeDeserializerDuplicateSiblingKey(t *testing.T) {
	var buf bytes.Buffer
	// length=2
	putInt32(&buf, 2)
	// entry 1: key="dup", child={ "a" -> {} }
	writeFQString(&buf, "dup")
	putInt32(&buf, 1)
	writeFQString(&buf, "a")
	putInt32(&buf, 0)
	// entry 2: key="dup", child={ "b" -> {} }
	writeFQString(&buf, "dup")
	putInt32(&buf, 1)
	writeFQString(&buf, "b")
	putInt32(&buf, 0)

	d := NewGraphBinaryDeserializer(&buf)
	result, err := d.readTree()
	if err != nil {
		t.Fatalf("readTree error: %v", err)
	}
	got := result.(*Tree)
	if len(got.entries) != 1 {
		t.Fatalf("duplicate sibling keys should collapse to 1 entry, got %d", len(got.entries))
	}
	if !reflect.DeepEqual(got.RootNodes(), []interface{}{"dup"}) {
		t.Fatalf("root nodes = %v, want [dup]", got.RootNodes())
	}
	merged, err := got.ChildAt("dup")
	if err != nil {
		t.Fatalf("ChildAt(dup) error: %v", err)
	}
	if !merged.HasChild("a") || !merged.HasChild("b") {
		t.Fatalf("merged subtree should contain both a and b, got %v", merged.RootNodes())
	}
}

// TestTreeDeserializerNullKeyRoundTrip verifies that a nil key (encoded as a
// fully-qualified null, not coerced to a string) survives deserialization with
// its subtree intact.
func TestTreeDeserializerNullKeyRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	// root: one entry whose key is a fully-qualified null and whose child has
	// key "child".
	putInt32(&buf, 1)
	writeFQNull(&buf)
	putInt32(&buf, 1)
	writeFQString(&buf, "child")
	putInt32(&buf, 0)

	d := NewGraphBinaryDeserializer(&buf)
	result, err := d.readTree()
	if err != nil {
		t.Fatalf("readTree error: %v", err)
	}
	got := result.(*Tree)
	if !got.HasChild(nil) {
		t.Fatalf("deserialized tree should have a nil key child")
	}
	sub, err := got.ChildAt(nil)
	if err != nil {
		t.Fatalf("ChildAt(nil) error: %v", err)
	}
	if !sub.HasChild("child") {
		t.Fatalf("nil key subtree should contain 'child'")
	}
}

// TestTreeEqualsElementKeys verifies that structural equality treats graph
// element keys by id and concrete type (via keyEquals), independent of sibling
// insertion order and other element fields.
func TestTreeEqualsElementKeys(t *testing.T) {
	t1 := &Tree{}
	t1.GetOrCreateChild(&Vertex{Element: Element{Id: 1, Label: "a"}}).
		GetOrCreateChild(&Vertex{Element: Element{Id: 2}})
	t1.GetOrCreateChild(&Edge{Element: Element{Id: 3}})

	// Same ids/types, different labels and reversed sibling order -> Equals.
	t2 := &Tree{}
	t2.GetOrCreateChild(&Edge{Element: Element{Id: 3, Label: "x"}})
	t2.GetOrCreateChild(&Vertex{Element: Element{Id: 1, Label: "b"}}).
		GetOrCreateChild(&Vertex{Element: Element{Id: 2, Label: "c"}})
	if !t1.Equals(t2) {
		t.Fatalf("trees with element keys equal by id/type should be Equals")
	}

	// Differing id in a subtree -> not Equals.
	t3 := &Tree{}
	t3.GetOrCreateChild(&Vertex{Element: Element{Id: 1}}).
		GetOrCreateChild(&Vertex{Element: Element{Id: 99}})
	t3.GetOrCreateChild(&Edge{Element: Element{Id: 3}})
	if t1.Equals(t3) {
		t.Fatalf("trees differing in an element subtree key id should not be Equals")
	}

	// Type mismatch at root (Vertex vs Edge, same id) -> not Equals.
	t4 := &Tree{}
	t4.GetOrCreateChild(&Edge{Element: Element{Id: 1}}).
		GetOrCreateChild(&Vertex{Element: Element{Id: 2}})
	t4.GetOrCreateChild(&Edge{Element: Element{Id: 3}})
	if t1.Equals(t4) {
		t.Fatalf("trees differing in element key concrete type should not be Equals")
	}
}

// ---- Common-standard cases shared across all four GLVs ----

// TestTreeAddTreeNilPanics verifies that AddTree(nil) is rejected: a nil
// argument is invalid and dereferencing it (ranging over other.entries) causes
// a nil-pointer panic, matching the addTree(null) rejection asserted by the
// other GLVs.
func TestTreeAddTreeNilPanics(t *testing.T) {
	tree := buildSampleTree()

	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("AddTree(nil) should panic, but it did not")
		}
	}()

	tree.AddTree(nil)
}

// TestTreeSplitParentsSingleRoot verifies that splitting a single-root tree
// returns a singleton slice whose one element equals the original tree.
func TestTreeSplitParentsSingleRoot(t *testing.T) {
	tree := &Tree{}
	tree.GetOrCreateChild("a").GetOrCreateChild("b")

	parents := tree.SplitParents()
	if len(parents) != 1 {
		t.Fatalf("SplitParents() on single-root tree len = %d, want 1", len(parents))
	}
	if !parents[0].Equals(tree) {
		t.Fatalf("SplitParents() single result should Equals the original tree")
	}
}

// TestTreeSplitParentsMultiRootAndEmpty verifies that a multi-root tree splits
// into N single-key trees (each retaining its original subtree), and that an
// empty tree's SplitParents() returns an empty slice.
func TestTreeSplitParentsMultiRootAndEmpty(t *testing.T) {
	// Multi-root: a -> b, c, d. Expect 3 single-key trees.
	tree := &Tree{}
	tree.GetOrCreateChild("a").GetOrCreateChild("b")
	tree.GetOrCreateChild("c")
	tree.GetOrCreateChild("d")

	parents := tree.SplitParents()
	if len(parents) != 3 {
		t.Fatalf("SplitParents() multi-root len = %d, want 3", len(parents))
	}
	gotRoots := make([]interface{}, 0, len(parents))
	for _, p := range parents {
		if len(p.RootNodes()) != 1 {
			t.Fatalf("each split tree should have exactly 1 root, got %v", p.RootNodes())
		}
		gotRoots = append(gotRoots, p.RootNodes()[0])
	}
	if !reflect.DeepEqual(gotRoots, []interface{}{"a", "c", "d"}) {
		t.Fatalf("split root keys = %v, want [a c d]", gotRoots)
	}
	// The "a" split should retain its original "b" subtree.
	aSplit := parents[0]
	aChild, err := aSplit.ChildAt("a")
	if err != nil || !aChild.HasChild("b") {
		t.Fatalf("split 'a' tree should retain child 'b': err=%v", err)
	}

	// Empty tree -> empty split.
	empty := &Tree{}
	if got := empty.SplitParents(); len(got) != 0 {
		t.Fatalf("SplitParents() on empty tree len = %d, want 0", len(got))
	}
}

// TestTreeGetLeafTrees verifies that GetLeafTrees returns one single-key tree
// per leaf, and that each returned tree has exactly one root node which is a
// leaf key of the original tree.
func TestTreeGetLeafTrees(t *testing.T) {
	tree := buildSampleTree() // leaves: c, e

	leafTrees := tree.GetLeafTrees()
	if len(leafTrees) != 2 {
		t.Fatalf("GetLeafTrees len = %d, want 2", len(leafTrees))
	}

	gotLeaves := make([]interface{}, 0, len(leafTrees))
	for _, lt := range leafTrees {
		roots := lt.RootNodes()
		if len(roots) != 1 {
			t.Fatalf("each leaf tree should have exactly 1 root node, got %v", roots)
		}
		// The single root must itself be a leaf in the returned tree.
		if !lt.IsLeaf() {
			sub, err := lt.ChildAt(roots[0])
			if err != nil {
				t.Fatalf("leaf tree ChildAt(%v) error: %v", roots[0], err)
			}
			if !sub.IsLeaf() {
				t.Fatalf("leaf tree key %v should map to a leaf subtree", roots[0])
			}
		}
		gotLeaves = append(gotLeaves, roots[0])
	}
	if !reflect.DeepEqual(gotLeaves, []interface{}{"c", "e"}) {
		t.Fatalf("GetLeafTrees root keys = %v, want [c e]", gotLeaves)
	}
}

// TestTreeChildAtMissingKey verifies the Go idiom for a missing key: ChildAt
// returns a non-nil error and a nil tree.
func TestTreeChildAtMissingKey(t *testing.T) {
	tree := buildSampleTree()

	child, err := tree.ChildAt("missing")
	if err == nil {
		t.Fatalf("ChildAt(missing) should return a non-nil error")
	}
	if child != nil {
		t.Fatalf("ChildAt(missing) should return a nil tree, got %v", child)
	}
}
