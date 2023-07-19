/*
Licensed to the Apache Software Foundation (ASF) Under one
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
	"math"
	"sync"
)

type Lambda struct {
	Script   string
	Language string
}

// GraphTraversal stores a Traversal.
type GraphTraversal struct {
	*Traversal
}

// NewGraphTraversal make a new GraphTraversal.
// why is this taking a non exported field as an exported function - remove or replace?
func NewGraphTraversal(graph *Graph, bytecode *Bytecode, remote *DriverRemoteConnection) *GraphTraversal {
	gt := &GraphTraversal{
		Traversal: &Traversal{
			graph:    graph,
			Bytecode: bytecode,
			remote:   remote,
		},
	}
	return gt
}

// Clone make a copy of a traversal that is reset for iteration.
func (g *GraphTraversal) Clone() *GraphTraversal {
	return NewGraphTraversal(g.graph, NewBytecode(g.Bytecode), g.remote)
}

// V adds the v step to the GraphTraversal.
func (g *GraphTraversal) V(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("V", args...)
	return g
}

// E adds the e step to the GraphTraversal.
func (g *GraphTraversal) E(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("E", args...)
	return g
}

// AddE adds the addE step to the GraphTraversal.
func (g *GraphTraversal) AddE(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("addE", args...)
	return g
}

// AddV adds the addV step to the GraphTraversal.
func (g *GraphTraversal) AddV(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("addV", args...)
	return g
}

// Aggregate adds the aggregate step to the GraphTraversal.
func (g *GraphTraversal) Aggregate(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("aggregate", args...)
	return g
}

// And adds the and step to the GraphTraversal.
func (g *GraphTraversal) And(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("and", args...)
	return g
}

// As adds the as step to the GraphTraversal.
func (g *GraphTraversal) As(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("as", args...)
	return g
}

// Barrier adds the barrier step to the GraphTraversal.
func (g *GraphTraversal) Barrier(args ...interface{}) *GraphTraversal {
	// Force int32 serialization for valid number values for server compatibility
	g.Bytecode.AddStep("barrier", int32Args(args)...)
	return g
}

// Both adds the both step to the GraphTraversal.
func (g *GraphTraversal) Both(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("both", args...)
	return g
}

// BothE adds the bothE step to the GraphTraversal.
func (g *GraphTraversal) BothE(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("bothE", args...)
	return g
}

// BothV adds the bothV step to the GraphTraversal.
func (g *GraphTraversal) BothV(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("bothV", args...)
	return g
}

// Branch adds the branch step to the GraphTraversal.
func (g *GraphTraversal) Branch(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("branch", args...)
	return g
}

// By adds the by step to the GraphTraversal.
func (g *GraphTraversal) By(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("by", args...)
	return g
}

// Call adds the call step to the GraphTraversal.
func (g *GraphTraversal) Call(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("call", args...)
	return g
}

// Cap adds the cap step to the GraphTraversal.
func (g *GraphTraversal) Cap(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("cap", args...)
	return g
}

// Choose adds the choose step to the GraphTraversal.
func (g *GraphTraversal) Choose(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("choose", args...)
	return g
}

// Coalesce adds the coalesce step to the GraphTraversal.
func (g *GraphTraversal) Coalesce(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("coalesce", args...)
	return g
}

// Coin adds the coint step to the GraphTraversal.
func (g *GraphTraversal) Coin(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("coin", args...)
	return g
}

// Concat adds the concat step to the GraphTraversal.
func (g *GraphTraversal) Concat(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("concat", args...)
	return g
}

// ConnectedComponent adds the connectedComponent step to the GraphTraversal.
func (g *GraphTraversal) ConnectedComponent(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("connectedComponent", args...)
	return g
}

// Constant adds the constant step to the GraphTraversal.
func (g *GraphTraversal) Constant(args ...interface{}) *GraphTraversal {
	// Force int32 serialization for valid number values for server compatibility
	g.Bytecode.AddStep("constant", int32Args(args)...)
	return g
}

// Count adds the count step to the GraphTraversal.
func (g *GraphTraversal) Count(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("count", args...)
	return g
}

// CyclicPath adds the cyclicPath step to the GraphTraversal.
func (g *GraphTraversal) CyclicPath(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("cyclicPath", args...)
	return g
}

// Dedup adds the dedup step to the GraphTraversal.
func (g *GraphTraversal) Dedup(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("dedup", args...)
	return g
}

// Drop adds the drop step to the GraphTraversal.
func (g *GraphTraversal) Drop(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("drop", args...)
	return g
}

// Element adds the element step to the GraphTraversal.
func (g *GraphTraversal) Element(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("element", args...)
	return g
}

// ElementMap adds the elementMap step to the GraphTraversal.
func (g *GraphTraversal) ElementMap(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("elementMap", args...)
	return g
}

// Emit adds the emit step to the GraphTraversal.
func (g *GraphTraversal) Emit(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("emit", args...)
	return g
}

// Fail adds the fail step to the GraphTraversal.
func (g *GraphTraversal) Fail(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("fail", args...)
	return g
}

// Filter adds the filter step to the GraphTraversal.
func (g *GraphTraversal) Filter(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("filter", args...)
	return g
}

// FlatMap adds the flatMap step to the GraphTraversal.
func (g *GraphTraversal) FlatMap(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("flatMap", args...)
	return g
}

// Fold adds the fold step to the GraphTraversal.
func (g *GraphTraversal) Fold(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("fold", args...)
	return g
}

// From adds the from step to the GraphTraversal.
func (g *GraphTraversal) From(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("from", args...)
	return g
}

// Group adds the group step to the GraphTraversal.
func (g *GraphTraversal) Group(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("group", args...)
	return g
}

// GroupCount adds the groupCount step to the GraphTraversal.
func (g *GraphTraversal) GroupCount(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("groupCount", args...)
	return g
}

// Has adds the has step to the GraphTraversal.
func (g *GraphTraversal) Has(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("has", args...)
	return g
}

// HasId adds the hasId step to the GraphTraversal.
func (g *GraphTraversal) HasId(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("hasId", args...)
	return g
}

// HasKey adds the hasKey step to the GraphTraversal.
func (g *GraphTraversal) HasKey(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("hasKey", args...)
	return g
}

// HasLabel adds the hasLabel step to the GraphTraversal.
func (g *GraphTraversal) HasLabel(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("hasLabel", args...)
	return g
}

// HasNot adds the hasNot step to the GraphTraversal.
func (g *GraphTraversal) HasNot(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("hasNot", args...)
	return g
}

// HasValue adds the hasValue step to the GraphTraversal.
func (g *GraphTraversal) HasValue(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("hasValue", args...)
	return g
}

// Id adds the id step to the GraphTraversal.
func (g *GraphTraversal) Id(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("id", args...)
	return g
}

// Identity adds the identity step to the GraphTraversal.
func (g *GraphTraversal) Identity(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("identity", args...)
	return g
}

// InE adds the inE step to the GraphTraversal.
func (g *GraphTraversal) InE(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("inE", args...)
	return g
}

// InV adds the inV step to the GraphTraversal.
func (g *GraphTraversal) InV(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("inV", args...)
	return g
}

// In adds the in step to the GraphTraversal.
func (g *GraphTraversal) In(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("in", args...)
	return g
}

// Index adds the index step to the GraphTraversal.
func (g *GraphTraversal) Index(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("index", args...)
	return g
}

// Inject adds the inject step to the GraphTraversal.
func (g *GraphTraversal) Inject(args ...interface{}) *GraphTraversal {
	// Force int32 serialization for valid number values for server compatibility
	g.Bytecode.AddStep("inject", int32Args(args)...)
	return g
}

// Is adds the is step to the GraphTraversal.
func (g *GraphTraversal) Is(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("is", args...)
	return g
}

// Key adds the key step to the GraphTraversal.
func (g *GraphTraversal) Key(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("key", args...)
	return g
}

// Label adds the label step to the GraphTraversal.
func (g *GraphTraversal) Label(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("label", args...)
	return g
}

// Limit adds the limit step to the GraphTraversal.
func (g *GraphTraversal) Limit(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("limit", args...)
	return g
}

// Local adds the local step to the GraphTraversal.
func (g *GraphTraversal) Local(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("local", args...)
	return g
}

// Loops adds the loops step to the GraphTraversal.
func (g *GraphTraversal) Loops(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("loops", args...)
	return g
}

// Map adds the map step to the GraphTraversal.
func (g *GraphTraversal) Map(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("map", args...)
	return g
}

// Match adds the match step to the GraphTraversal.
func (g *GraphTraversal) Match(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("match", args...)
	return g
}

// Math adds the math step to the GraphTraversal.
func (g *GraphTraversal) Math(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("math", args...)
	return g
}

// Max adds the max step to the GraphTraversal.
func (g *GraphTraversal) Max(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("max", args...)
	return g
}

// Mean adds the mean step to the GraphTraversal.
func (g *GraphTraversal) Mean(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("mean", args...)
	return g
}

// MergeE adds the mergeE step to the GraphTraversal.
func (g *GraphTraversal) MergeE(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("mergeE", args...)
	return g
}

// MergeV adds the mergeE step to the GraphTraversal.
func (g *GraphTraversal) MergeV(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("mergeV", args...)
	return g
}

// Min adds the min step to the GraphTraversal.
func (g *GraphTraversal) Min(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("min", args...)
	return g
}

// None adds the none step to the GraphTraversal.
func (g *GraphTraversal) None(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("none", args...)
	return g
}

// Not adds the not step to the GraphTraversal.
func (g *GraphTraversal) Not(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("not", args...)
	return g
}

// Option adds the option step to the GraphTraversal.
func (g *GraphTraversal) Option(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("option", args...)
	return g
}

// Optional adds the optional step to the GraphTraversal.
func (g *GraphTraversal) Optional(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("optional", args...)
	return g
}

// Or adds the or step to the GraphTraversal.
func (g *GraphTraversal) Or(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("or", args...)
	return g
}

// Order adds the order step to the GraphTraversal.
func (g *GraphTraversal) Order(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("order", args...)
	return g
}

// OtherV adds the otherV step to the GraphTraversal.
func (g *GraphTraversal) OtherV(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("otherV", args...)
	return g
}

// Out adds the out step to the GraphTraversal.
func (g *GraphTraversal) Out(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("out", args...)
	return g
}

// OutE adds the outE step to the GraphTraversal.
func (g *GraphTraversal) OutE(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("outE", args...)
	return g
}

// OutV adds the outV step to the GraphTraversal.
func (g *GraphTraversal) OutV(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("outV", args...)
	return g
}

// PageRank adds the pageRank step to the GraphTraversal.
func (g *GraphTraversal) PageRank(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("pageRank", args...)
	return g
}

// Path adds the path step to the GraphTraversal.
func (g *GraphTraversal) Path(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("path", args...)
	return g
}

// PeerPressure adds the peerPressure step to the GraphTraversal.
func (g *GraphTraversal) PeerPressure(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("peerPressure", args...)
	return g
}

// Profile adds the profile step to the GraphTraversal.
func (g *GraphTraversal) Profile(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("profile", args...)
	return g
}

// Program adds the program step to the GraphTraversal.
func (g *GraphTraversal) Program(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("program", args...)
	return g
}

// Project adds the project step to the GraphTraversal.
func (g *GraphTraversal) Project(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("project", args...)
	return g
}

// Properties adds the properties step to the GraphTraversal.
func (g *GraphTraversal) Properties(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("properties", args...)
	return g
}

// Property adds the property step to the GraphTraversal.
func (g *GraphTraversal) Property(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("property", args...)
	return g
}

// PropertyMap adds the propertyMap step to the GraphTraversal.
func (g *GraphTraversal) PropertyMap(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("propertyMap", args...)
	return g
}

// Range adds the range step to the GraphTraversal.
func (g *GraphTraversal) Range(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("range", args...)
	return g
}

// Read adds the read step to the GraphTraversal.
func (g *GraphTraversal) Read(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("read", args...)
	return g
}

// Repeat adds the repeat step to the GraphTraversal.
func (g *GraphTraversal) Repeat(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("repeat", args...)
	return g
}

// Sack adds the sack step to the GraphTraversal.
func (g *GraphTraversal) Sack(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("sack", args...)
	return g
}

// Sample adds the sample step to the GraphTraversal.
func (g *GraphTraversal) Sample(args ...interface{}) *GraphTraversal {
	// Force int32 serialization for valid number values for server compatibility
	g.Bytecode.AddStep("sample", int32Args(args)...)
	return g
}

// Select adds the select step to the GraphTraversal.
func (g *GraphTraversal) Select(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("select", args...)
	return g
}

// ShortestPath adds the shortestPath step to the GraphTraversal.
func (g *GraphTraversal) ShortestPath(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("shortestPath", args...)
	return g
}

// SideEffect adds the sideEffect step to the GraphTraversal.
func (g *GraphTraversal) SideEffect(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("sideEffect", args...)
	return g
}

// SimplePath adds the simplePath step to the GraphTraversal.
func (g *GraphTraversal) SimplePath(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("simplePath", args...)
	return g
}

// Skip adds the skip step to the GraphTraversal.
func (g *GraphTraversal) Skip(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("skip", args...)
	return g
}

// Store adds the store step to the GraphTraversal.
func (g *GraphTraversal) Store(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("store", args...)
	return g
}

// Subgraph adds the subgraph step to the GraphTraversal.
func (g *GraphTraversal) Subgraph(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("subgraph", args...)
	return g
}

// Sum adds the sum step to the GraphTraversal.
func (g *GraphTraversal) Sum(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("sum", args...)
	return g
}

// Tail adds the tail step to the GraphTraversal.
func (g *GraphTraversal) Tail(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("tail", args...)
	return g
}

// TimeLimit adds the timeLimit step to the GraphTraversal.
func (g *GraphTraversal) TimeLimit(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("timeLimit", args...)
	return g
}

// Times adds the times step to the GraphTraversal.
func (g *GraphTraversal) Times(args ...interface{}) *GraphTraversal {
	// Force int32 serialization for valid number values for server compatibility
	g.Bytecode.AddStep("times", int32Args(args)...)
	return g
}

// To adds the to step to the GraphTraversal.
func (g *GraphTraversal) To(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("to", args...)
	return g
}

// ToE adds the toE step to the GraphTraversal.
func (g *GraphTraversal) ToE(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("toE", args...)
	return g
}

// ToV adds the toV step to the GraphTraversal.
func (g *GraphTraversal) ToV(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("toV", args...)
	return g
}

// Tree adds the tree step to the GraphTraversal.
func (g *GraphTraversal) Tree(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("tree", args...)
	return g
}

// Unfold adds the unfold step to the GraphTraversal.
func (g *GraphTraversal) Unfold(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("unfold", args...)
	return g
}

// Union adds the union step to the GraphTraversal.
func (g *GraphTraversal) Union(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("union", args...)
	return g
}

// Until adds the until step to the GraphTraversal.
func (g *GraphTraversal) Until(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("until", args...)
	return g
}

// Value adds the value step to the GraphTraversal.
func (g *GraphTraversal) Value(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("value", args...)
	return g
}

// ValueMap adds the valueMap step to the GraphTraversal.
func (g *GraphTraversal) ValueMap(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("valueMap", args...)
	return g
}

// Values adds the values step to the GraphTraversal.
func (g *GraphTraversal) Values(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("values", args...)
	return g
}

// Where adds the where step to the GraphTraversal.
func (g *GraphTraversal) Where(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("where", args...)
	return g
}

// With adds the with step to the GraphTraversal.
func (g *GraphTraversal) With(args ...interface{}) *GraphTraversal {
	// Force int32 serialization for valid number values for server compatibility
	g.Bytecode.AddStep("with", int32Args(args)...)
	return g
}

// Write adds the write step to the GraphTraversal.
func (g *GraphTraversal) Write(args ...interface{}) *GraphTraversal {
	g.Bytecode.AddStep("write", args...)
	return g
}

func int32Args(args []interface{}) []interface{} {
	for i, arg := range args {
		switch val := arg.(type) {
		case uint:
			if val <= math.MaxInt32 {
				args[i] = int32(val)
			}
		case uint32:
			if val <= math.MaxInt32 {
				args[i] = int32(val)
			}
		case uint64:
			if val <= math.MaxInt32 {
				args[i] = int32(val)
			}
		case int:
			if val <= math.MaxInt32 && val >= math.MinInt32 {
				args[i] = int32(val)
			}
		case int64:
			if val <= math.MaxInt32 && val >= math.MinInt32 {
				args[i] = int32(val)
			}
		}
	}
	return args
}

type Transaction struct {
	g                      *GraphTraversalSource
	sessionBasedConnection *DriverRemoteConnection
	remoteConnection       *DriverRemoteConnection
	isOpen                 bool
	mutex                  sync.Mutex
}

func (t *Transaction) Begin() (*GraphTraversalSource, error) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	if err := t.verifyTransactionState(false, newError(err1101TransactionRepeatedOpenError)); err != nil {
		return nil, err
	}

	sc, err := t.remoteConnection.CreateSession()
	if err != nil {
		return nil, err
	}
	t.sessionBasedConnection = sc
	t.isOpen = true

	gts := &GraphTraversalSource{
		graph:            t.g.graph,
		bytecode:         t.g.bytecode,
		remoteConnection: t.sessionBasedConnection}
	return gts, nil
}

func (t *Transaction) Rollback() error {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	if err := t.verifyTransactionState(true, newError(err1102TransactionRollbackNotOpenedError)); err != nil {
		return err
	}

	return t.closeSession(t.sessionBasedConnection.rollback())
}

func (t *Transaction) Commit() error {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	if err := t.verifyTransactionState(true, newError(err1103TransactionCommitNotOpenedError)); err != nil {
		return err
	}

	return t.closeSession(t.sessionBasedConnection.commit())
}

func (t *Transaction) Close() error {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	if err := t.verifyTransactionState(true, newError(err1104TransactionRepeatedCloseError)); err != nil {
		return err
	}

	return t.closeSession(nil, nil)
}

func (t *Transaction) IsOpen() bool {
	if t.sessionBasedConnection != nil && t.sessionBasedConnection.isClosed {
		t.isOpen = false
	}
	return t.isOpen
}

func (t *Transaction) verifyTransactionState(state bool, err error) error {
	if t.IsOpen() != state {
		return err
	}
	return nil
}

func (t *Transaction) closeSession(rs ResultSet, err error) error {
	defer t.closeConnection()
	if err != nil {
		return err
	}
	if rs == nil {
		return nil
	}
	_, e := rs.All()
	return e
}

func (t *Transaction) closeConnection() {
	t.sessionBasedConnection.Close()

	// remove session based connection from spawnedSessions
	connectionCount := len(t.remoteConnection.spawnedSessions)
	if connectionCount > 0 {
		for i, x := range t.remoteConnection.spawnedSessions {
			if x == t.sessionBasedConnection {
				t.remoteConnection.spawnedSessions[i] = t.remoteConnection.spawnedSessions[connectionCount-1]
				t.remoteConnection.spawnedSessions = t.remoteConnection.spawnedSessions[:connectionCount-1]
				break
			}
		}
	}
	t.isOpen = false
}
