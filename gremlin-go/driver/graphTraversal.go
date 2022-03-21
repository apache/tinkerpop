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

type traversalStrategy struct {
	name          string
	configuration map[string]interface{}
	apply         func(g GraphTraversal)
}

type Lambda struct {
	Script   string
	Language string
}

// GraphTraversal stores a Traversal.
type GraphTraversal struct {
	*Traversal
}

// NewGraphTraversal make a new GraphTraversal.
func NewGraphTraversal(graph *Graph, traversalStrategies *TraversalStrategies, bytecode *bytecode, remote *DriverRemoteConnection) *GraphTraversal {
	gt := &GraphTraversal{
		Traversal: &Traversal{
			graph:               graph,
			traversalStrategies: traversalStrategies,
			bytecode:            bytecode,
			remote:              remote,
		},
	}
	return gt
}

// Clone make a copy of a traversal that is reset for iteration.
func (g *GraphTraversal) Clone() *GraphTraversal {
	return NewGraphTraversal(g.graph, g.traversalStrategies, newBytecode(g.bytecode), g.remote)
}

// V adds the v step to the GraphTraversal.
func (g *GraphTraversal) V(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("V", args...)
	return g
}

// AddE adds the addE step to the GraphTraversal.
func (g *GraphTraversal) AddE(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("addE", args...)
	return g
}

// AddV adds the addV step to the GraphTraversal.
func (g *GraphTraversal) AddV(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("addV", args...)
	return g
}

// Aggregate adds the aggregate step to the GraphTraversal.
func (g *GraphTraversal) Aggregate(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("aggregate", args...)
	return g
}

// And adds the and step to the GraphTraversal.
func (g *GraphTraversal) And(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("and", args...)
	return g
}

// As adds the as step to the GraphTraversal.
func (g *GraphTraversal) As(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("as", args...)
	return g
}

// Barrier adds the barrier step to the GraphTraversal.
func (g *GraphTraversal) Barrier(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("barrier", args...)
	return g
}

// Both adds the both step to the GraphTraversal.
func (g *GraphTraversal) Both(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("both", args...)
	return g
}

// BothE adds the bothE step to the GraphTraversal.
func (g *GraphTraversal) BothE(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("bothE", args...)
	return g
}

// BothV adds the bothV step to the GraphTraversal.
func (g *GraphTraversal) BothV(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("bothV", args...)
	return g
}

// Branch adds the branch step to the GraphTraversal.
func (g *GraphTraversal) Branch(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("branch", args...)
	return g
}

// By adds the by step to the GraphTraversal.
func (g *GraphTraversal) By(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("by", args...)
	return g
}

// Cap adds the cap step to the GraphTraversal.
func (g *GraphTraversal) Cap(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("cap", args...)
	return g
}

// Choose adds the choose step to the GraphTraversal.
func (g *GraphTraversal) Choose(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("choose", args...)
	return g
}

// Coalesce adds the coalesce step to the GraphTraversal.
func (g *GraphTraversal) Coalesce(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("coalesce", args...)
	return g
}

// Coin adds the coint step to the GraphTraversal.
func (g *GraphTraversal) Coin(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("coin", args...)
	return g
}

// ConnectedComponent adds the connectedComponent step to the GraphTraversal.
func (g *GraphTraversal) ConnectedComponent(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("connectedComponent", args...)
	return g
}

// Constant adds the constant step to the GraphTraversal.
func (g *GraphTraversal) Constant(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("constant", args...)
	return g
}

// Count adds the count step to the GraphTraversal.
func (g *GraphTraversal) Count(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("count", args...)
	return g
}

// CyclicPath adds the cyclicPath step to the GraphTraversal.
func (g *GraphTraversal) CyclicPath(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("cyclicPath", args...)
	return g
}

// Dedup adds the dedup step to the GraphTraversal.
func (g *GraphTraversal) Dedup(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("dedup", args...)
	return g
}

// Drop adds the drop step to the GraphTraversal.
func (g *GraphTraversal) Drop(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("drop", args...)
	return g
}

// ElementMap adds the elementMap step to the GraphTraversal.
func (g *GraphTraversal) ElementMap(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("elementMap", args...)
	return g
}

// Emit adds the emit step to the GraphTraversal.
func (g *GraphTraversal) Emit(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("emit", args...)
	return g
}

// Filter adds the filter step to the GraphTraversal.
func (g *GraphTraversal) Filter(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("filter", args...)
	return g
}

// FlatMap adds the flatMap step to the GraphTraversal.
func (g *GraphTraversal) FlatMap(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("flatMap", args...)
	return g
}

// Fold adds the fold step to the GraphTraversal.
func (g *GraphTraversal) Fold(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("fold", args...)
	return g
}

// From adds the from step to the GraphTraversal.
func (g *GraphTraversal) From(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("from", args...)
	return g
}

// Group adds the group step to the GraphTraversal.
func (g *GraphTraversal) Group(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("group", args...)
	return g
}

// GroupCount adds the groupCount step to the GraphTraversal.
func (g *GraphTraversal) GroupCount(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("groupCount", args...)
	return g
}

// Has adds the has step to the GraphTraversal.
func (g *GraphTraversal) Has(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("has", args...)
	return g
}

// HasId adds the hasId step to the GraphTraversal.
func (g *GraphTraversal) HasId(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("hasId", args...)
	return g
}

// HasKey adds the hasKey step to the GraphTraversal.
func (g *GraphTraversal) HasKey(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("hasKey", args...)
	return g
}

// HasLabel adds the hasLabel step to the GraphTraversal.
func (g *GraphTraversal) HasLabel(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("hasLabel", args...)
	return g
}

// HasNot adds the hasNot step to the GraphTraversal.
func (g *GraphTraversal) HasNot(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("hasNot", args...)
	return g
}

// HasValue adds the hasValue step to the GraphTraversal.
func (g *GraphTraversal) HasValue(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("hasValue", args...)
	return g
}

// Id adds the id step to the GraphTraversal.
func (g *GraphTraversal) Id(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("id", args...)
	return g
}

// Identity adds the identity step to the GraphTraversal.
func (g *GraphTraversal) Identity(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("identity", args...)
	return g
}

// InE adds the inE step to the GraphTraversal.
func (g *GraphTraversal) InE(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("inE", args...)
	return g
}

// InV adds the inV step to the GraphTraversal.
func (g *GraphTraversal) InV(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("inV", args...)
	return g
}

// In adds the in step to the GraphTraversal.
func (g *GraphTraversal) In(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("in", args...)
	return g
}

// Index adds the index step to the GraphTraversal.
func (g *GraphTraversal) Index(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("index", args...)
	return g
}

// Inject adds the inject step to the GraphTraversal.
func (g *GraphTraversal) Inject(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("inject", args...)
	return g
}

// Is adds the is step to the GraphTraversal.
func (g *GraphTraversal) Is(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("is", args...)
	return g
}

// Key adds the key step to the GraphTraversal.
func (g *GraphTraversal) Key(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("key", args...)
	return g
}

// Label adds the label step to the GraphTraversal.
func (g *GraphTraversal) Label(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("label", args...)
	return g
}

// Limit adds the limit step to the GraphTraversal.
func (g *GraphTraversal) Limit(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("limit", args...)
	return g
}

// Local adds the local step to the GraphTraversal.
func (g *GraphTraversal) Local(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("local", args...)
	return g
}

// Loops adds the loops step to the GraphTraversal.
func (g *GraphTraversal) Loops(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("loops", args...)
	return g
}

// Map adds the map step to the GraphTraversal.
func (g *GraphTraversal) Map(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("map", args...)
	return g
}

// Match adds the match step to the GraphTraversal.
func (g *GraphTraversal) Match(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("match", args...)
	return g
}

// Math adds the math step to the GraphTraversal.
func (g *GraphTraversal) Math(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("math", args...)
	return g
}

// Max adds the max step to the GraphTraversal.
func (g *GraphTraversal) Max(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("max", args...)
	return g
}

// Mean adds the mean step to the GraphTraversal.
func (g *GraphTraversal) Mean(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("mean", args...)
	return g
}

// Min adds the min step to the GraphTraversal.
func (g *GraphTraversal) Min(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("min", args...)
	return g
}

// None adds the none step to the GraphTraversal.
func (g *GraphTraversal) None(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("none", args...)
	return g
}

// Not adds the not step to the GraphTraversal.
func (g *GraphTraversal) Not(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("not", args...)
	return g
}

// Option adds the option step to the GraphTraversal.
func (g *GraphTraversal) Option(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("option", args...)
	return g
}

// Optional adds the optional step to the GraphTraversal.
func (g *GraphTraversal) Optional(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("optional", args...)
	return g
}

// Or adds the or step to the GraphTraversal.
func (g *GraphTraversal) Or(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("or", args...)
	return g
}

// Order adds the order step to the GraphTraversal.
func (g *GraphTraversal) Order(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("order", args...)
	return g
}

// OtherV adds the otherV step to the GraphTraversal.
func (g *GraphTraversal) OtherV(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("otherV", args...)
	return g
}

// Out adds the out step to the GraphTraversal.
func (g *GraphTraversal) Out(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("out", args...)
	return g
}

// OutE adds the outE step to the GraphTraversal.
func (g *GraphTraversal) OutE(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("outE", args...)
	return g
}

// OutV adds the outV step to the GraphTraversal.
func (g *GraphTraversal) OutV(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("outV", args...)
	return g
}

// PageRank adds the pageRank step to the GraphTraversal.
func (g *GraphTraversal) PageRank(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("pageRank", args...)
	return g
}

// Path adds the path step to the GraphTraversal.
func (g *GraphTraversal) Path(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("path", args...)
	return g
}

// PeerPressure adds the peerPressure step to the GraphTraversal.
func (g *GraphTraversal) PeerPressure(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("peerPressure", args...)
	return g
}

// Profile adds the profile step to the GraphTraversal.
func (g *GraphTraversal) Profile(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("profile", args...)
	return g
}

// Program adds the program step to the GraphTraversal.
func (g *GraphTraversal) Program(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("program", args...)
	return g
}

// Project adds the project step to the GraphTraversal.
func (g *GraphTraversal) Project(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("project", args...)
	return g
}

// Properties adds the properties step to the GraphTraversal.
func (g *GraphTraversal) Properties(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("properties", args...)
	return g
}

// Property adds the property step to the GraphTraversal.
func (g *GraphTraversal) Property(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("property", args...)
	return g
}

// PropertyMap adds the propertyMap step to the GraphTraversal.
func (g *GraphTraversal) PropertyMap(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("propertyMap", args...)
	return g
}

// Range adds the range step to the GraphTraversal.
func (g *GraphTraversal) Range(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("range", args...)
	return g
}

// Read adds the read step to the GraphTraversal.
func (g *GraphTraversal) Read(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("read", args...)
	return g
}

// Repeat adds the repeat step to the GraphTraversal.
func (g *GraphTraversal) Repeat(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("repeat", args...)
	return g
}

// Sack adds the sack step to the GraphTraversal.
func (g *GraphTraversal) Sack(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("sack", args...)
	return g
}

// Sample adds the sample step to the GraphTraversal.
func (g *GraphTraversal) Sample(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("sample", args...)
	return g
}

// Select adds the select step to the GraphTraversal.
func (g *GraphTraversal) Select(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("select", args...)
	return g
}

// ShortestPath adds the shortestPath step to the GraphTraversal.
func (g *GraphTraversal) ShortestPath(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("shortestPath", args...)
	return g
}

// SideEffect adds the sideEffect step to the GraphTraversal.
func (g *GraphTraversal) SideEffect(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("sideEffect", args...)
	return g
}

// SimplePath adds the simplePath step to the GraphTraversal.
func (g *GraphTraversal) SimplePath(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("simplePath", args...)
	return g
}

// Skip adds the skip step to the GraphTraversal.
func (g *GraphTraversal) Skip(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("skip", args...)
	return g
}

// Store adds the store step to the GraphTraversal.
func (g *GraphTraversal) Store(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("store", args...)
	return g
}

// Subgraph adds the subgraph step to the GraphTraversal.
func (g *GraphTraversal) Subgraph(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("subgraph", args...)
	return g
}

// Sum adds the sum step to the GraphTraversal.
func (g *GraphTraversal) Sum(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("sum", args...)
	return g
}

// Tail adds the tail step to the GraphTraversal.
func (g *GraphTraversal) Tail(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("tail", args...)
	return g
}

// TimeLimit adds the timeLimit step to the GraphTraversal.
func (g *GraphTraversal) TimeLimit(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("timeLimit", args...)
	return g
}

// Times adds the times step to the GraphTraversal.
func (g *GraphTraversal) Times(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("times", args...)
	return g
}

// To adds the to step to the GraphTraversal.
func (g *GraphTraversal) To(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("to", args...)
	return g
}

// ToE adds the toE step to the GraphTraversal.
func (g *GraphTraversal) ToE(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("toE", args...)
	return g
}

// ToV adds the toV step to the GraphTraversal.
func (g *GraphTraversal) ToV(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("toV", args...)
	return g
}

// Tree adds the tree step to the GraphTraversal.
func (g *GraphTraversal) Tree(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("tree", args...)
	return g
}

// Unfold adds the unfold step to the GraphTraversal.
func (g *GraphTraversal) Unfold(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("unfold", args...)
	return g
}

// Union adds the union step to the GraphTraversal.
func (g *GraphTraversal) Union(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("union", args...)
	return g
}

// Until adds the until step to the GraphTraversal.
func (g *GraphTraversal) Until(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("until", args...)
	return g
}

// Value adds the value step to the GraphTraversal.
func (g *GraphTraversal) Value(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("value", args...)
	return g
}

// ValueMap adds the valueMap step to the GraphTraversal.
func (g *GraphTraversal) ValueMap(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("valueMap", args...)
	return g
}

// Values adds the values step to the GraphTraversal.
func (g *GraphTraversal) Values(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("values", args...)
	return g
}

// Where adds the where step to the GraphTraversal.
func (g *GraphTraversal) Where(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("where", args...)
	return g
}

// With adds the with step to the GraphTraversal.
func (g *GraphTraversal) With(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("with", args...)
	return g
}

// Write adds the write step to the GraphTraversal.
func (g *GraphTraversal) Write(args ...interface{}) *GraphTraversal {
	g.bytecode.addStep("write", args...)
	return g
}
