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

// AnonymousTraversalSource interface for generating anonymous traversals
type AnonymousTraversalSource interface {
	// WithRemote used to set the DriverRemoteConnection within the AnonymousTraversalSource
	WithRemote(drc *DriverRemoteConnection) *GraphTraversalSource
}

// anonymousTraversalSource struct used to generate anonymous traversals
type anonymousTraversalSource struct {
}

var traversalSource = &anonymousTraversalSource{}

// WithRemote used to set the DriverRemoteConnection within the AnonymousTraversalSource
func (ats *anonymousTraversalSource) WithRemote(drc *DriverRemoteConnection) *GraphTraversalSource {
	return NewDefaultGraphTraversalSource().WithRemote(drc)
}

// Traversal_ gets an AnonymousTraversalSource
func Traversal_() AnonymousTraversalSource {
	return traversalSource
}

// AnonymousTraversal interface for anonymous traversals
type AnonymousTraversal interface {
	// T__ creates an empty GraphTraversal
	T__(args ...interface{}) *GraphTraversal
	// V adds the v step to the GraphTraversal
	V(args ...interface{}) *GraphTraversal
	// AddE adds the addE step to the GraphTraversal
	AddE(args ...interface{}) *GraphTraversal
	// AddV adds the addV step to the GraphTraversal
	AddV(args ...interface{}) *GraphTraversal
	// Aggregate adds the aggregate step to the GraphTraversal
	Aggregate(args ...interface{}) *GraphTraversal
	// And adds the and step to the GraphTraversal
	And(args ...interface{}) *GraphTraversal
	// As adds the as step to the GraphTraversal
	As(args ...interface{}) *GraphTraversal
	// Barrier adds the barrier step to the GraphTraversal
	Barrier(args ...interface{}) *GraphTraversal
	// Both adds the both step to the GraphTraversal
	Both(args ...interface{}) *GraphTraversal
	// BothE adds the bothE step to the GraphTraversal
	BothE(args ...interface{}) *GraphTraversal
	// BothV adds the bothV step to the GraphTraversal
	BothV(args ...interface{}) *GraphTraversal
	// Branch adds the branch step to the GraphTraversal
	Branch(args ...interface{}) *GraphTraversal
	// By adds the by step to the GraphTraversal
	By(args ...interface{}) *GraphTraversal
	// Cap adds the cap step to the GraphTraversal
	Cap(args ...interface{}) *GraphTraversal
	// Choose adds the choose step to the GraphTraversal
	Choose(args ...interface{}) *GraphTraversal
	// Coalesce adds the coalesce step to the GraphTraversal
	Coalesce(args ...interface{}) *GraphTraversal
	// Coin adds the coin step to the GraphTraversal
	Coin(args ...interface{}) *GraphTraversal
	// ConnectedComponent adds the connectedComponent step to the GraphTraversal
	ConnectedComponent(args ...interface{}) *GraphTraversal
	// Constant adds the constant step to the GraphTraversal
	Constant(args ...interface{}) *GraphTraversal
	// Count adds the count step to the GraphTraversal
	Count(args ...interface{}) *GraphTraversal
	// CyclicPath adds the cyclicPath step to the GraphTraversal
	CyclicPath(args ...interface{}) *GraphTraversal
	// Dedup adds the dedup step to the GraphTraversal
	Dedup(args ...interface{}) *GraphTraversal
	// Drop adds the drop step to the GraphTraversal
	Drop(args ...interface{}) *GraphTraversal
	// ElementMap adds the elementMap step to the GraphTraversal
	ElementMap(args ...interface{}) *GraphTraversal
	// Emit adds the emit step to the GraphTraversal
	Emit(args ...interface{}) *GraphTraversal
	// Filter adds the filter step to the GraphTraversal
	Filter(args ...interface{}) *GraphTraversal
	// FlatMap adds the flatMap step to the GraphTraversal
	FlatMap(args ...interface{}) *GraphTraversal
	// Fold adds the fold step to the GraphTraversal
	Fold(args ...interface{}) *GraphTraversal
	// From adds the from step to the GraphTraversal
	From(args ...interface{}) *GraphTraversal
	// Group adds the group step to the GraphTraversal
	Group(args ...interface{}) *GraphTraversal
	// GroupCount adds the groupCount step to the GraphTraversal
	GroupCount(args ...interface{}) *GraphTraversal
	// Has adds the has step to the GraphTraversal
	Has(args ...interface{}) *GraphTraversal
	// HasId adds the hasId step to the GraphTraversal
	HasId(args ...interface{}) *GraphTraversal
	// HasKey adds the hasKey step to the GraphTraversal
	HasKey(args ...interface{}) *GraphTraversal
	// HasLabel adds the hasLabel step to the GraphTraversal
	HasLabel(args ...interface{}) *GraphTraversal
	// HasNot adds the hasNot step to the GraphTraversal
	HasNot(args ...interface{}) *GraphTraversal
	// HasValue adds the hasValue step to the GraphTraversal
	HasValue(args ...interface{}) *GraphTraversal
	// Id adds the id step to the GraphTraversal
	Id(args ...interface{}) *GraphTraversal
	// Identity adds the identity step to the GraphTraversal
	Identity(args ...interface{}) *GraphTraversal
	// InE adds the inE step to the GraphTraversal
	InE(args ...interface{}) *GraphTraversal
	// InV adds the inV step to the GraphTraversal
	InV(args ...interface{}) *GraphTraversal
	// In adds the in step to the GraphTraversal
	In(args ...interface{}) *GraphTraversal
	// Index adds the index step to the GraphTraversal
	Index(args ...interface{}) *GraphTraversal
	// Inject adds the inject step to the GraphTraversal
	Inject(args ...interface{}) *GraphTraversal
	// Is adds the is step to the GraphTraversal
	Is(args ...interface{}) *GraphTraversal
	// Key adds the key step to the GraphTraversal
	Key(args ...interface{}) *GraphTraversal
	// Label adds the label step to the GraphTraversal
	Label(args ...interface{}) *GraphTraversal
	// Limit adds the limit step to the GraphTraversal
	Limit(args ...interface{}) *GraphTraversal
	// Local adds the local step to the GraphTraversal
	Local(args ...interface{}) *GraphTraversal
	// Loops adds the loops step to the GraphTraversal
	Loops(args ...interface{}) *GraphTraversal
	// Map adds the map step to the GraphTraversal
	Map(args ...interface{}) *GraphTraversal
	// Match adds the match step to the GraphTraversal
	Match(args ...interface{}) *GraphTraversal
	// Math adds the math step to the GraphTraversal
	Math(args ...interface{}) *GraphTraversal
	// Max adds the max step to the GraphTraversal
	Max(args ...interface{}) *GraphTraversal
	// Mean adds the mean step to the GraphTraversal
	Mean(args ...interface{}) *GraphTraversal
	// Min adds the min step to the GraphTraversal
	Min(args ...interface{}) *GraphTraversal
	// None adds the none step to the GraphTraversal
	None(args ...interface{}) *GraphTraversal
	// Not adds the not step to the GraphTraversal
	Not(args ...interface{}) *GraphTraversal
	// Option adds the option step to the GraphTraversal
	Option(args ...interface{}) *GraphTraversal
	// Optional adds the optional step to the GraphTraversal
	Optional(args ...interface{}) *GraphTraversal
	// Or adds the or step to the GraphTraversal
	Or(args ...interface{}) *GraphTraversal
	// Order adds the order step to the GraphTraversal
	Order(args ...interface{}) *GraphTraversal
	// OtherV adds the otherV step to the GraphTraversal
	OtherV(args ...interface{}) *GraphTraversal
	// Out adds the out step to the GraphTraversal
	Out(args ...interface{}) *GraphTraversal
	// OutE adds the outE step to the GraphTraversal
	OutE(args ...interface{}) *GraphTraversal
	// OutV adds the outV step to the GraphTraversal
	OutV(args ...interface{}) *GraphTraversal
	// PageRank adds the pageRank step to the GraphTraversal
	PageRank(args ...interface{}) *GraphTraversal
	// Path adds the path step to the GraphTraversal
	Path(args ...interface{}) *GraphTraversal
	// PeerPressure adds the peerPressure step to the GraphTraversal
	PeerPressure(args ...interface{}) *GraphTraversal
	// Profile adds the profile step to the GraphTraversal
	Profile(args ...interface{}) *GraphTraversal
	// Program adds the program step to the GraphTraversal
	Program(args ...interface{}) *GraphTraversal
	// Project adds the project step to the GraphTraversal
	Project(args ...interface{}) *GraphTraversal
	// Properties adds the properties step to the GraphTraversal
	Properties(args ...interface{}) *GraphTraversal
	// Property adds the property step to the GraphTraversal
	Property(args ...interface{}) *GraphTraversal
	// PropertyMap adds the propertyMap step to the GraphTraversal
	PropertyMap(args ...interface{}) *GraphTraversal
	// Range adds the range step to the GraphTraversal
	Range(args ...interface{}) *GraphTraversal
	// Read adds the read step to the GraphTraversal
	Read(args ...interface{}) *GraphTraversal
	// Repeat adds the repeat step to the GraphTraversal
	Repeat(args ...interface{}) *GraphTraversal
	// Sack adds the sack step to the GraphTraversal
	Sack(args ...interface{}) *GraphTraversal
	// Sample adds the sample step to the GraphTraversal
	Sample(args ...interface{}) *GraphTraversal
	// Select adds the select step to the GraphTraversal
	Select(args ...interface{}) *GraphTraversal
	// ShortestPath adds the shortestPath step to the GraphTraversal
	ShortestPath(args ...interface{}) *GraphTraversal
	// SideEffect adds the sideEffect step to the GraphTraversal
	SideEffect(args ...interface{}) *GraphTraversal
	// SimplePath adds the simplePath step to the GraphTraversal
	SimplePath(args ...interface{}) *GraphTraversal
	// Skip adds the skip step to the GraphTraversal
	Skip(args ...interface{}) *GraphTraversal
	// Store adds the store step to the GraphTraversal
	Store(args ...interface{}) *GraphTraversal
	// Subgraph adds the subgraph step to the GraphTraversal
	Subgraph(args ...interface{}) *GraphTraversal
	// Sum adds the sum step to the GraphTraversal
	Sum(args ...interface{}) *GraphTraversal
	// Tail adds the tail step to the GraphTraversal
	Tail(args ...interface{}) *GraphTraversal
	// TimeLimit adds the timeLimit step to the GraphTraversal
	TimeLimit(args ...interface{}) *GraphTraversal
	// Times adds the times step to the GraphTraversal
	Times(args ...interface{}) *GraphTraversal
	// To adds the to step to the GraphTraversal
	To(args ...interface{}) *GraphTraversal
	// ToE adds the toE step to the GraphTraversal
	ToE(args ...interface{}) *GraphTraversal
	// ToV adds the toV step to the GraphTraversal
	ToV(args ...interface{}) *GraphTraversal
	// Tree adds the tree step to the GraphTraversal
	Tree(args ...interface{}) *GraphTraversal
	// Unfold adds the unfold step to the GraphTraversal
	Unfold(args ...interface{}) *GraphTraversal
	// Union adds the union step to the GraphTraversal
	Union(args ...interface{}) *GraphTraversal
	// Until adds the until step to the GraphTraversal
	Until(args ...interface{}) *GraphTraversal
	// Value adds the value step to the GraphTraversal
	Value(args ...interface{}) *GraphTraversal
	// ValueMap adds the valueMap step to the GraphTraversal
	ValueMap(args ...interface{}) *GraphTraversal
	// Values adds the values step to the GraphTraversal
	Values(args ...interface{}) *GraphTraversal
	// Where adds the where step to the GraphTraversal
	Where(args ...interface{}) *GraphTraversal
	// With adds the with step to the GraphTraversal
	With(args ...interface{}) *GraphTraversal
	// Write adds the write step to the GraphTraversal
	Write(args ...interface{}) *GraphTraversal
}

type anonymousTraversal struct {
	graphTraversal func() *GraphTraversal
}

var T__ AnonymousTraversal = &anonymousTraversal{
	func() *GraphTraversal {
		return NewGraphTraversal(nil, nil, newBytecode(nil), nil)
	},
}

// T__ creates an empty GraphTraversal
func (anonymousTraversal *anonymousTraversal) T__(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.Inject(args...)
}

// V adds the v step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) V(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().V(args...)
}

// AddE adds the addE step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) AddE(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().AddE(args...)
}

// AddV adds the addV step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) AddV(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().AddV(args...)
}

// Aggregate adds the aggregate step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) Aggregate(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().Aggregate(args...)
}

// And adds the and step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) And(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().And(args...)
}

// As adds the as step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) As(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().As(args...)
}

// Barrier adds the barrier step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) Barrier(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().Barrier(args...)
}

// Both adds the both step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) Both(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().Both(args...)
}

// BothE adds the bothE step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) BothE(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().BothE(args...)
}

// BothV adds the bothV step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) BothV(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().BothV(args...)
}

// Branch adds the branch step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) Branch(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().Branch(args...)
}

// By adds the by step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) By(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().By(args...)
}

// Cap adds the cap step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) Cap(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().Cap(args...)
}

// Choose adds the choose step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) Choose(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().Choose(args...)
}

// Coalesce adds the coalesce step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) Coalesce(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().Coalesce(args...)
}

// Coin adds the coin step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) Coin(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().Coin(args...)
}

// ConnectedComponent adds the connectedComponent step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) ConnectedComponent(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().ConnectedComponent(args...)
}

// Constant adds the constant step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) Constant(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().Constant(args...)
}

// Count adds the count step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) Count(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().Count(args...)
}

// CyclicPath adds the cyclicPath step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) CyclicPath(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().CyclicPath(args...)
}

// Dedup adds the dedup step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) Dedup(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().Dedup(args...)
}

// Drop adds the drop step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) Drop(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().Drop(args...)
}

// ElementMap adds the elementMap step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) ElementMap(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().ElementMap(args...)
}

// Emit adds the emit step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) Emit(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().Emit(args...)
}

// Filter adds the filter step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) Filter(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().Filter(args...)
}

// FlatMap adds the flatMap step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) FlatMap(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().FlatMap(args...)
}

// Fold adds the fold step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) Fold(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().Fold(args...)
}

// From adds the from step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) From(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().From(args...)
}

// Group adds the group step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) Group(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().Group(args...)
}

// GroupCount adds the groupCount step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) GroupCount(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().GroupCount(args...)
}

// Has adds the has step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) Has(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().Has(args...)
}

// HasId adds the hasId step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) HasId(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().HasId(args...)
}

// HasKey adds the hasKey step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) HasKey(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().HasKey(args...)
}

// HasLabel adds the hasLabel step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) HasLabel(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().HasLabel(args...)
}

// HasNot adds the hasNot step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) HasNot(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().HasNot(args...)
}

// HasValue adds the hasValue step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) HasValue(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().HasValue(args...)
}

// Id adds the id step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) Id(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().Id(args...)
}

// Identity adds the identity step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) Identity(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().Identity(args...)
}

// InE adds the inE step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) InE(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().InE(args...)
}

// InV adds the inV step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) InV(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().InV(args...)
}

// In adds the in step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) In(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().In(args...)
}

// Index adds the index step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) Index(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().Index(args...)
}

// Inject adds the inject step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) Inject(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().Inject(args...)
}

// Is adds the is step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) Is(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().Is(args...)
}

// Key adds the key step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) Key(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().Key(args...)
}

// Label adds the label step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) Label(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().Label(args...)
}

// Limit adds the limit step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) Limit(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().Limit(args...)
}

// Local adds the local step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) Local(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().Local(args...)
}

// Loops adds the loops step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) Loops(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().Loops(args...)
}

// Map adds the map step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) Map(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().Map(args...)
}

// Match adds the match step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) Match(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().Match(args...)
}

// Math adds the math step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) Math(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().Math(args...)
}

// Max adds the max step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) Max(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().Max(args...)
}

// Mean adds the mean step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) Mean(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().Mean(args...)
}

// Min adds the min step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) Min(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().Min(args...)
}

// None adds the none step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) None(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().None(args...)
}

// Not adds the not step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) Not(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().Not(args...)
}

// Option adds the option step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) Option(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().Option(args...)
}

// Optional adds the optional step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) Optional(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().Optional(args...)
}

// Or adds the or step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) Or(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().Or(args...)
}

// Order adds the order step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) Order(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().Order(args...)
}

// OtherV adds the otherV step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) OtherV(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().OtherV(args...)
}

// Out adds the out step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) Out(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().Out(args...)
}

// OutE adds the outE step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) OutE(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().OutE(args...)
}

// OutV adds the outV step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) OutV(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().OutV(args...)
}

// PageRank adds the pageRank step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) PageRank(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().PageRank(args...)
}

// Path adds the path step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) Path(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().Path(args...)
}

// PeerPressure adds the peerPressure step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) PeerPressure(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().PeerPressure(args...)
}

// Profile adds the profile step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) Profile(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().Profile(args...)
}

// Program adds the program step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) Program(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().Program(args...)
}

// Project adds the project step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) Project(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().Project(args...)
}

// Properties adds the properties step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) Properties(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().Properties(args...)
}

// Property adds the property step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) Property(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().Property(args...)
}

// PropertyMap adds the propertyMap step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) PropertyMap(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().PropertyMap(args...)
}

// Range adds the range step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) Range(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().Range(args...)
}

// Read adds the read step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) Read(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().Read(args...)
}

// Repeat adds the repeat step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) Repeat(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().Repeat(args...)
}

// Sack adds the sack step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) Sack(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().Sack(args...)
}

// Sample adds the sample step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) Sample(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().Sample(args...)
}

// Select adds the select step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) Select(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().Select(args...)
}

// ShortestPath adds the shortestPath step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) ShortestPath(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().ShortestPath(args...)
}

// SideEffect adds the sideEffect step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) SideEffect(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().SideEffect(args...)
}

// SimplePath adds the simplePath step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) SimplePath(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().SimplePath(args...)
}

// Skip adds the skip step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) Skip(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().Skip(args...)
}

// Store adds the store step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) Store(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().Store(args...)
}

// Subgraph adds the subgraph step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) Subgraph(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().Subgraph(args...)
}

// Sum adds the sum step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) Sum(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().Sum(args...)
}

// Tail adds the tail step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) Tail(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().Tail(args...)
}

// TimeLimit adds the timeLimit step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) TimeLimit(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().TimeLimit(args...)
}

// Times adds the times step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) Times(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().Times(args...)
}

// To adds the to step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) To(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().To(args...)
}

// ToE adds the toE step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) ToE(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().ToE(args...)
}

// ToV adds the toV step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) ToV(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().ToV(args...)
}

// Tree adds the tree step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) Tree(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().Tree(args...)
}

// Unfold adds the unfold step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) Unfold(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().Unfold(args...)
}

// Union adds the union step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) Union(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().Union(args...)
}

// Until adds the until step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) Until(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().Until(args...)
}

// Value adds the value step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) Value(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().Value(args...)
}

// ValueMap adds the valueMap step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) ValueMap(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().ValueMap(args...)
}

// Values adds the values step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) Values(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().Values(args...)
}

// Where adds the where step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) Where(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().Where(args...)
}

// With adds the with step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) With(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().With(args...)
}

// Write adds the write step to the GraphTraversal
func (anonymousTraversal *anonymousTraversal) Write(args ...interface{}) *GraphTraversal {
	return anonymousTraversal.graphTraversal().Write(args...)
}
