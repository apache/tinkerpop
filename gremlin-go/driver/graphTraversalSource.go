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

func convertStrategyVarargs(strategies []TraversalStrategy) []interface{} {
	converted := make([]interface{}, 0)
	for _, strategy := range strategies {
		converted = append(converted, strategy)
	}
	return converted
}

// remoteConnection abstracts the submission of traversals to a remote server.
// Both DriverRemoteConnection and transactionRemoteConnection satisfy this interface.
type remoteConnection interface {
	submitGremlinLang(gremlinLang *GremlinLang) (ResultSet, error)
	Close()
}

// GraphTraversalSource can be used to start GraphTraversal.
type GraphTraversalSource struct {
	graph            *Graph
	gremlinLang      *GremlinLang
	remoteConnection remoteConnection
	graphTraversal   *GraphTraversal
}

// NewGraphTraversalSource creates a new GraphTraversalSource from a Graph, DriverRemoteConnection, and any TraversalStrategy.
// Graph and DriverRemoteConnection can be set to nil as valid default values.
func NewGraphTraversalSource(graph *Graph, remoteConnection remoteConnection,
	traversalStrategies ...TraversalStrategy) *GraphTraversalSource {
	// TODO: revisit when updating strategies
	gl := NewGremlinLang(nil)
	if drc, ok := remoteConnection.(*DriverRemoteConnection); ok && drc != nil &&
		drc.settings != nil && drc.settings.PDTRegistry != nil {
		gl.pdtRegistry = drc.settings.PDTRegistry
	}
	return &GraphTraversalSource{graph: graph, gremlinLang: gl, remoteConnection: remoteConnection}
}

// NewDefaultGraphTraversalSource creates a new graph GraphTraversalSource without a graph, strategy, or existing traversal.
func NewDefaultGraphTraversalSource() *GraphTraversalSource {
	return &GraphTraversalSource{graph: nil, gremlinLang: NewGremlinLang(nil), remoteConnection: nil}
}

func (gts *GraphTraversalSource) GetGremlinLang() *GremlinLang {
	return gts.gremlinLang
}

// GetGraphTraversal gets the graph traversal associated with this graph traversal source.
func (gts *GraphTraversalSource) GetGraphTraversal() *GraphTraversal {
	return NewGraphTraversal(gts.graph, NewGremlinLang(gts.gremlinLang), gts.remoteConnection)
}

func (gts *GraphTraversalSource) clone() *GraphTraversalSource {
	return cloneGraphTraversalSource(gts.graph, NewGremlinLang(gts.gremlinLang), gts.remoteConnection)
}

func cloneGraphTraversalSource(graph *Graph, gl *GremlinLang, remoteConnection remoteConnection) *GraphTraversalSource {
	return &GraphTraversalSource{graph: graph,
		gremlinLang:      gl,
		remoteConnection: remoteConnection,
	}
}

// WithBulk allows for control of bulking operations.
func (gts *GraphTraversalSource) WithBulk(args ...interface{}) *GraphTraversalSource {
	source := gts.clone()
	source.gremlinLang.AddSource("withBulk", args...)
	return source
}

// WithPath adds a path to be used throughout the life of a spawned Traversal.
func (gts *GraphTraversalSource) WithPath(args ...interface{}) *GraphTraversalSource {
	source := gts.clone()
	source.gremlinLang.AddSource("withPath", args...)
	return source
}

// WithSack adds a sack to be used throughout the life of a spawned Traversal.
func (gts *GraphTraversalSource) WithSack(args ...interface{}) *GraphTraversalSource {
	source := gts.clone()
	// Force int32 serialization for valid number values for server compatibility
	source.gremlinLang.AddSource("withSack", int32Args(args)...)
	return source
}

// WithSideEffect adds a side effect to be used throughout the life of a spawned Traversal.
func (gts *GraphTraversalSource) WithSideEffect(args ...interface{}) *GraphTraversalSource {
	source := gts.clone()
	source.gremlinLang.AddSource("withSideEffect", args...)
	return source
}

// WithStrategies adds an arbitrary collection of TraversalStrategies instances to the traversal source.
func (gts *GraphTraversalSource) WithStrategies(args ...TraversalStrategy) *GraphTraversalSource {
	convertedArgs := convertStrategyVarargs(args)
	source := gts.clone()
	source.gremlinLang.AddSource("withStrategies", convertedArgs...)
	return source
}

// WithoutStrategies removes an arbitrary collection of TraversalStrategies instances to the traversal source.
func (gts *GraphTraversalSource) WithoutStrategies(args ...TraversalStrategy) *GraphTraversalSource {
	convertedArgs := convertStrategyVarargs(args)
	source := gts.clone()
	source.gremlinLang.AddSource("withoutStrategies", convertedArgs...)
	return source
}

// With provides a configuration to a traversal in the form of a key value pair.
func (gts *GraphTraversalSource) With(key interface{}, value interface{}) *GraphTraversalSource {
	source := gts.clone()

	//TODO verify remote when connection is set-up
	var optionsStrategy TraversalStrategy = nil
	if len(gts.gremlinLang.optionsStrategies) != 0 {
		optionsStrategy = gts.gremlinLang.optionsStrategies[0]
	}

	if optionsStrategy == nil {
		optionsStrategy = OptionsStrategy(map[string]interface{}{key.(string): value})
		return source.WithStrategies(optionsStrategy)
	}

	options := optionsStrategy.(*traversalStrategy)
	options.configuration[key.(string)] = value
	return source
}

// WithComputer adds a GraphComputer to be used to process the Traversal as an OLAP job. It applies a
// VertexProgramStrategy to the traversal source configured with the supplied options. Calling it with no
// configuration submits the Traversal for OLAP execution using the server's default GraphComputer.
func (gts *GraphTraversalSource) WithComputer(config ...VertexProgramStrategyConfig) *GraphTraversalSource {
	var c VertexProgramStrategyConfig
	if len(config) > 0 {
		c = config[0]
	}
	return gts.WithStrategies(VertexProgramStrategy(c))
}

// WithRemote adds a remote to be used throughout the life of a spawned Traversal.
func (gts *GraphTraversalSource) WithRemote(remoteConnection *DriverRemoteConnection) *GraphTraversalSource {
	gts.remoteConnection = remoteConnection
	if remoteConnection != nil && remoteConnection.settings != nil && remoteConnection.settings.PDTRegistry != nil {
		gts.gremlinLang.pdtRegistry = remoteConnection.settings.PDTRegistry
	}
	if gts.graphTraversal != nil {
		gts.graphTraversal.remote = remoteConnection
	}
	return gts.clone()
}

// E reads edges from the graph to start the traversal.
func (gts *GraphTraversalSource) E(args ...interface{}) *GraphTraversal {
	traversal := gts.GetGraphTraversal()
	traversal.GremlinLang.AddStep("E", args...)
	return traversal
}

// V reads vertices from the graph to start the traversal.
func (gts *GraphTraversalSource) V(args ...interface{}) *GraphTraversal {
	for i := 0; i < len(args); i++ {
		if v, ok := args[i].(*Vertex); ok {
			args[i] = v.Id
		}
	}
	traversal := gts.GetGraphTraversal()
	traversal.GremlinLang.AddStep("V", args...)
	return traversal
}

// AddE adds an Edge to start the traversal.
func (gts *GraphTraversalSource) AddE(args ...interface{}) *GraphTraversal {
	traversal := gts.GetGraphTraversal()
	traversal.GremlinLang.AddStep("addE", args...)
	return traversal
}

// AddV adds a Vertex to start the traversal.
func (gts *GraphTraversalSource) AddV(args ...interface{}) *GraphTraversal {
	traversal := gts.GetGraphTraversal()
	traversal.GremlinLang.AddStep("addV", args...)
	return traversal
}

// Call starts the traversal by executing a procedure
func (gts *GraphTraversalSource) Call(args ...interface{}) *GraphTraversal {
	traversal := gts.GetGraphTraversal()
	traversal.GremlinLang.AddStep("call", args...)
	return traversal
}

// Inject inserts arbitrary objects to start the traversal.
func (gts *GraphTraversalSource) Inject(args ...interface{}) *GraphTraversal {
	traversal := gts.GetGraphTraversal()
	// Force int32 serialization for valid number values for server compatibility
	traversal.GremlinLang.AddStep("inject", int32Args(args)...)
	return traversal
}

// Io adds the io steps to start the traversal.
func (gts *GraphTraversalSource) Io(args ...interface{}) *GraphTraversal {
	traversal := gts.GetGraphTraversal()
	traversal.GremlinLang.AddStep("io", args...)
	return traversal
}

// MergeE uses an upsert-like operation to add an Edge to start the traversal.
func (gts *GraphTraversalSource) MergeE(args ...interface{}) *GraphTraversal {
	if len(args) != 0 {
		if m, ok := args[0].(map[interface{}]interface{}); ok {
			if v, ok := m[Direction.Out].(*Vertex); ok {
				m[Direction.Out] = v.Id
			}
			if v, ok := m[Direction.In].(*Vertex); ok {
				m[Direction.In] = v.Id
			}
		}
	}
	traversal := gts.GetGraphTraversal()
	traversal.GremlinLang.AddStep("mergeE", args...)
	return traversal
}

// MergeV uses an upsert-like operation to add a Vertex to start the traversal.
func (gts *GraphTraversalSource) MergeV(args ...interface{}) *GraphTraversal {
	traversal := gts.GetGraphTraversal()
	traversal.GremlinLang.AddStep("mergeV", args...)
	return traversal
}

// Match starts the traversal with a declarative pattern match query. Accepts either a GQL
// query string (with optional bound parameters map) or the traditional traversal form.
// The query language defaults to "gql" and can be overridden with .With("queryLanguage", value).
func (gts *GraphTraversalSource) Match(args ...interface{}) *GraphTraversal {
	traversal := gts.GetGraphTraversal()
	traversal.GremlinLang.AddStep("match", args...)
	return traversal
}

// Union allows for a multi-branched start to a traversal.
func (gts *GraphTraversalSource) Union(args ...interface{}) *GraphTraversal {
	traversal := gts.GetGraphTraversal()
	traversal.GremlinLang.AddStep("union", args...)
	return traversal
}

func (gts *GraphTraversalSource) Tx() *Transaction {
	if gts.remoteConnection == nil {
		panic("remote connection is required for transactions")
	}
	// If this GTS is already bound to a transaction, return that transaction.
	if txRC, ok := gts.remoteConnection.(*transactionRemoteConnection); ok {
		return txRC.transaction
	}
	// Extract the Client from the DriverRemoteConnection.
	drc, ok := gts.remoteConnection.(*DriverRemoteConnection)
	if !ok {
		panic("transactions require a DriverRemoteConnection")
	}
	return &Transaction{client: drc.client}
}

// ExecuteInTx runs txWork inside a single, automatically managed transaction and
// returns any error that occurred.
//
// It owns the full lifecycle: it obtains a transaction via Tx, calls Begin to
// start it, passes the resulting transaction-bound GraphTraversalSource (gtx) to
// txWork, and then commits on success or rolls back on failure. The body must use
// only the gtx it receives; the non-transactional source is never in scope.
//
// This is single-shot: exactly one attempt is made (begin, run, commit/rollback)
// with no automatic retry.
//
// Error handling:
//   - If Begin fails, that error is returned and txWork is never invoked.
//   - If txWork returns a non-nil error, the transaction is rolled back and the
//     original body error is returned unchanged.
//   - If the commit fails, a rollback is attempted for server-side hygiene and
//     the commit error is returned (it takes precedence over any rollback error).
//   - If a rollback attempted during cleanup itself fails, a warning is logged
//     but the primary (body or commit) error still propagates.
//   - If txWork panics, the transaction is rolled back and the panic is
//     re-raised (it is never swallowed).
//
// For a transaction body that needs to return a value, use EvaluateInTx instead.
func (gts *GraphTraversalSource) ExecuteInTx(txWork func(*GraphTraversalSource) error) error {
	_, err := gts.EvaluateInTx(func(gtx *GraphTraversalSource) (interface{}, error) {
		return nil, txWork(gtx)
	})
	return err
}

// EvaluateInTx runs txWork inside a single, automatically managed transaction and
// returns the value produced by txWork along with any error.
//
// It is the value-returning counterpart to ExecuteInTx. The value is returned as
// interface{}, matching the rest of the driver's untyped result API (e.g.
// Traversal.Next returns a *Result whose concrete value is obtained via the
// Result.Get* accessors); the caller type-asserts the returned value as needed.
//
// It owns the full lifecycle: it obtains a transaction via gts.Tx, calls Begin to
// start it, passes the resulting transaction-bound GraphTraversalSource (gtx) to
// txWork, and then commits on success or rolls back on failure. The body must use
// only the gtx it receives; the non-transactional source is never in scope.
//
// This is single-shot: exactly one attempt is made (begin, run, commit/rollback)
// with no automatic retry.
//
// Error handling:
//   - If Begin fails, that error is returned (with a nil value) and txWork is
//     never invoked.
//   - If txWork returns a non-nil error, the transaction is rolled back and the
//     original body error is returned unchanged, along with the value txWork
//     returned.
//   - If the commit fails, a rollback is attempted for server-side hygiene and
//     the commit error is returned (it takes precedence over any rollback error).
//   - If a rollback attempted during cleanup itself fails, a warning is logged
//     but the primary (body or commit) error still propagates.
//   - If txWork panics, the transaction is rolled back and the panic is
//     re-raised (it is never swallowed).
func (gts *GraphTraversalSource) EvaluateInTx(
	txWork func(*GraphTraversalSource) (interface{}, error)) (interface{}, error) {

	var result interface{}
	tx := gts.Tx()
	gtx, err := tx.Begin()
	if err != nil {
		return result, err
	}

	// rollbackQuietly performs a best-effort cleanup rollback. It never propagates
	// a failure - a returned error is logged, and a panic from Rollback itself is
	// recovered and discarded - so it can never mask the primary (body, commit, or
	// panic) error. A failed rollback is not fatal anyway: the server rolls the
	// transaction back when it hits its transaction timeout.
	rollbackQuietly := func() {
		defer func() { _ = recover() }()
		if rbErr := tx.Rollback(); rbErr != nil {
			tx.logRollbackWarning(rbErr)
		}
	}

	// A panic in the body must roll back the transaction and then re-panic so the
	// original panic is never swallowed.
	defer func() {
		if r := recover(); r != nil {
			rollbackQuietly()
			panic(r)
		}
	}()

	result, err = txWork(gtx)
	if err != nil {
		// Body returned an error: roll back and surface the exact original error.
		rollbackQuietly()
		return result, err
	}

	if commitErr := tx.Commit(); commitErr != nil {
		// Commit failed: attempt a rollback for server-side hygiene, but the
		// commit error remains the primary error returned to the caller.
		rollbackQuietly()
		return result, commitErr
	}

	return result, nil
}
