/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.language.grammar;

import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.tinkerpop.gremlin.process.traversal.GremlinLang;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;

import java.util.function.Supplier;

/**
 * This is the entry point for converting the Gremlin Antlr grammar into Java. It is bound to a {@link Graph} instance
 * as that instance may spawn specific {@link Traversal} or {@link TraversalSource} types. A new instance should be
 * created for each parse execution.
 */
public class GremlinAntlrToJava extends DefaultGremlinBaseVisitor<Object> {

    /**
     * The {@link Graph} instance to which this instance is bound.
     */
    final Graph graph;

    /**
     * The "g" from which to start the traversal.
     */
    final GraphTraversalSource g;

    /**
     * A {@link DefaultGremlinBaseVisitor} that processes {@link TraversalSource} methods.
     */
    final DefaultGremlinBaseVisitor<GraphTraversalSource> gvisitor;

    /**
     * A {@link DefaultGremlinBaseVisitor} that processes {@link Traversal} methods and is meant to construct traversals
     * anonymously.
     */
    final DefaultGremlinBaseVisitor<GraphTraversal> tvisitor;

    /**
     * A {@link DefaultGremlinBaseVisitor} that is meant to construct a list of traversals anonymously.
     */
    final DefaultGremlinBaseVisitor<Traversal[]> tListVisitor;

    /**
     * Handles transactions.
     */
    final DefaultGremlinBaseVisitor<Void> txVisitor;

    /**
     * Creates a {@link GraphTraversal} implementation that is meant to be anonymous. This provides a way to change the
     * type of implementation that will be used as anonymous traversals. By default, it uses {@link __} which generates
     * a {@link DefaultGraphTraversal}
     */
    final Supplier<GraphTraversal<?,?>> createAnonymous;

    /**
     * Parses arguments, which may or may not be variables.
     */
    final ArgumentVisitor argumentVisitor;

    /**
     * Parses literals.
     */
    final GenericLiteralVisitor genericVisitor;

    /**
     * Parses {@link TraversalStrategy} instances.
     */
    final TraversalStrategyVisitor traversalStrategyVisitor;

    /**
     * Parses {@link P} instances.
     */
    final TraversalPredicateVisitor traversalPredicateVisitor;

    /**
     * Parses structure instances like {@link Vertex}.
     */
    final StructureElementVisitor structureVisitor;

    /**
     * Constructs a new instance and is bound to an {@link EmptyGraph}. This form of construction is helpful for
     * generating {@link GremlinLang} or for various forms of testing. {@link Traversal} instances constructed from this
     * form will not be capable of iterating. Assumes that "g" is the name of the {@link GraphTraversalSource}.
     */
    public GremlinAntlrToJava() {
        this(EmptyGraph.instance());
    }

    /**
     * Constructs a new instance that is bound to the specified {@link Graph} instance. Assumes that "g" is the name
     * of the {@link GraphTraversalSource}.
     */
    public GremlinAntlrToJava(final Graph graph) {
        this(graph, __::start);
    }

    /**
     * Constructs a new instance that is bound to the specified {@link Graph} instance. Assumes that "g" is the name
     * of the {@link GraphTraversalSource}.
     */
    public GremlinAntlrToJava(final Graph graph, final VariableResolver variableResolver) {
        this(GraphTraversalSourceVisitor.TRAVERSAL_ROOT, graph, __::start, null, variableResolver);
    }

    /**
     * Constructs a new instance that is bound to the specified {@link GraphTraversalSource} and thus spawns the
     * {@link Traversal} from this "g" rather than from a fresh one constructed from the {@link Graph} instance.
     */
    public GremlinAntlrToJava(final GraphTraversalSource g) {
        this(g, __::start);
    }

    /**
     * Constructs a new instance that is bound to the specified {@link GraphTraversalSource} and thus spawns the
     * {@link Traversal} from this "g" rather than from a fresh one constructed from the {@link Graph} instance.
     * Allows for specification of a {@link VariableResolver} to allow parameters to be resolved.
     */
    public GremlinAntlrToJava(final GraphTraversalSource g, final VariableResolver variableResolver) {
        this(GraphTraversalSourceVisitor.TRAVERSAL_ROOT, g.getGraph(), __::start, g, variableResolver);
    }

    /**
     * Constructs a new instance that is bound to the specified {@link Graph} instance with an override to using
     * {@link __} for constructing anonymous {@link Traversal} instances. Assumes that "g" is the name of the
     * {@link GraphTraversalSource}.
     */
    protected GremlinAntlrToJava(final Graph graph, final Supplier<GraphTraversal<?,?>> createAnonymous) {
        this(GraphTraversalSourceVisitor.TRAVERSAL_ROOT, graph, createAnonymous);
    }

    /**
     * Constructs a new instance that is bound to the specified {@link GraphTraversalSource} and thus spawns the
     * {@link Traversal} from this "g" rather than from a fresh one constructed from the {@link Graph} instance.
     */
    protected GremlinAntlrToJava(final GraphTraversalSource g, final Supplier<GraphTraversal<?,?>> createAnonymous) {
        this(GraphTraversalSourceVisitor.TRAVERSAL_ROOT, g.getGraph(), createAnonymous, g, VariableResolver.NoVariableResolver.instance());
    }

    /**
     * Constructs a new instance that is bound to the specified {@link Graph} instance with an override to using
     * {@link __} for constructing anonymous {@link Traversal} instances.
     *
     * @param traversalSourceName The name of the traversal source which will be "g" if not specified.
     */
    protected GremlinAntlrToJava(final String traversalSourceName, final Graph graph,
                                 final Supplier<GraphTraversal<?,?>> createAnonymous) {
        this(traversalSourceName, graph, createAnonymous, null, VariableResolver.NoVariableResolver.instance());
    }

    /**
     * Constructs a new instance that is bound to the specified {@link Graph} instance with an override to using
     * {@link __} for constructing anonymous {@link Traversal} instances. If the {@link GraphTraversalSource} is
     * provided then the {@link Traversal} will spawn from it as opposed to a fresh one from the {@link Graph}
     * instance. When a {@link VariableResolver} is supplied it will attempt to resolve parameters in the Gremlin to
     * objects.
     *
     * @param traversalSourceName The name of the traversal source which will be "g" if not specified.
     */
    protected GremlinAntlrToJava(final String traversalSourceName, final Graph graph,
                                 final Supplier<GraphTraversal<?,?>> createAnonymous,
                                 final GraphTraversalSource g, final VariableResolver variableResolver) {
        this.g = g;
        this.graph = graph;
        this.gvisitor = new GraphTraversalSourceVisitor(
                null == traversalSourceName ? GraphTraversalSourceVisitor.TRAVERSAL_ROOT : traversalSourceName,this);
        this.tvisitor = new TraversalRootVisitor(this);
        this.tListVisitor = new NestedTraversalSourceListVisitor(this);
        this.createAnonymous = createAnonymous;
        this.txVisitor = new TraversalSourceTxVisitor(g, this);
        this.traversalPredicateVisitor = new TraversalPredicateVisitor(this);
        this.traversalStrategyVisitor = new TraversalStrategyVisitor(this);
        this.structureVisitor = new StructureElementVisitor(this);
        this.genericVisitor = new GenericLiteralVisitor(this);
        this.argumentVisitor = new ArgumentVisitor(variableResolver, this);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object visitQuery(final GremlinParser.QueryContext ctx) {
        final int childCount = ctx.getChildCount();
        if (childCount <= 3) {
            final ParseTree firstChild = ctx.getChild(0);
            if (firstChild instanceof GremlinParser.TraversalSourceContext) {
                if (childCount == 1) {
                    // handle traversalSource
                    return gvisitor.visitTraversalSource((GremlinParser.TraversalSourceContext) firstChild);
                } else {
                    // handle traversalSource DOT transactionPart
                    // third child is the tx info
                    return txVisitor.visitTransactionPart((GremlinParser.TransactionPartContext) ctx.getChild(2));
                }
            } else if (firstChild instanceof GremlinParser.EmptyQueryContext) {
                // handle empty query
                return "";
            } else {
                if (childCount == 1) {
                    // handle rootTraversal
                    return tvisitor.visitRootTraversal(
                            (GremlinParser.RootTraversalContext) firstChild);
                } else {
                    // handle rootTraversal DOT traversalTerminalMethod
                    return new TraversalTerminalMethodVisitor(tvisitor.visitRootTraversal(
                            (GremlinParser.RootTraversalContext) firstChild)).visitTraversalTerminalMethod(
                            (GremlinParser.TraversalTerminalMethodContext)ctx.getChild(2));
                }
            }
        } else {
            // handle toString
            return String.valueOf(visitChildren(ctx));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object visitQueryList(final GremlinParser.QueryListContext ctx) {
        return visitChildren(ctx);
    }

    /**
     * Override the aggregate result behavior. If the next result is {@code null}, return the current result. This is
     * used to handle child EOF, which is the last child of the {@code QueryList} context. If the next Result is not
     * {@code null}, return the next result. This is used to handle multiple queries, and return only the last query
     * result logic.
     */
    @Override
    protected Object aggregateResult(final Object result, final Object nextResult) {
        if (nextResult == null) {
            return result;
        } else {
            return nextResult;
        }
    }
}
