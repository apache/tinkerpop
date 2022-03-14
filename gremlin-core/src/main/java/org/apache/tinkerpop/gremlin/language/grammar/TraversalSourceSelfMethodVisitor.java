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

import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A {@link GraphTraversalSource} self method visitor.
 */
public class TraversalSourceSelfMethodVisitor extends DefaultGremlinBaseVisitor<GraphTraversalSource> {

    private DefaultGremlinBaseVisitor<TraversalStrategy> traversalStrategyVisitor;
    private GraphTraversalSource source;
    private final GremlinAntlrToJava antlr;

    public TraversalSourceSelfMethodVisitor(final GraphTraversalSource source, final GremlinAntlrToJava antlr) {
        this.source = source;
        this.antlr = antlr;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversalSource visitTraversalSourceSelfMethod(final GremlinParser.TraversalSourceSelfMethodContext ctx) {
        return visitChildren(ctx);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversalSource visitTraversalSourceSelfMethod_withBulk(final GremlinParser.TraversalSourceSelfMethod_withBulkContext ctx)
    {
        final int childIndexOfParameterUseBulk = 2;

        final Boolean useBulk = (Boolean)(GenericLiteralVisitor.instance().visitBooleanLiteral(
                (GremlinParser.BooleanLiteralContext)(ctx.getChild(childIndexOfParameterUseBulk))));

        return source.withBulk(useBulk);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversalSource visitTraversalSourceSelfMethod_withPath(final GremlinParser.TraversalSourceSelfMethod_withPathContext ctx)
    {
        return source.withPath();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversalSource visitTraversalSourceSelfMethod_withSack(final GremlinParser.TraversalSourceSelfMethod_withSackContext ctx)
    {
        final int childIndexOfParameterInitialValue = 2;

        if (ctx.getChildCount() == 4) {
            return source.withSack(GenericLiteralVisitor.instance().visitGenericLiteral(
                    ParseTreeContextCastHelper.castChildToGenericLiteral(ctx, childIndexOfParameterInitialValue)));
        } else {
            final int childIndexOfParameterMergeOperator = 4;

            return source.withSack(GenericLiteralVisitor.instance().visitGenericLiteral(
                    ParseTreeContextCastHelper.castChildToGenericLiteral(ctx, childIndexOfParameterInitialValue)),
                    TraversalEnumParser.parseTraversalEnumFromContext(Operator.class, ctx.getChild(childIndexOfParameterMergeOperator)));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversalSource visitTraversalSourceSelfMethod_withSideEffect(final GremlinParser.TraversalSourceSelfMethod_withSideEffectContext ctx)
    {
        final int childIndexOfParameterKey = 2;
        final int childIndexOfParameterInitialValue = 4;

        final String argument1 = (String)(GenericLiteralVisitor.instance().visitStringBasedLiteral(
                (GremlinParser.StringBasedLiteralContext)(ctx.getChild(childIndexOfParameterKey))));
        final Object argument2 = GenericLiteralVisitor.instance().visitGenericLiteral(
                (GremlinParser.GenericLiteralContext)(ctx.getChild(childIndexOfParameterInitialValue)));

        return source.withSideEffect(argument1, argument2);
    }

    @Override
    public GraphTraversalSource visitTraversalSourceSelfMethod_withStrategies(final GremlinParser.TraversalSourceSelfMethod_withStrategiesContext ctx) {

        if (null == traversalStrategyVisitor)
            traversalStrategyVisitor = new TraversalStrategyVisitor((DefaultGremlinBaseVisitor) antlr.tvisitor);

        // with 4 children withStrategies() was called with a single TraversalStrategy, otherwise multiple were
        // specified.
        if (ctx.getChildCount() < 5) {
            return source.withStrategies(traversalStrategyVisitor.visitTraversalStrategy((GremlinParser.TraversalStrategyContext) ctx.getChild(2)));
        } else {
            final Object[] vargs = GenericLiteralVisitor.getTraversalStrategyList(
                    (GremlinParser.TraversalStrategyListContext) ctx.getChild(4), traversalStrategyVisitor);
            final List<TraversalStrategy> strats = new ArrayList<>(Arrays.asList(Arrays.copyOf(vargs, vargs.length, TraversalStrategy[].class)));
            strats.add(0, traversalStrategyVisitor.visitTraversalStrategy((GremlinParser.TraversalStrategyContext) ctx.getChild(2)));
            return source.withStrategies(strats.toArray(new TraversalStrategy[strats.size()]));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversalSource visitTraversalSourceSelfMethod_with(final GremlinParser.TraversalSourceSelfMethod_withContext ctx)
    {
        final int childIndexOfParameterKey = 2;

        if (ctx.getChildCount() == 4) {
            final String argument1 = (String)(GenericLiteralVisitor.instance().visitStringBasedLiteral(
                    (GremlinParser.StringBasedLiteralContext)(ctx.getChild(childIndexOfParameterKey))));
            return source.with(argument1);
        } else {
            final int childIndexOfParameterInitialValue = 4;

            final String argument1 = (String)(GenericLiteralVisitor.instance().visitStringBasedLiteral(
                    (GremlinParser.StringBasedLiteralContext)(ctx.getChild(childIndexOfParameterKey))));
            final Object argument2 = GenericLiteralVisitor.instance().visitGenericLiteral(
                    (GremlinParser.GenericLiteralContext)(ctx.getChild(childIndexOfParameterInitialValue)));
            return source.with(argument1, argument2);
        }
    }
}
