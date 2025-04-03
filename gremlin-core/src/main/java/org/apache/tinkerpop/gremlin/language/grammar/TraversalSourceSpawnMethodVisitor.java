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

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import java.util.Map;

/**
 * Use a {@link GraphTraversalSource} as the source and returns a {@link GraphTraversal} object.
 */
public class TraversalSourceSpawnMethodVisitor extends DefaultGremlinBaseVisitor<GraphTraversal> {

    protected GraphTraversalSource traversalSource;
    protected GraphTraversal graphTraversal;
    protected final DefaultGremlinBaseVisitor<Traversal> anonymousVisitor;

    protected final GremlinAntlrToJava antlr;

    public TraversalSourceSpawnMethodVisitor(final GraphTraversalSource traversalSource,
                                             final DefaultGremlinBaseVisitor<Traversal> anonymousVisitor,
                                             final GremlinAntlrToJava antlr) {
        this.traversalSource = traversalSource;
        this.anonymousVisitor = anonymousVisitor;
        this.antlr = antlr;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalSourceSpawnMethod(final GremlinParser.TraversalSourceSpawnMethodContext ctx) {
        return visitChildren(ctx);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalSourceSpawnMethod_addE(final GremlinParser.TraversalSourceSpawnMethod_addEContext ctx) {
        if (ctx.stringArgument() != null) {
            return this.traversalSource.addE(antlr.argumentVisitor.parseString(ctx.stringArgument()));
        } else if (ctx.nestedTraversal() != null) {
            return this.traversalSource.addE(anonymousVisitor.visitNestedTraversal(ctx.nestedTraversal()));
        } else {
            throw new IllegalArgumentException("addE with empty arguments is not valid.");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalSourceSpawnMethod_addV(final GremlinParser.TraversalSourceSpawnMethod_addVContext ctx) {
        if (ctx.stringArgument() != null) {
            return this.traversalSource.addV(antlr.argumentVisitor.parseString(ctx.stringArgument()));
        } else if (ctx.nestedTraversal() != null) {
            return this.traversalSource.addV(anonymousVisitor.visitNestedTraversal(ctx.nestedTraversal()));
        } else {
            return this.traversalSource.addV();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalSourceSpawnMethod_E(final GremlinParser.TraversalSourceSpawnMethod_EContext ctx) {
        return this.traversalSource.E(antlr.genericVisitor.parseObjectVarargs(ctx.genericLiteralVarargs()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalSourceSpawnMethod_V(final GremlinParser.TraversalSourceSpawnMethod_VContext ctx) {
        return this.traversalSource.V(antlr.genericVisitor.parseObjectVarargs(ctx.genericLiteralVarargs()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalSourceSpawnMethod_inject(final GremlinParser.TraversalSourceSpawnMethod_injectContext ctx) {
        return this.traversalSource.inject(antlr.genericVisitor.parseObjectVarargs(ctx.genericLiteralVarargs()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalSourceSpawnMethod_io(final GremlinParser.TraversalSourceSpawnMethod_ioContext ctx) {
        if (ctx.getChildCount() > 2) {
            this.graphTraversal = this.traversalSource.io(antlr.genericVisitor.parseString(ctx.stringLiteral()));
        }
        return graphTraversal;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalSourceSpawnMethod_mergeV_Map(final GremlinParser.TraversalSourceSpawnMethod_mergeV_MapContext ctx) {
        return this.traversalSource.mergeV(antlr.argumentVisitor.parseMap(ctx.genericLiteralMapNullableArgument()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalSourceSpawnMethod_mergeV_Traversal(final GremlinParser.TraversalSourceSpawnMethod_mergeV_TraversalContext ctx) {
        return this.traversalSource.mergeV(anonymousVisitor.visitNestedTraversal(ctx.nestedTraversal()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalSourceSpawnMethod_mergeE_Traversal(final GremlinParser.TraversalSourceSpawnMethod_mergeE_TraversalContext ctx) {
        return this.traversalSource.mergeE(anonymousVisitor.visitNestedTraversal(ctx.nestedTraversal()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalSourceSpawnMethod_mergeE_Map(final GremlinParser.TraversalSourceSpawnMethod_mergeE_MapContext ctx) {
        return this.traversalSource.mergeE(antlr.argumentVisitor.parseMap(ctx.genericLiteralMapNullableArgument()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalSourceSpawnMethod_call_empty(final GremlinParser.TraversalSourceSpawnMethod_call_emptyContext ctx) {
        return this.traversalSource.call();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalSourceSpawnMethod_call_string(final GremlinParser.TraversalSourceSpawnMethod_call_stringContext ctx) {
        return this.traversalSource.call(antlr.argumentVisitor.parseString(ctx.stringArgument()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalSourceSpawnMethod_call_string_map(final GremlinParser.TraversalSourceSpawnMethod_call_string_mapContext ctx) {
        return this.traversalSource.call(antlr.argumentVisitor.parseString(ctx.stringArgument()),
                antlr.argumentVisitor.parseMap(ctx.genericLiteralMapArgument()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalSourceSpawnMethod_call_string_traversal(final GremlinParser.TraversalSourceSpawnMethod_call_string_traversalContext ctx) {
        return this.traversalSource.call(antlr.argumentVisitor.parseString(ctx.stringArgument()),
                anonymousVisitor.visitNestedTraversal(ctx.nestedTraversal()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalSourceSpawnMethod_call_string_map_traversal(final GremlinParser.TraversalSourceSpawnMethod_call_string_map_traversalContext ctx) {
        return this.traversalSource.call(antlr.argumentVisitor.parseString(ctx.stringArgument()),
                antlr.argumentVisitor.parseMap(ctx.genericLiteralMapArgument()),
                anonymousVisitor.visitNestedTraversal(ctx.nestedTraversal()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalSourceSpawnMethod_union(final GremlinParser.TraversalSourceSpawnMethod_unionContext ctx) {
        return this.traversalSource.union(antlr.tListVisitor.visitNestedTraversalList(ctx.nestedTraversalList()));
    }
}
