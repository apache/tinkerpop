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

/**
 * Use a {@link GraphTraversalSource} as the source and returns a {@link GraphTraversal} object.
 */
public class TraversalSourceSpawnMethodVisitor extends GremlinBaseVisitor<GraphTraversal> {

    protected GraphTraversalSource traversalSource;
    protected GraphTraversal graphTraversal;
    protected final GremlinBaseVisitor<Traversal> anonymousVisitor;

    public TraversalSourceSpawnMethodVisitor(final GraphTraversalSource traversalSource,
                                             final GremlinBaseVisitor<Traversal> anonymousVisitor) {
        this.traversalSource = traversalSource;
        this.anonymousVisitor = anonymousVisitor;
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
        if (ctx.stringLiteral() != null) {
            return this.traversalSource.addE(GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral()));
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
        if (ctx.stringLiteral() != null) {
            return this.traversalSource.addV(GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral()));
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
        if (ctx.genericLiteralList().getChildCount() > 0) {
            return this.traversalSource.E(GenericLiteralVisitor.getGenericLiteralList(ctx.genericLiteralList()));
        } else {
            return this.traversalSource.E();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalSourceSpawnMethod_V(final GremlinParser.TraversalSourceSpawnMethod_VContext ctx) {
        if (ctx.genericLiteralList().getChildCount() > 0) {
            return this.traversalSource.V(GenericLiteralVisitor.getGenericLiteralList(ctx.genericLiteralList()));
        } else {
            return this.traversalSource.V();
        }
    }
    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalSourceSpawnMethod_inject(final GremlinParser.TraversalSourceSpawnMethod_injectContext ctx) {
        return this.traversalSource.inject(GenericLiteralVisitor.getGenericLiteralList(ctx.genericLiteralList()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalSourceSpawnMethod_io(final GremlinParser.TraversalSourceSpawnMethod_ioContext ctx) {
        if (ctx.getChildCount() > 2) {
            this.graphTraversal = this.traversalSource.io(ctx.getChild(2).getText());
        }
        return graphTraversal;
    }
}
