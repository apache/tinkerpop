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

/**
 * This class implements Gremlin grammar's nested-traversal-list methods that returns a {@link Traversal} {@code []}
 * to the callers.
 */
public class NestedTraversalSourceListVisitor extends GremlinBaseVisitor<Traversal[]> {

    protected final GremlinAntlrToJava context;

    public NestedTraversalSourceListVisitor(final GremlinAntlrToJava context) {
        this.context = context;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Traversal[] visitNestedTraversalList(final GremlinParser.NestedTraversalListContext ctx) {
        if (ctx.children == null) {
            return new Traversal[0];
        }

        return this.visitChildren(ctx);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Traversal[] visitNestedTraversalExpr(final GremlinParser.NestedTraversalExprContext ctx) {
        final int childCount = ctx.getChildCount();

        // handle arbitrary number of traversals that are separated by comma
        final Traversal[] results = new Traversal[(childCount + 1) / 2];
        int childIndex = 0;
        while (childIndex < ctx.getChildCount()) {
            results[childIndex / 2] = context.tvisitor.visitNestedTraversal(
                    (GremlinParser.NestedTraversalContext)ctx.getChild(childIndex));
            // skip comma child
            childIndex += 2;
        }

        return results;
    }
}
