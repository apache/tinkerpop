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
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;

/**
 * Traversal terminal method visitor
 */
public class TraversalTerminalMethodVisitor extends DefaultGremlinBaseVisitor<Object> {

    protected final Traversal<?,?> traversal;

    public TraversalTerminalMethodVisitor(final Traversal<?,?> traversal) {
        this.traversal = traversal;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object visitTraversalTerminalMethod(final GremlinParser.TraversalTerminalMethodContext ctx) {
        return visitChildren(ctx);
    }

    /**
     * {@inheritDoc}
     *
     * Traversal terminal method explain step
     */
    @Override
    public Object visitTraversalTerminalMethod_explain(final GremlinParser.TraversalTerminalMethod_explainContext ctx) {
        return traversal.explain();
    }

    /**
     * {@inheritDoc}
     *
     * Traversal terminal method has next step
     */
    @Override
    public Object visitTraversalTerminalMethod_hasNext(final GremlinParser.TraversalTerminalMethod_hasNextContext ctx) {
        try {
            return traversal.hasNext();
        } finally {
            CloseableIterator.closeIterator(traversal);
        }
    }

    /**
     * {@inheritDoc}
     *
     * Traversal terminal method iterate step
     */
    @Override
    public Object visitTraversalTerminalMethod_iterate(final GremlinParser.TraversalTerminalMethod_iterateContext ctx) {
        return traversal.iterate();
    }

    /**
     * {@inheritDoc}
     *
     * Traversal terminal method try next step
     */
    @Override
    public Object visitTraversalTerminalMethod_tryNext(final GremlinParser.TraversalTerminalMethod_tryNextContext ctx) {
        try {
            return traversal.tryNext();
        } finally {
            CloseableIterator.closeIterator(traversal);
        }
    }

    /**
     * {@inheritDoc}
     *
     * Traversal terminal method next step
     */
    @Override
    public Object visitTraversalTerminalMethod_next(final GremlinParser.TraversalTerminalMethod_nextContext ctx) {
        try {
            if (ctx.getChildCount() == 3) {
                return traversal.next();
            } else {
                // the 3rd child is integer value
                final int childIndexOfParamaterAmount = 2;
                return traversal.next(Integer.decode(ctx.getChild(childIndexOfParamaterAmount).getText()));
            }
        } finally {
            CloseableIterator.closeIterator(traversal);
        }
    }

    /**
     * {@inheritDoc}
     *
     * Traversal terminal method to list step
     */
    @Override
    public Object visitTraversalTerminalMethod_toList(final GremlinParser.TraversalTerminalMethod_toListContext ctx) {
        return traversal.toList();
    }

    /**
     * {@inheritDoc}
     *
     * Traversal terminal method to set step
     */
    @Override
    public Object visitTraversalTerminalMethod_toSet(final GremlinParser.TraversalTerminalMethod_toSetContext ctx) {
        return traversal.toSet();
    }

    /**
     * {@inheritDoc}
     *
     * Traversal terminal method to bulk set step
     */
    @Override
    public Object visitTraversalTerminalMethod_toBulkSet(final GremlinParser.TraversalTerminalMethod_toBulkSetContext ctx) {
        return traversal.toBulkSet();
    }
}
