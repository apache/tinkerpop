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

import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.T;

import java.util.function.Function;

/**
 * Traversal Function parser parses Function enums.
 */
public class TraversalFunctionVisitor extends GremlinBaseVisitor<Function> {

    private TraversalFunctionVisitor() {}

    private static TraversalFunctionVisitor instance;

    public static TraversalFunctionVisitor instance() {
        if (instance == null) {
            instance = new TraversalFunctionVisitor();
        }
        return instance;
    }

    /**
     * @deprecated As of release 3.5.2, replaced by {@link #instance()}.
     */
    @Deprecated
    public static TraversalFunctionVisitor getInstance() {
        return instance();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Function visitTraversalFunction(final GremlinParser.TraversalFunctionContext ctx) {
        return this.visitChildren(ctx);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Function visitTraversalToken(final GremlinParser.TraversalTokenContext ctx) {
        return TraversalEnumParser.parseTraversalEnumFromContext(T.class, ctx);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Function visitTraversalColumn(final GremlinParser.TraversalColumnContext ctx) {
        return TraversalEnumParser.parseTraversalEnumFromContext(Column.class, ctx);
    }
}
