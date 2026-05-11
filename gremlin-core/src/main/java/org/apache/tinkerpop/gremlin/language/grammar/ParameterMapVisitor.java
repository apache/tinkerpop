/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.language.grammar;

/**
 * A visitor for parsing parameter map strings that prevents traversal injection.
 * <p>
 * Extends {@link GenericLiteralVisitor} and overrides traversal-related visit methods
 * to throw, ensuring that no traversal can be constructed or executed during the
 * parameter map parsing walk. This is critical for security because
 * {@code visitTerminatedTraversal} in the base class would execute the traversal
 * immediately via {@link TraversalTerminalMethodVisitor}.
 */
public class ParameterMapVisitor extends GenericLiteralVisitor {

    private static final int DEFAULT_MAX_NESTING_DEPTH = 32;

    private final int maxNestingDepth;
    private int currentNestingDepth = 0;

    public ParameterMapVisitor(final GremlinAntlrToJava antlr) {
        this(antlr, DEFAULT_MAX_NESTING_DEPTH);
    }

    public ParameterMapVisitor(final GremlinAntlrToJava antlr, final int maxNestingDepth) {
        super(antlr);
        this.maxNestingDepth = maxNestingDepth;
    }

    /**
     * Overridden to prevent nested traversal construction in parameter maps.
     */
    @Override
    public Object visitNestedTraversal(final GremlinParser.NestedTraversalContext ctx) {
        throw new GremlinParserException("Traversals are not allowed in parameter maps");
    }

    /**
     * Overridden to prevent terminated traversal execution in parameter maps.
     * This is the critical override because the base class would execute the traversal
     * immediately via {@link TraversalTerminalMethodVisitor}.
     */
    @Override
    public Object visitTerminatedTraversal(final GremlinParser.TerminatedTraversalContext ctx) {
        throw new GremlinParserException("Traversals are not allowed in parameter maps");
    }

    @Override
    public Object visitGenericMapLiteral(final GremlinParser.GenericMapLiteralContext ctx) {
        currentNestingDepth++;
        if (currentNestingDepth > maxNestingDepth) {
            throw new GremlinParserException("Parameter map nesting depth exceeds maximum of " + maxNestingDepth);
        }
        try {
            return super.visitGenericMapLiteral(ctx);
        } finally {
            currentNestingDepth--;
        }
    }

    @Override
    public Object visitGenericCollectionLiteral(final GremlinParser.GenericCollectionLiteralContext ctx) {
        currentNestingDepth++;
        if (currentNestingDepth > maxNestingDepth) {
            throw new GremlinParserException("Parameter map nesting depth exceeds maximum of " + maxNestingDepth);
        }
        try {
            return super.visitGenericCollectionLiteral(ctx);
        } finally {
            currentNestingDepth--;
        }
    }

    @Override
    public Object visitGenericSetLiteral(final GremlinParser.GenericSetLiteralContext ctx) {
        currentNestingDepth++;
        if (currentNestingDepth > maxNestingDepth) {
            throw new GremlinParserException("Parameter map nesting depth exceeds maximum of " + maxNestingDepth);
        }
        try {
            return super.visitGenericSetLiteral(ctx);
        } finally {
            currentNestingDepth--;
        }
    }
}
