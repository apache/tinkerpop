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
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.T;

import java.util.function.Function;

/**
 * Traversal enum parser parses all the enums like (e.g. {@link Scope} in graph traversal.
 */
public class TraversalEnumParser {

    /**
     * Parse enum text from a parse tree context into a enum object
     * @param enumType : class of enum
     * @param context : parse tree context
     * @return enum object
     */
    public static <E extends Enum<E>, C extends ParseTree> E parseTraversalEnumFromContext(final Class<E> enumType, final C context) {
        final String text = context.getText();
        final String className = enumType.getSimpleName();

        // Support qualified class names like (ex: T.id or Scope.local)
        if (text.startsWith(className)) {
            final String strippedText = text.substring(className.length() + 1);
            return E.valueOf(enumType, strippedText);
        } else {
            return E.valueOf(enumType, text);
        }
    }

    /**
     * Parsing of {@link Direction} requires some special handling because of aliases (from/to).
     */
    public static Direction parseTraversalDirectionFromContext(final GremlinParser.TraversalDirectionContext context) {
        String text = context.getText();
        if (text.startsWith(Direction.class.getSimpleName()))
            text = text.substring(Direction.class.getSimpleName().length() + 1);
        return Direction.directionValueOf(text);
    }

    public static Function parseTraversalFunctionFromContext(final GremlinParser.TraversalFunctionContext context) {
        if (context.traversalToken() != null) {
            return TraversalEnumParser.parseTraversalEnumFromContext(T.class, context.traversalToken());
        } else if (context.traversalColumn() != null)
            return TraversalEnumParser.parseTraversalEnumFromContext(Column.class, context.traversalColumn());
        else {
            throw new GremlinParserException("Unrecognized enum for traversal function");
        }
    }
}
