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
package org.apache.tinkerpop.gremlin.language.translator;

import org.apache.tinkerpop.gremlin.language.grammar.GremlinParser;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex;

/**
 * Converts a Gremlin traversal string into a Groovy source code representation of that traversal with an aim at
 * sacrificing some formatting for the ability to compile correctly. The translations may require use of TinkerPop's
 * sugar syntax and therefore requires use of the {@code GremlinLoader} in the gremlin-groovy module unless you are
 * specifically certain that your translations will not result in the use of that syntax. If in doubt, prefer the
 * {@link JavaTranslateVisitor} instead.
 * <ul>
 *     <li>Normalize numeric suffixes to lower case</li>
 *     <li>If floats are not suffixed they will translate as BigDecimal</li>
 *     <li>Makes anonymous traversals explicit with double underscore</li>
 *     <li>Makes enums explicit with their proper name</li>
 * </ul>
 */
public class GroovyTranslateVisitor extends TranslateVisitor {
    private static final String vertexClassName = ReferenceVertex.class.getSimpleName();
    public GroovyTranslateVisitor() {
        this("g");
    }

    public GroovyTranslateVisitor(final String graphTraversalSourceName) {
        super(graphTraversalSourceName);
    }

    @Override
    public Void visitStructureVertex(final GremlinParser.StructureVertexContext ctx) {
        sb.append("new ");
        sb.append(vertexClassName);
        sb.append("(");
        visit(ctx.getChild(3)); // id
        sb.append(", ");
        visit(ctx.getChild(5)); // label
        sb.append(")");
        return null;
    }

    @Override
    public Void visitIntegerLiteral(final GremlinParser.IntegerLiteralContext ctx) {
        final String integerLiteral = ctx.getText().toLowerCase();

        // check suffix
        final int lastCharIndex = integerLiteral.length() - 1;
        final char lastCharacter = integerLiteral.charAt(lastCharIndex);
        switch (lastCharacter) {
            case 'b':
                // parse B/b as byte
                sb.append("new Byte(");
                sb.append(integerLiteral, 0, lastCharIndex);
                sb.append(")");
                break;
            case 's':
                // parse S/s as short
                sb.append("new Short(");
                sb.append(integerLiteral, 0, lastCharIndex);
                sb.append(")");
                break;
            case 'n':
                // parse N/n as BigInteger which for groovy is "g" shorthand
                sb.append(integerLiteral, 0, lastCharIndex).append("g");
                break;
            default:
                // covers I/i and L/l as Integer and Long respectively
                // everything else just goes as specified
                sb.append(integerLiteral);
                break;
        }
        return null;
    }

    @Override
    public Void visitFloatLiteral(final GremlinParser.FloatLiteralContext ctx) {
        final String floatLiteral = ctx.getText().toLowerCase();

        // check suffix
        final int lastCharIndex = floatLiteral.length() - 1;
        final char lastCharacter = floatLiteral.charAt(lastCharIndex);
        switch (lastCharacter) {
            case 'f':
            case 'd':
                // parse F/f as Float and D/d suffix as Double
                sb.append(floatLiteral, 0, lastCharIndex).append(lastCharacter);
                break;
            case 'm':
                // parse N/n as BigDecimal which for groovy is "g" shorthand
                sb.append(floatLiteral, 0, lastCharIndex).append("g");
                break;
            default:
                // everything else just goes as specified
                sb.append(floatLiteral);
                break;
        }
        return null;
    }
}
