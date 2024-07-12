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

import org.antlr.v4.runtime.ParserRuleContext;
import org.apache.tinkerpop.gremlin.language.grammar.GremlinParser;
import org.apache.tinkerpop.gremlin.language.grammar.GremlinParser.GenericLiteralVarargsContext;
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
                sb.append("(byte)");
                sb.append(integerLiteral, 0, lastCharIndex);
                break;
            case 's':
                // parse S/s as short
                sb.append("(short)");
                sb.append(integerLiteral, 0, lastCharIndex);
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
                // parse N/n as BigDecimal which for groovy is default for floating point numerics
                if (!floatLiteral.contains(".")) {
                    // 1g is interpreted as BigInteger, so 'g' suffix can't be used here
                    sb.append("new BigDecimal(").append(floatLiteral, 0, lastCharIndex).append("L)");
                } else {
                    sb.append(floatLiteral, 0, lastCharIndex);
                }
                break;
            default:
                // everything else just goes as specified
                sb.append(floatLiteral);
                break;
        }
        return null;
    }

    @Override
    public Void visitInfLiteral(final GremlinParser.InfLiteralContext ctx) {
        if (ctx.SignedInfLiteral().getText().equals("-Infinity")) {
            sb.append("Double.NEGATIVE_INFINITY");
        } else {
            sb.append("Double.POSITIVE_INFINITY");
        }
        return null;
    }

    @Override
    public Void visitNullLiteral(final GremlinParser.NullLiteralContext ctx) {
        if (ctx.getParent() instanceof GremlinParser.GenericLiteralMapNullableArgumentContext) {
            sb.append("null as Map");
            return null;
        }

        sb.append(ctx.getText());
        return null;
    }

    @Override
    public Void visitGenericLiteralSet(GremlinParser.GenericLiteralSetContext ctx) {
        sb.append("[");
        for (int i = 0; i < ctx.genericLiteral().size(); i++) {
            final GremlinParser.GenericLiteralContext genericLiteralContext = ctx.genericLiteral(i);
            visit(genericLiteralContext);
            if (i < ctx.genericLiteral().size() - 1)
                sb.append(", ");
        }
        sb.append("] as Set");
        return null;
    }

    @Override
    public Void visitStringLiteral(final GremlinParser.StringLiteralContext ctx) {
        String literal = ctx.getText();
        literal = literal.replace("$", "\\$");
        sb.append(literal);
        return null;
    }

    @Override
    public Void visitTraversalStrategy(final GremlinParser.TraversalStrategyContext ctx) {
        if (ctx.getChildCount() == 1) {
            sb.append(ctx.getText());
        }
        else {
            // 'new' token is optional for gremlin-lang strategy construction, but required in gremlin-groovy
            if(!ctx.getChild(0).getText().equals("new")) {
                sb.append("new ");
            }
            visitChildren(ctx);
        }
        return null;
    }

    @Override
    public Void visitTraversalSourceSpawnMethod_inject(final GremlinParser.TraversalSourceSpawnMethod_injectContext ctx) {
        return handleInject(ctx);
    }

    @Override
    public Void visitTraversalMethod_inject(final GremlinParser.TraversalMethod_injectContext ctx) {
        return handleInject(ctx);
    }

    @Override
    public Void visitTraversalMethod_hasLabel_String_String(final GremlinParser.TraversalMethod_hasLabel_String_StringContext ctx) {
        // special handling for resolving ambiguous invocations of the hasLabel() step. coerces to the string form
        if (ctx.getChildCount() > 4 && ctx.getChild(2).getText().equals("null")) {
            final GremlinParser.StringLiteralVarargsContext varArgs = (GremlinParser.StringLiteralVarargsContext) ctx.getChild(4);
            sb.append(ctx.getChild(0).getText());
            sb.append("((String) null, ");

            for (int i = 0; i < varArgs.getChildCount(); i += 2) {
                if (varArgs.getChild(i).getText().equals("null")) {
                    sb.append("(String) null");
                } else {
                    visit(varArgs.getChild(i));
                }

                if (i < varArgs.getChildCount() - 1) {
                    sb.append(", ");
                }
            }

            sb.append(")");
            return null;
        }

        return visitChildren(ctx);
    }

    /**
    * Special handling for inject with second `null` argument like g.inject(1, null)
    * inject() ends up being ambiguous with groovy's jdk extension of inject(Object initialValue, Closure closure)
    */
    private Void handleInject(final ParserRuleContext ctx) {
        if (ctx.getChildCount() > 3 && ctx.getChild(2) instanceof GenericLiteralVarargsContext) {
            final GenericLiteralVarargsContext varArgs = (GenericLiteralVarargsContext) ctx.getChild(2);
            if (varArgs.getChildCount() > 2 && "null".equals(varArgs.getChild(2).getText())) {
                sb.append(ctx.getChild(0).getText());
                sb.append("(");
                for (int i = 0; i < varArgs.getChildCount(); i += 2) {
                    if (i == 2) {
                        sb.append("(Object) null");
                    } else {
                        visit(varArgs.getChild(i));
                    }

                    if (i < varArgs.getChildCount() - 1) {
                        sb.append(", ");
                    }
                }

                sb.append(")");
                return null;
            }
        }

        return visitChildren(ctx);
    }
}
