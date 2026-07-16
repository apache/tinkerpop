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
package org.apache.tinkerpop.gremlin.language.translator;

import org.apache.tinkerpop.gremlin.language.grammar.GremlinParser;

/**
 * Translates Gremlin traversals to Dart source.
 */
public class DartTranslateVisitor extends AbstractTranslateVisitor {

    public DartTranslateVisitor() {
        super("g");
    }

    public DartTranslateVisitor(final String graphTraversalSourceName) {
        super(graphTraversalSourceName);
    }

    @Override
    public Void visitIntegerLiteral(final GremlinParser.IntegerLiteralContext ctx) {
        final String literal = ctx.getText().toLowerCase();
        final int lastCharIndex = literal.length() - 1;
        final char suffix = literal.charAt(lastCharIndex);
        final String value = Character.isAlphabetic(suffix) ? literal.substring(0, lastCharIndex) : literal;

        switch (suffix) {
            case 'b':
                sb.append("GByte(");
                sb.append(value).append(")");
                return null;
            case 's':
                sb.append("GShort(");
                sb.append(value).append(")");
                return null;
            case 'l':
                sb.append("GLong(");
                sb.append(value).append(")");
                return null;
            case 'n':
                sb.append("BigInt.parse('").append(value).append("')");
                return null;
            case 'i':
            default:
                sb.append("GInt(");
                sb.append(value).append(")");
                return null;
        }
        return null;
    }

    @Override
    public Void visitFloatLiteral(final GremlinParser.FloatLiteralContext ctx) {
        if (ctx.infLiteral() != null) return visit(ctx.infLiteral());
        if (ctx.nanLiteral() != null) return visit(ctx.nanLiteral());

        final String literal = ctx.getText().toLowerCase();
        final int lastCharIndex = literal.length() - 1;
        final char suffix = literal.charAt(lastCharIndex);
        final String value = Character.isAlphabetic(suffix) ? literal.substring(0, lastCharIndex) : literal;

        if (suffix == 'f') {
            sb.append("GFloat(");
        } else {
            sb.append("GDouble(");
        }
        sb.append(value).append(")");
        return null;
    }

    @Override
    public Void visitGenericCollectionLiteral(final GremlinParser.GenericCollectionLiteralContext ctx) {
        sb.append("[");
        for (int i = 0; i < ctx.genericLiteral().size(); i++) {
            visit(ctx.genericLiteral(i));
            if (i < ctx.genericLiteral().size() - 1) appendArgumentSeparator();
        }
        sb.append("]");
        return null;
    }

    @Override
    public Void visitGenericMapLiteral(final GremlinParser.GenericMapLiteralContext ctx) {
        sb.append("{");
        for (int i = 0; i < ctx.mapEntry().size(); i++) {
            visit(ctx.mapEntry(i));
            if (i < ctx.mapEntry().size() - 1) appendArgumentSeparator();
        }
        sb.append("}");
        return null;
    }

    @Override
    public Void visitMapEntry(final GremlinParser.MapEntryContext ctx) {
        visit(ctx.mapKey());
        sb.append(": ");
        visit(ctx.genericLiteral());
        return null;
    }

    @Override
    public Void visitNullLiteral(final GremlinParser.NullLiteralContext ctx) {
        sb.append("null");
        return null;
    }

    @Override
    protected void handleStringLiteralText(final String text) {
        sb.append("'").append(text.replace("'", "\\'")).append("'");
    }
}
