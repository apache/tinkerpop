/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.tinkerpop.gremlin.language.translator;

import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.tinkerpop.gremlin.language.grammar.GremlinParser;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.util.DatetimeHelper;

import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Converts a Gremlin traversal string into a Javascript source code representation of that traversal with an aim at
 * sacrificing some formatting for the ability to compile correctly.
 * <ul>
 *     <li>Range syntax has no direct support</li>
 *     <li>Normalizes whitespace</li>
 *     <li>Makes anonymous traversals explicit with double underscore</li>
 *     <li>Makes enums explicit with their proper name</li>
 * </ul>
 */
public class JavascriptTranslateVisitor extends AbstractTranslateVisitor {
    public JavascriptTranslateVisitor() {
        this("g");
    }

    public JavascriptTranslateVisitor(String graphTraversalSourceName) {
        super(graphTraversalSourceName);
    }

    @Override
    public Void visitTraversalStrategy(final GremlinParser.TraversalStrategyContext ctx) {
        sb.append("new ");

        if (ctx.getChildCount() == 1)
            sb.append(ctx.getText()).append("()");
        else {
            sb.append(ctx.getChild(0).getText().equals("new") ? ctx.getChild(1).getText() : ctx.getChild(0).getText()).append("({");

            final List<ParseTree> configs = ctx.children.stream().
                    filter(c -> c instanceof GremlinParser.ConfigurationContext).collect(Collectors.toList());

            // the rest are the arguments to the strategy
            for (int ix = 0; ix < configs.size(); ix++) {
                visit(configs.get(ix));
                if (ix < configs.size() - 1)
                    sb.append(", ");
            }

            sb.append("})");
        }

        return null;
    }

    @Override
    public Void visitConfiguration(final GremlinParser.ConfigurationContext ctx) {
        // form of three tokens of key:value to become key=value
        sb.append(SymbolHelper.toJavascript(ctx.getChild(0).getText()));
        sb.append(": ");
        visit(ctx.getChild(2));
        return null;
    }

    @Override
    public Void visitTraversalGType(GremlinParser.TraversalGTypeContext ctx) {
        final String[] split = ctx.getText().split("\\.");
        sb.append(processGremlinSymbol(split[0])).append(".");
        sb.append(processGremlinSymbol(split[1].toLowerCase()));
        return null;
    }

    @Override
    public Void visitGenericMapLiteral(final GremlinParser.GenericMapLiteralContext ctx) {
        sb.append("new Map([");
        for (int i = 0; i < ctx.mapEntry().size(); i++) {
            final GremlinParser.MapEntryContext mapEntryContext = ctx.mapEntry(i);
            visit(mapEntryContext);
            if (i < ctx.mapEntry().size() - 1)
                sb.append(", ");
        }
        sb.append("])");
        return null;
    }

    @Override
    public Void visitMapEntry(final GremlinParser.MapEntryContext ctx) {
        // if it is a terminal node that isn't a starting form like "(T.id)" then it has to be processed as a string
        // for Javascript but otherwise it can just be handled as a generic literal
        final boolean isKeyWrappedInParens = ctx.getChild(0).getText().equals("(");
        sb.append("[");
        visit(ctx.mapKey());
        sb.append(", ");
        visit(ctx.genericLiteral()); // value
        sb.append("]");
        return null;
    }

    @Override
    public Void visitMapKey(final GremlinParser.MapKeyContext ctx) {
        final int keyIndex = ctx.LPAREN() != null && ctx.RPAREN() != null ? 1 : 0;
        visit(ctx.getChild(keyIndex));
        return null;
    }

    @Override
    public Void visitDateLiteral(final GremlinParser.DateLiteralContext ctx) {
        // child at 2 is the date argument to datetime() and comes enclosed in quotes
        final String dtString = ctx.getChild(2).getText();
        // for consistency, use the way OffsetDateTime formats the date strings
        final OffsetDateTime dt = DatetimeHelper.parse(removeFirstAndLastCharacters(dtString));
        sb.append("new Date('");
        sb.append(dt);
        sb.append("')");
        return null;
    }

    @Override
    public Void visitNanLiteral(final GremlinParser.NanLiteralContext ctx) {
        sb.append("Number.NaN");
        return null;
    }

    @Override
    public Void visitInfLiteral(final GremlinParser.InfLiteralContext ctx) {
        if (ctx.SignedInfLiteral() != null && ctx.SignedInfLiteral().getText().equals("-Infinity"))
            sb.append("Number.NEGATIVE_INFINITY");
        else
            sb.append("Number.POSITIVE_INFINITY");
        return null;
    }

    @Override
    public Void visitIntegerLiteral(final GremlinParser.IntegerLiteralContext ctx) {
        final String integerLiteral = ctx.getText().toLowerCase();

        int lastCharIndex = integerLiteral.length() - 1;
        char lastChar = integerLiteral.charAt(lastCharIndex);

        // if there is a suffix then strip it
        if (Character.isAlphabetic(lastChar))
            sb.append(integerLiteral, 0, lastCharIndex);
        else
            sb.append(integerLiteral);

        return null;
    }

    @Override
    public Void visitFloatLiteral(final GremlinParser.FloatLiteralContext ctx) {
        if (ctx.infLiteral() != null) return visit(ctx.infLiteral());
        if (ctx.nanLiteral() != null) return visit(ctx.nanLiteral());

        final String floatLiteral = ctx.getText().toLowerCase();

        // check suffix
        final int lastCharIndex = floatLiteral.length() - 1;
        final char lastChar = floatLiteral.charAt(lastCharIndex);
        // if there is a suffix then strip it
        if (Character.isAlphabetic(lastChar))
            sb.append(floatLiteral, 0, lastCharIndex);
        else
            sb.append(floatLiteral);

        return null;
    }

    @Override
    public Void visitGenericRangeLiteral(final GremlinParser.GenericRangeLiteralContext ctx) {
        throw new TranslatorException("Javascript does not support range literals");
    }

    @Override
    public Void visitGenericSetLiteral(final GremlinParser.GenericSetLiteralContext ctx) {
        sb.append("new Set([");
        for (int i = 0; i < ctx.genericLiteral().size(); i++) {
            final GremlinParser.GenericLiteralContext genericLiteralContext = ctx.genericLiteral(i);
            visit(genericLiteralContext);
            if (i < ctx.genericLiteral().size() - 1)
                sb.append(", ");
        }
        sb.append("])");
        return null;
    }

    @Override
    public Void visitGenericCollectionLiteral(final GremlinParser.GenericCollectionLiteralContext ctx) {
        sb.append("[");
        for (int i = 0; i < ctx.genericLiteral().size(); i++) {
            final GremlinParser.GenericLiteralContext genericLiteralContext = ctx.genericLiteral(i);
            visit(genericLiteralContext);
            if (i < ctx.genericLiteral().size() - 1)
                sb.append(", ");
        }
        sb.append("]");
        return null;
    }

    @Override
    public Void visitUuidLiteral(final GremlinParser.UuidLiteralContext ctx) {
        if (ctx.stringLiteral() == null) {
            sb.append("uuid.v4()");
            return null;
        }
        visitStringLiteral(ctx.stringLiteral());
        return null;
    }

    @Override
    protected String getCardinalityFunctionClass() {
        return "CardinalityValue";
    }

    @Override
    protected String processGremlinSymbol(final String step) {
        return SymbolHelper.toJavascript(step);
    }

    private void wrapTextInQuotes(final String text) {
        sb.append("\"");
        sb.append(text);
        sb.append("\"");
    }

    static final class SymbolHelper {

        private final static Map<String, String> TO_JS_MAP = new HashMap<>();
        private final static Map<String, String> FROM_JS_MAP = new HashMap<>();

        static {
            TO_JS_MAP.put("from", "from_");
            TO_JS_MAP.put("in", "in_");
            TO_JS_MAP.put("with", "with_");
            TO_JS_MAP.put("bigdecimal", "bigDecimal");
            TO_JS_MAP.put("bigint", "bigInt");
            //
            TO_JS_MAP.forEach((k, v) -> FROM_JS_MAP.put(v, k));
        }

        private SymbolHelper() {
            // static methods only, do not instantiate
        }

        public static String toJavascript(final String symbol) {
            return TO_JS_MAP.getOrDefault(symbol, symbol);
        }

        public static String toJava(final String symbol) {
            return FROM_JS_MAP.getOrDefault(symbol, symbol);
        }

    }
}
