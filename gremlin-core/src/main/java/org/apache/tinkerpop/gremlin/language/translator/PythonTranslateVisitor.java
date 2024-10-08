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

import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.tinkerpop.gremlin.language.grammar.GremlinParser;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.util.DatetimeHelper;

import java.math.BigInteger;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Converts a Gremlin traversal string into a Python source code representation of that traversal with an aim at
 * sacrificing some formatting for the ability to compile correctly.
 * <ul>
 *     <li>Range syntax has no direct support</li>
 *     <li>Normalizes whitespace</li>
 *     <li>If floats are not suffixed they will translate as BigDecimal</li>
 *     <li>Makes anonymous traversals explicit with double underscore</li>
 *     <li>Makes enums explicit with their proper name</li>
 * </ul>
 */
public class PythonTranslateVisitor extends AbstractTranslateVisitor {

    public PythonTranslateVisitor() {
        super("g");
    }

    public PythonTranslateVisitor(final String graphTraversalSourceName) {
        super(graphTraversalSourceName);
    }

    @Override
    public Void visitBooleanLiteral(final GremlinParser.BooleanLiteralContext ctx) {
        // capitalize the first letter of the text
        final String text = ctx.getText();
        sb.append(text.substring(0, 1).toUpperCase()).append(text.substring(1));
        return null;
    }

    @Override
    public Void visitStructureVertex(final GremlinParser.StructureVertexContext ctx) {
        sb.append("Vertex(");
        visit(ctx.getChild(3)); // id
        sb.append(", ");
        visit(ctx.getChild(5)); // label
        sb.append(")");
        return null;
    }

    @Override
    public Void visitTraversalStrategy(final GremlinParser.TraversalStrategyContext ctx) {
        if (ctx.getChildCount() == 1)
            sb.append(ctx.getText()).append("()");
        else {
            sb.append(ctx.getChild(0).getText().equals("new") ? ctx.getChild(1).getText() : ctx.getChild(0).getText()).append("(");

            final List<ParseTree> configs = ctx.children.stream().
                    filter(c -> c instanceof GremlinParser.ConfigurationContext).collect(Collectors.toList());

            // the rest are the arguments to the strategy
            for (int ix = 0; ix < configs.size(); ix++) {
                visit(configs.get(ix));
                if (ix < configs.size() - 1)
                    sb.append(", ");
            }

            sb.append(")");
        }

        return null;
    }

    @Override
    public Void visitConfiguration(final GremlinParser.ConfigurationContext ctx) {
        // form of three tokens of key:value to become key=value
        sb.append(SymbolHelper.toPython(ctx.getChild(0).getText()));
        sb.append("=");
        visit(ctx.getChild(2));
        return null;
    }

    @Override
    public Void visitGenericLiteralMap(final GremlinParser.GenericLiteralMapContext ctx) {
        sb.append("{ ");
        for (int i = 0; i < ctx.mapEntry().size(); i++) {
            final GremlinParser.MapEntryContext mapEntryContext = ctx.mapEntry(i);
            visit(mapEntryContext);
            if (i < ctx.mapEntry().size() - 1)
                sb.append(", ");
        }
        sb.append(" }");
        return null;
    }

    @Override
    public Void visitMapEntry(final GremlinParser.MapEntryContext ctx) {
        // if it is a terminal node that isn't a starting form like "(T.id)" then it has to be processed as a string
        // for Java but otherwise it can just be handled as a generic literal
        final boolean isKeyWrappedInParens = ctx.getChild(0).getText().equals("(");
        if (ctx.getChild(0) instanceof TerminalNode && !isKeyWrappedInParens) {
            handleStringLiteralText(ctx.getChild(0).getText());
        }  else {
            final int indexOfActualKey = isKeyWrappedInParens ? 1 : 0;
            visit(ctx.getChild(indexOfActualKey));
        }
        sb.append(": ");
        final int indexOfValue = isKeyWrappedInParens ? 4 : 2;
        visit(ctx.getChild(indexOfValue)); // value
        return null;
    }

    @Override
    public Void visitDateLiteral(final GremlinParser.DateLiteralContext ctx) {
        // child at 2 is the date argument to datetime() and comes enclosed in quotes
        final String dtString = ctx.getChild(2).getText();
        final OffsetDateTime dt = DatetimeHelper.parse(removeFirstAndLastCharacters(dtString));
        sb.append("datetime.datetime.fromtimestamp(" + dt.toEpochSecond() + ").astimezone(datetime.timezone.utc)");
        return null;
    }

    @Override
    public Void visitNullLiteral(final GremlinParser.NullLiteralContext ctx) {
        sb.append("None");
        return null;
    }

    @Override
    public Void visitNanLiteral(final GremlinParser.NanLiteralContext ctx) {
        sb.append("float('nan')");
        return null;
    }

    @Override
    public Void visitInfLiteral(final GremlinParser.InfLiteralContext ctx) {
        if (ctx.SignedInfLiteral().getText().equals("-Infinity"))
            sb.append("float('-inf')");
        else
            sb.append("float('inf')");
        return null;
    }

    @Override
    public Void visitStringNullableLiteral(final GremlinParser.StringNullableLiteralContext ctx) {
        // remove the first and last character (single or double quotes) but only if it is not null
        if (ctx.getText().equals("null")) {
            sb.append("None");
        } else {
            final String text = removeFirstAndLastCharacters(ctx.getText());
            handleStringLiteralText(text);
        }
        return null;
    }

    @Override
    public Void visitIntegerLiteral(final GremlinParser.IntegerLiteralContext ctx) {
        String integerLiteral = ctx.getText().toLowerCase();

        // check suffix
        int lastCharIndex = integerLiteral.length() - 1;
        char lastChar = integerLiteral.charAt(lastCharIndex);

        // if the last character is not alphabetic then try to interpret the right type and append the suffix
        if (!Character.isAlphabetic(lastChar)) {
            final BigInteger bi = new BigInteger(integerLiteral);

            // get explicit about long
            if (bi.bitLength() > 32) {
                integerLiteral = integerLiteral + "l";
                lastChar = 'l';
                lastCharIndex++;
            }
        }

        switch (lastChar) {
            case 'b':
            case 's':
            case 'i':
                sb.append(integerLiteral, 0, lastCharIndex);
                break;
            case 'n':
                sb.append("bigint(");
                sb.append(integerLiteral, 0, lastCharIndex);
                sb.append(")");
                break;
            case 'l':
                sb.append("long(");
                sb.append(integerLiteral, 0, lastCharIndex);
                sb.append(")");
                break;
            default:
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
        final char lastChar = floatLiteral.charAt(lastCharIndex);
        switch (lastChar) {
            case 'm':
            case 'f':
            case 'd':
                sb.append(floatLiteral, 0, lastCharIndex);
                break;
            default:
                // everything else just goes as specified
                sb.append(floatLiteral);
                break;
        }
        return null;
    }

    @Override
    public Void visitGenericLiteralRange(final GremlinParser.GenericLiteralRangeContext ctx) {
        throw new TranslatorException("Python does not support range literals");
    }

    @Override
    public Void visitGenericLiteralSet(final GremlinParser.GenericLiteralSetContext ctx) {
        sb.append("{");
        for (int i = 0; i < ctx.genericLiteral().size(); i++) {
            final GremlinParser.GenericLiteralContext genericLiteralContext = ctx.genericLiteral(i);
            visit(genericLiteralContext);
            if (i < ctx.genericLiteral().size() - 1)
                sb.append(", ");
        }
        sb.append("}");
        return null;
    }

    @Override
    public Void visitGenericLiteralCollection(final GremlinParser.GenericLiteralCollectionContext ctx) {
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
    public Void visitTraversalCardinality(final GremlinParser.TraversalCardinalityContext ctx) {
        // handle the enum style of cardinality if there is one child, otherwise it's the function call style
        if (ctx.getChildCount() == 1)
            appendExplicitNaming(ctx.getText(), VertexProperty.Cardinality.class.getSimpleName());
        else {
            String txt = ctx.getChild(0).getText();
            if (txt.startsWith("Cardinality.")) {
                txt = txt.replaceFirst("Cardinality.", "");
            }
            appendExplicitNaming(txt, "CardinalityValue");
            appendStepOpen();
            visit(ctx.getChild(2));
            appendStepClose();
        }

        return null;
    }

    @Override
    protected String processGremlinSymbol(final String step) {
        return SymbolHelper.toPython(step);
    }

    protected void handleStringLiteralText(final String text) {
        sb.append("'");
        sb.append(text);
        sb.append("'");
    }

    static final class SymbolHelper {

        private final static Map<String, String> TO_PYTHON_MAP = new HashMap<>();
        private final static Map<String, String> FROM_PYTHON_MAP = new HashMap<>();

        static {
            TO_PYTHON_MAP.put("global", "global_");
            TO_PYTHON_MAP.put("all", "all_");
            TO_PYTHON_MAP.put("and", "and_");
            TO_PYTHON_MAP.put("any", "any_");
            TO_PYTHON_MAP.put("as", "as_");
            TO_PYTHON_MAP.put("filter", "filter_");
            TO_PYTHON_MAP.put("format", "format_");
            TO_PYTHON_MAP.put("from", "from_");
            TO_PYTHON_MAP.put("id", "id_");
            TO_PYTHON_MAP.put("in", "in_");
            TO_PYTHON_MAP.put("is", "is_");
            TO_PYTHON_MAP.put("list", "list_");
            TO_PYTHON_MAP.put("max", "max_");
            TO_PYTHON_MAP.put("min", "min_");
            TO_PYTHON_MAP.put("or", "or_");
            TO_PYTHON_MAP.put("not", "not_");
            TO_PYTHON_MAP.put("range", "range_");
            TO_PYTHON_MAP.put("set", "set_");
            TO_PYTHON_MAP.put("sum", "sum_");
            TO_PYTHON_MAP.put("with", "with_");
            //
            TO_PYTHON_MAP.forEach((k, v) -> FROM_PYTHON_MAP.put(v, k));
        }

        private SymbolHelper() {
            // static methods only, do not instantiate
        }

        public static String toPython(final String symbol) {
            return TO_PYTHON_MAP.getOrDefault(symbol, convertCamelCaseToSnakeCase(symbol));
        }

        public static String convertCamelCaseToSnakeCase(final String camelCase) {
            if (camelCase == null || camelCase.isEmpty())
                return camelCase;

            // skip if this is a class/enum indicated by the first letter being upper case
            if (Character.isUpperCase(camelCase.charAt(0)))
                return camelCase;

            final StringBuilder sb = new StringBuilder();
            for (int i = 0; i < camelCase.length(); i++) {
                final char c = camelCase.charAt(i);
                if (Character.isUpperCase(c)) {
                    sb.append("_");
                    sb.append(Character.toLowerCase(c));
                } else {
                    sb.append(c);
                }
            }
            return sb.toString();
        }

        public static String toJava(final String symbol) {
            return FROM_PYTHON_MAP.getOrDefault(symbol, symbol);
        }

    }

}
