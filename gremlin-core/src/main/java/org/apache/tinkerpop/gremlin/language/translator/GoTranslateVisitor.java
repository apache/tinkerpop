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
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.language.grammar.GremlinParser;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.OptionsStrategy;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.util.DatetimeHelper;

import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class GoTranslateVisitor extends AbstractTranslateVisitor {
    private final static String GO_PACKAGE_NAME = "gremlingo.";
    private final static List<String> STRATEGY_WITH_MAP_OPTS = Collections.unmodifiableList(Arrays.asList(
            "OptionsStrategy",
            "ReferenceElementStrategy", "ComputerFinalizationStrategy", "ProfileStrategy",
            "ComputerVerificationStrategy", "StandardVerificationStrategy", "VertexProgramRestrictionStrategy"));
    private final static List<String> STRATEGY_WITH_STRING_SLICE = Collections.unmodifiableList(Arrays.asList(
            "ReservedKeysVerificationStrategy", "ProductiveByStrategy"));

    public GoTranslateVisitor() {
        super("g");
    }

    public GoTranslateVisitor(final String graphTraversalSourceName) {
        super(graphTraversalSourceName);
    }

    @Override
    public Void visitDateLiteral(final GremlinParser.DateLiteralContext ctx) {
        // child at 2 is the date argument to datetime() and comes enclosed in quotes
        final String dtString = ctx.getChild(2).getText();
        final OffsetDateTime dt = DatetimeHelper.parse(removeFirstAndLastCharacters(dtString));
        final String zoneInfo = dt.getOffset().getId().equals("Z") ? "UTC+00:00" : "UTC" + dt.getOffset().getId();
        sb.append("time.Date(").append(dt.getYear()).
                append(", ").append(dt.getMonthValue()).
                append(", ").append(dt.getDayOfMonth()).
                append(", ").append(dt.getHour()).
                append(", ").append(dt.getMinute()).
                append(", ").append(dt.getSecond()).
                append(", ").append(dt.getNano()).
                append(", time.FixedZone(\"").append(zoneInfo).append("\", ").append(dt.getOffset().getTotalSeconds()).append(")").
                append(")");
        return null;
    }

    @Override
    public Void visitInfLiteral(final GremlinParser.InfLiteralContext ctx) {
        if (ctx.SignedInfLiteral() != null && ctx.SignedInfLiteral().getText().equals("-Infinity"))
            sb.append("math.Inf(-1)");
        else
            sb.append("math.Inf(1)");
        return null;
    }

    @Override
    public Void visitIntegerLiteral(final GremlinParser.IntegerLiteralContext ctx) {
        String integerLiteral = ctx.getText().toLowerCase();

        // check suffix
        int lastCharIndex = integerLiteral.length() - 1;
        char lastChar = integerLiteral.charAt(lastCharIndex);

        // if the last character is not alphabetic then try to interpret the right type and append the suffix
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

        String floatLiteral = ctx.getText().toLowerCase();

        // check suffix
        int lastCharIndex = floatLiteral.length() - 1;
        char lastChar = floatLiteral.charAt(lastCharIndex);

        // if the last character is not alphabetic then try to interpret the right type and append the suffix
        if (Character.isAlphabetic(lastChar))
            sb.append(floatLiteral, 0, lastCharIndex);
        else
            sb.append(floatLiteral);
        return null;
    }

    @Override
    public Void visitGenericRangeLiteral(final GremlinParser.GenericRangeLiteralContext ctx) {
        throw new TranslatorException("Go does not support range literals");
    }

    @Override
    public Void visitGenericSetLiteral(final GremlinParser.GenericSetLiteralContext ctx) {
        sb.append(GO_PACKAGE_NAME);
        sb.append("NewSimpleSet(");
        for (int i = 0; i < ctx.genericLiteral().size(); i++) {
            final GremlinParser.GenericLiteralContext genericLiteralContext = ctx.genericLiteral(i);
            visit(genericLiteralContext);
            if (i < ctx.genericLiteral().size() - 1)
                sb.append(", ");
        }
        sb.append(")");
        return null;
    }

    @Override
    public Void visitGenericCollectionLiteral(final GremlinParser.GenericCollectionLiteralContext ctx) {
        sb.append("[]interface{}{");
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
    public Void visitGenericMapLiteral(final GremlinParser.GenericMapLiteralContext ctx) {
        sb.append("map[interface{}]interface{}{");
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
        visit(ctx.mapKey());
        sb.append(": ");
        visit(ctx.genericLiteral());
        return null;
    }

    @Override
    public Void visitMapKey(final GremlinParser.MapKeyContext ctx) {
        final int keyIndex = ctx.LPAREN() != null && ctx.RPAREN() != null ? 1 : 0;
        visit(ctx.getChild(keyIndex));
        return null;
    }

    @Override
    public Void visitStringNullableLiteral(GremlinParser.StringNullableLiteralContext ctx) {
        // remove the first and last character (single or double quotes) but only if it is not null
        if (ctx.getText().equals("null")) {
            sb.append("nil");
        } else {
            final String text = removeFirstAndLastCharacters(ctx.getText());
            handleStringLiteralText(text);
        }
        return null;
    }

    @Override
    public Void visitNanLiteral(final GremlinParser.NanLiteralContext ctx) {
        sb.append("math.NaN()");
        return null;
    }

    @Override
    public Void visitNullLiteral(final GremlinParser.NullLiteralContext ctx) {
        sb.append("nil");
        return null;
    }

    @Override
    public Void visitTraversalStrategy(final GremlinParser.TraversalStrategyContext ctx) {
        if (ctx.getChildCount() == 1)
            sb.append(GO_PACKAGE_NAME).append(ctx.getText()).append("()");
        else {
            String strategyName = ctx.getChild(0).getText().equals("new") ? ctx.getChild(1).getText() : ctx.getChild(0).getText();
            sb.append(GO_PACKAGE_NAME).append(strategyName).append("(");
            if (!STRATEGY_WITH_MAP_OPTS.contains(strategyName)) { // omit strategies which use plain map instead of Config struct
                sb.append(GO_PACKAGE_NAME).append(strategyName).append("Config{");
            }

            // get a list of all the arguments to the strategy - i.e. anything not a terminal node
            final List<ParseTree> configs = ctx.children.stream().
                    filter(c -> c instanceof GremlinParser.ConfigurationContext).collect(Collectors.toList());

            // the rest are the arguments to the strategy
            for (int ix = 0; ix < configs.size(); ix++) {
                visit(configs.get(ix));
                if (ix < configs.size() - 1)
                    sb.append(", ");
            }

            if (!Objects.equals(strategyName, "OptionsStrategy")) {
                sb.append("}");
            }
            sb.append(")");
        }
        return null;
    }

    @Override
    public Void visitConfiguration(final GremlinParser.ConfigurationContext ctx) {
        String parent = ctx.getParent().getText();
        String parentName = parent.startsWith("new") ? parent.substring(3, parent.indexOf('(')) : parent.substring(0, parent.indexOf('('));
        if (STRATEGY_WITH_MAP_OPTS.contains(parentName)) { // handle strategies which use plain map instead of Config struct
            sb.append("map[string]interface{}{\"");
            sb.append(ctx.getChild(0).getText());
            sb.append("\": ");
            visit(ctx.getChild(2));
            sb.append("}");
        } else {
            // form of three tokens of key:value to become key=value
            sb.append(SymbolHelper.toGo(ctx.getChild(0).getText()));
            sb.append(": ");
            visit(ctx.getChild(2));
            // handles strategies that takes string slices as config
            if (STRATEGY_WITH_STRING_SLICE.contains(parentName)) {
                final int ix = sb.lastIndexOf("[]interface{}");
                if (ix > 0) {
                    sb.replace(ix, ix +"[]interface{}".length(), "[]string");
                }
            }
        }

        // need to convert List to Set for readPartitions until TINKERPOP-3032
        if (ctx.getChild(0).getText().equals("readPartitions")) {
            final int ix = sb.lastIndexOf("ReadPartitions: [");
            if (ix > 0) {
                final int endIx = sb.indexOf("\"}", ix);
                sb.replace(endIx, endIx + 2, "\")");
                sb.replace(ix, ix + "ReadPartitions: []interface{}{".length(), "ReadPartitions: gremlingo.NewSimpleSet(");
            }

        }

        return null;
    }

    @Override
    public Void visitTraversalSourceSelfMethod_withoutStrategies(final GremlinParser.TraversalSourceSelfMethod_withoutStrategiesContext ctx) {
        sb.append("WithoutStrategies(");
        sb.append(GO_PACKAGE_NAME).append(ctx.classType().getText()).append("()");

        if (ctx.classTypeList() != null && ctx.classTypeList().classTypeExpr() != null) {
            for (GremlinParser.ClassTypeContext classTypeContext : ctx.classTypeList().classTypeExpr().classType()) {
                sb.append(", ").append(GO_PACKAGE_NAME).append(classTypeContext.getText()).append("()");
            }
        }

        sb.append(")");
        return null;
    }

    @Override
    public Void visitUuidLiteral(final GremlinParser.UuidLiteralContext ctx) {
        if (ctx.stringLiteral() == null) {
            sb.append("uuid.New()");
            return null;
        }
        sb.append("uuid.MustParse(");
        visitStringLiteral(ctx.stringLiteral());
        sb.append(")");
        return null;
    }

    @Override
    protected String getCardinalityFunctionClass() {
        return "CardinalityValue";
    }

    protected void visitP(final ParserRuleContext ctx, final Class<?> clazzOfP, final String methodName) {
        sb.append(GO_PACKAGE_NAME);
        super.visitP(ctx, clazzOfP, methodName);
    }

    @Override
    protected String processGremlinSymbol(final String step) {
        return SymbolHelper.toGo(step);
    }

    @Override
    protected void appendExplicitNaming(final String txt, final String prefix) {
        sb.append(GO_PACKAGE_NAME);
        super.appendExplicitNaming(txt, prefix);
    }

    @Override
    protected void appendAnonymousSpawn() {
        sb.append(GO_PACKAGE_NAME).append("T__.");
    }

    static final class SymbolHelper {

        private final static Map<String, String> TO_GO_MAP = new HashMap<>();
        private final static Map<String, String> FROM_GO_MAP = new HashMap<>();

        static {
            TO_GO_MAP.put("OUT", "Out");
            TO_GO_MAP.put("IN", "In");
            TO_GO_MAP.put("BOTH", "Both");
            TO_GO_MAP.put("WithOptions", GO_PACKAGE_NAME + "WithOptions");
            TO_GO_MAP.put("IO", GO_PACKAGE_NAME + "IO");
            TO_GO_MAP.put("__", GO_PACKAGE_NAME + "T__");
            TO_GO_MAP.forEach((k, v) -> FROM_GO_MAP.put(v, k));
        }

        private SymbolHelper() {
            // static methods only, do not instantiate
        }

        public static String toGo(final String symbol) {
            return TO_GO_MAP.getOrDefault(symbol, StringUtils.capitalize(symbol));
        }

        public static String toJava(final String symbol) {
            return FROM_GO_MAP.getOrDefault(symbol, StringUtils.uncapitalize(symbol));
        }

    }
}
