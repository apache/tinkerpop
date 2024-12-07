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
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.tinkerpop.gremlin.process.traversal.Merge;
import org.apache.tinkerpop.gremlin.process.traversal.Pick;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.util.DatetimeHelper;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Visitor class to handle generic literal. All visitor methods return type is Object. It maybe used as a singleton
 * in cases where a {@link Traversal} object is not expected, otherwise a new instance must be constructed.
 */
public class GenericLiteralVisitor extends DefaultGremlinBaseVisitor<Object> {
    /**
     * Limit for integer range result count. It is used to avoid OOM in JVM.
     */
    public static final int TOTAL_INTEGER_RANGE_RESULT_COUNT_LIMIT = 1_000_000;
    protected final GremlinAntlrToJava antlr;

    public GenericLiteralVisitor(final GremlinAntlrToJava antlr) {
        this.antlr = antlr;
    }

    /**
     * Parse integral literal context and return an integral type number.
     */
    public Number parseIntegral(final GremlinParser.IntegerLiteralContext integerLiteral) {
        return (Number) visitIntegerLiteral(integerLiteral);
    }

    /**
     * Parse floating literal context and return a floating type number.
     */
    public Number parseFloating(final GremlinParser.FloatLiteralContext floatLiteral) {
        return (Number) visitFloatLiteral(floatLiteral);
    }

    /**
     * Parse a string based literal context and return the string.
     */
    public String parseString(final GremlinParser.StringLiteralContext stringLiteral) {
        return (String) visitStringLiteral(stringLiteral);
    }

    /**
     * Parse a string based literal context and return the string.
     */
    public String parseString(final GremlinParser.StringNullableLiteralContext stringLiteral) {
        return (String) visitStringNullableLiteral(stringLiteral);
    }

    /**
     * Parse a Date based literal context and return the Date.
     */
    public OffsetDateTime parseDate(final GremlinParser.DateLiteralContext dateLiteral) {
        return (OffsetDateTime) visitDateLiteral(dateLiteral);
    }

    /**
     * Parse a map literal context and return the map literal
     */
    public Map parseMap(final GremlinParser.GenericLiteralMapContext mapLiteral) {
        return (Map) visitGenericLiteralMap(mapLiteral);
    }

    /**
     * Parse a boolean literal context and return the boolean literal
     */
    public boolean parseBoolean(final GremlinParser.BooleanLiteralContext booleanLiteral) {
        return (boolean) visitBooleanLiteral(booleanLiteral);
    }

    /**
     * Parse a String literal list context and return a string array
     */
    public String[] parseStringList(final GremlinParser.StringLiteralListContext stringLiteralList) {
        return (String[]) visitStringLiteralList(stringLiteralList);
    }

    /**
     * Parse a generic literal list, and return an object array
     */
    public Object[] parseObjectList(final GremlinParser.GenericLiteralListContext objectLiteralList) {
        if (objectLiteralList == null || objectLiteralList.genericLiteralExpr() == null) {
            return new Object[0];
        }
        return objectLiteralList.genericLiteralExpr().genericLiteral()
                .stream()
                .filter(Objects::nonNull)
                .map(antlr.genericVisitor::visitGenericLiteral)
                .toArray(Object[]::new);
    }

    /**
     * Parse a generic literal varargs, and return an object array
     */
    public Object[] parseObjectVarargs(final GremlinParser.GenericLiteralVarargsContext varargsContext) {
        if (varargsContext == null || varargsContext.genericLiteralArgument() == null) {
            return new Object[0];
        }
        return varargsContext.genericLiteralArgument()
                .stream()
                .filter(Objects::nonNull)
                .map(antlr.argumentVisitor::visitGenericLiteralArgument)
                .toArray(Object[]::new);
    }

    /**
     * Parse a string literal varargs, and return a string array
     */
    public String[] parseStringVarargsLiterals(final GremlinParser.StringLiteralVarargsLiteralsContext varargsContext) {
        if (varargsContext == null || varargsContext.stringNullableLiteral() == null) {
            return new String[0];
        }
        return varargsContext.stringNullableLiteral()
                .stream()
                .filter(Objects::nonNull)
                .map(antlr.genericVisitor::parseString)
                .toArray(String[]::new);
    }

    /**
     * Parse a TraversalStrategy literal list context and return a string array
     */
    public static TraversalStrategy[] parseTraversalStrategyList(final GremlinParser.TraversalStrategyListContext traversalStrategyListContext,
                                                                 final DefaultGremlinBaseVisitor<TraversalStrategy> traversalStrategyVisitor) {
        if (traversalStrategyListContext == null || traversalStrategyListContext.traversalStrategyExpr() == null) {
            return new TraversalStrategy[0];
        }
        return traversalStrategyListContext.traversalStrategyExpr().traversalStrategy()
                .stream()
                .filter(Objects::nonNull)
                .map(tstrat -> traversalStrategyVisitor.visitTraversalStrategy(tstrat))
                .toArray(TraversalStrategy[]::new);
    }

    /**
     * Remove single/double quotes around String literal
     *
     * @param quotedString : quoted string
     * @return quotes stripped string
     */
    private static String stripQuotes(final String quotedString) {
        return quotedString.substring(1, quotedString.length() - 1);
    }

    /**
     * create an integer range from start to end, based on groovy syntax
     * http://groovy-lang.org/operators.html#_range_operator
     *
     * @param start : start of range
     * @param end   : end of range
     * @param range : original range string, for error message
     * @return : return an object which is type of array of object, and each object is a Integer inside the range.
     */
    private static Object createIntegerRange(final int start, final int end, final String range) {
        final List<Object> results = new ArrayList<>();
        int total_result_count = Math.abs(start - end);

        // validate result count not exceeding limit
        if (total_result_count > TOTAL_INTEGER_RANGE_RESULT_COUNT_LIMIT) {
            throw new IllegalArgumentException("Range " + range + " is too wide. Current limit is " + TOTAL_INTEGER_RANGE_RESULT_COUNT_LIMIT + " items");
        }

        if (start <= end) {
            // handle start <= end
            int cur = start;
            while (cur <= end) {
                results.add(cur);
                cur++;
            }
        } else {
            // handle start > end
            int cur = start;
            while (cur >= end) {
                results.add(cur);
                cur--;
            }
        }

        return results;
    }

    /**
     * create a string range from start to end, based on groovy syntax
     * http://groovy-lang.org/operators.html#_range_operator
     * The start and end needs to have same length and share same prefix except the last character.
     *
     * @param start : start of range
     * @param end   : end of range
     * @param range : original range string, for error message
     * @return : return an object which is type of array of object, and each object is a String inside the range.
     */
    private static Object createStringRange(final String start, final String end, final String range) {
        final List<Object> results = new ArrayList<>();

        // verify lengths of start and end are same.
        if (start.length() != end.length()) {
            throw new IllegalArgumentException("The start and end of Range " + range + " does not have same number of characters");
        }

        if (start.isEmpty()) {
            // return empty result for empty string ranges
            return results;
        }

        // verify start and end share same prefix
        final String commonPrefix = start.substring(0, start.length() - 1);
        if (!end.startsWith(commonPrefix)) {
            throw new IllegalArgumentException("The start and end of Range " + range +
                    " does not share same prefix until the last character");
        }

        final char startLastCharacter = start.charAt(start.length() - 1);
        final char endLastCharacter = end.charAt(end.length() - 1);

        if (startLastCharacter <= endLastCharacter) {
            // handle start <= end
            char cur = startLastCharacter;
            while (cur <= endLastCharacter) {
                results.add(commonPrefix + cur);
                cur++;
            }
        } else {
            // handle start > end
            char cur = startLastCharacter;
            while (cur >= endLastCharacter) {
                results.add(commonPrefix + cur);
                cur--;
            }
        }

        return results;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object visitGenericLiteralList(final GremlinParser.GenericLiteralListContext ctx) {
        return visitChildren(ctx);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object visitGenericLiteralExpr(final GremlinParser.GenericLiteralExprContext ctx) {
        final int childCount = ctx.getChildCount();
        switch (childCount) {
            case 0:
                // handle empty expression
                return new Object[0];
            case 1:
                // handle single generic literal
                return antlr.genericVisitor.visitGenericLiteral(ctx.genericLiteral(0));
            default:
                // handle multiple generic literal separated by comma
                final List<Object> genericLiterals = new ArrayList<>();
                for (GremlinParser.GenericLiteralContext ic : ctx.genericLiteral()) {
                    genericLiterals.add(antlr.genericVisitor.visitGenericLiteral(ic));
                }
                return genericLiterals.toArray();
        }
    }

    @Override
    public Object visitGenericLiteralSet(final GremlinParser.GenericLiteralSetContext ctx) {
        final Set<Object> result = new HashSet<>(ctx.getChildCount() / 2);
        for (GremlinParser.GenericLiteralContext ic : ctx.genericLiteral()) {
            result.add(antlr.genericVisitor.visitGenericLiteral(ic));
        }
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object visitGenericLiteral(final GremlinParser.GenericLiteralContext ctx) {
        return visitChildren(ctx);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object visitGenericLiteralMap(final GremlinParser.GenericLiteralMapContext ctx) {
        if (ctx == null) {
            return null;
        }

        final LinkedHashMap<Object, Object> literalMap = new LinkedHashMap<>();

        // filter out tokens and just grab map entries
        ctx.children.stream().filter(c -> c instanceof GremlinParser.MapEntryContext).forEach(c -> {
            // [key : value] - index 0 is key, index 1 is COLON, index 2 is value
            // also note that a child count of 5 indicates the key is an expression wrapped
            // in parens as in `[(T.id): 1]`
            final boolean isKeyExpression = c.getChildCount() == 5;
            final Object kctx = isKeyExpression ? c.getChild(1) : c.getChild(0);
            final Object key;
            if (kctx instanceof GremlinParser.StringLiteralContext) {
                key = visitStringLiteral((GremlinParser.StringLiteralContext) kctx);
            } else if (kctx instanceof GremlinParser.NumericLiteralContext) {
                key = visitNumericLiteral((GremlinParser.NumericLiteralContext) kctx);
            } else if (kctx instanceof GremlinParser.TraversalTokenContext) {
                key = visitTraversalToken((GremlinParser.TraversalTokenContext) kctx);
            } else if (kctx instanceof GremlinParser.TraversalDirectionContext) {
                key = visitTraversalDirection((GremlinParser.TraversalDirectionContext) kctx);
            } else if (kctx instanceof GremlinParser.GenericLiteralCollectionContext) {
                key = visitGenericLiteralCollection((GremlinParser.GenericLiteralCollectionContext) kctx);
            } else if (kctx instanceof GremlinParser.GenericLiteralSetContext) {
                key = visitGenericLiteralSet((GremlinParser.GenericLiteralSetContext) kctx);
            } else if (kctx instanceof GremlinParser.GenericLiteralMapContext) {
                key = visitGenericLiteralMap((GremlinParser.GenericLiteralMapContext) kctx);
            } else if (kctx instanceof GremlinParser.KeywordContext) {
                key = ((GremlinParser.KeywordContext) kctx).getText();
            } else if (kctx instanceof TerminalNode) {
                key = ((TerminalNode) kctx).getText();
            } else {
                throw new GremlinParserException("Invalid key for map " + ((ParseTree) kctx).getText());
            }

            final int valueIndex = isKeyExpression ? 4 : 2;
            final Object value = visitGenericLiteral((GremlinParser.GenericLiteralContext) c.getChild(valueIndex));
            literalMap.put(key, value);
        });

        return literalMap;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object visitNestedTraversal(final GremlinParser.NestedTraversalContext ctx) {
        return antlr.tvisitor.visitNestedTraversal(ctx);
    }

    @Override
    public Object visitStructureVertex(final GremlinParser.StructureVertexContext ctx) {
        return antlr.structureVisitor.visitStructureVertex(ctx);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object visitTerminatedTraversal(final GremlinParser.TerminatedTraversalContext ctx) {
        final Traversal traversal = antlr.tvisitor.visitRootTraversal(
                (GremlinParser.RootTraversalContext) ctx.getChild(0));
        return new TraversalTerminalMethodVisitor(traversal).visitTraversalTerminalMethod(
                (GremlinParser.TraversalTerminalMethodContext) ctx.getChild(2));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object visitIntegerLiteral(final GremlinParser.IntegerLiteralContext ctx) {
        String integerLiteral = ctx.getText().toLowerCase().replace("_", "");
        // handle suffixes for specific types
        final int lastCharIndex = integerLiteral.length() - 1;
        final char suffix = integerLiteral.charAt(lastCharIndex);
        switch (suffix) {
            case 'b':
                integerLiteral = integerLiteral.substring(0, lastCharIndex);
                return Byte.decode(integerLiteral);
            case 's':
                integerLiteral = integerLiteral.substring(0, lastCharIndex);
                return Short.decode(integerLiteral);
            case 'i':
                integerLiteral = integerLiteral.substring(0, lastCharIndex);
                return Integer.decode(integerLiteral);
            case 'l':
                integerLiteral = integerLiteral.substring(0, lastCharIndex);
                return Long.decode(integerLiteral);
            case 'n':
                integerLiteral = integerLiteral.substring(0, lastCharIndex);
                return new BigInteger(integerLiteral);
        }

        try {
            // try to parse it as integer first
            return Integer.decode(integerLiteral);
        } catch (NumberFormatException ignoredExpection1) {
            try {
                // If range exceeds integer limit, try to parse it as long
                return Long.decode(integerLiteral);
            } catch (NumberFormatException ignoredExpection2) {
                // If range exceeds Long limit, parse it as BigInteger
                // as the literal range is longer than long, the number of character should be much more than 3,
                // so we skip boundary check below.

                // parse sign character
                int startIndex = 0;
                final char firstChar = integerLiteral.charAt(0);
                final boolean negative = (firstChar == '-');
                if ((firstChar == '-') || (firstChar == '+')) {
                    startIndex++;
                }

                // parse radix based on format
                int radix = 10;
                if (integerLiteral.charAt(startIndex + 1) == 'x') {
                    radix = 16;
                    startIndex += 2;
                    integerLiteral = integerLiteral.substring(startIndex);
                    if (negative) {
                        integerLiteral = '-' + integerLiteral;
                    }
                } else if (integerLiteral.charAt(startIndex) == '0') {
                    radix = 8;
                }

                // create big integer
                return new BigInteger(integerLiteral, radix);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object visitFloatLiteral(final GremlinParser.FloatLiteralContext ctx) {
        final String floatLiteral = ctx.getText().toLowerCase();

        // check suffix
        final int lastCharIndex = floatLiteral.length() - 1;
        final char lastCharacter = floatLiteral.charAt(lastCharIndex);
        if (lastCharacter == 'm') {
            // parse M/m or whatever which could be a parse exception
            return new BigDecimal(floatLiteral.substring(0, lastCharIndex));
        } else if (lastCharacter == 'f') {
            // parse F/f suffix as Float
            return new Float(ctx.getText());
        } else if (lastCharacter == 'd'){
            // parse D/d suffix as Double
            return new Double(floatLiteral);
        } else {
            return new BigDecimal(floatLiteral);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object visitBooleanLiteral(final GremlinParser.BooleanLiteralContext ctx) {
        return Boolean.valueOf(ctx.getText());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object visitDateLiteral(final GremlinParser.DateLiteralContext ctx) {
        if (ctx.stringArgument() == null)
            return DatetimeHelper.datetime();
        return DatetimeHelper.parse((String) antlr.argumentVisitor.visitStringArgument(ctx.stringArgument()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object visitNumericLiteral(final GremlinParser.NumericLiteralContext ctx) {
        if (ctx.floatLiteral() != null) return visitFloatLiteral(ctx.floatLiteral());
        if (ctx.integerLiteral() != null) return visitIntegerLiteral(ctx.integerLiteral());
        throw new GremlinParserException("Invalid numeric");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object visitStringLiteral(final GremlinParser.StringLiteralContext ctx) {
        return StringEscapeUtils.unescapeJava(stripQuotes(ctx.getText()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object visitTraversalToken(final GremlinParser.TraversalTokenContext ctx) {
        return TraversalEnumParser.parseTraversalEnumFromContext(T.class, ctx);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object visitTraversalCardinality(final GremlinParser.TraversalCardinalityContext ctx) {
        // could be Cardinality.single() the method or single the enum so grab the right child index based on
        // number of children
        if (ctx.getChildCount() == 1) {
            return TraversalEnumParser.parseTraversalEnumFromContext(VertexProperty.Cardinality.class, ctx);
        } else {
            final int idx = ctx.getChildCount() == 5 ? 1 : 0;
            final String specifiedCard = ctx.children.get(idx).getText();
            if (specifiedCard.endsWith(VertexProperty.Cardinality.single.name()))
                return VertexProperty.Cardinality.single(visitGenericLiteral(ctx.genericLiteral()));
            else if (specifiedCard.endsWith(VertexProperty.Cardinality.list.name()))
                return VertexProperty.Cardinality.list(visitGenericLiteral(ctx.genericLiteral()));
            else if (specifiedCard.endsWith(VertexProperty.Cardinality.set.name()))
                return VertexProperty.Cardinality.set(visitGenericLiteral(ctx.genericLiteral()));
            else
                throw new GremlinParserException(String.format(
                        "A Cardinality value not recognized: %s", specifiedCard));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object visitTraversalDirection(final GremlinParser.TraversalDirectionContext ctx) {
        return TraversalEnumParser.parseTraversalDirectionFromContext(ctx);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object visitTraversalMerge(final GremlinParser.TraversalMergeContext ctx) {
        return TraversalEnumParser.parseTraversalEnumFromContext(Merge.class, ctx);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object visitTraversalPick(final GremlinParser.TraversalPickContext ctx) {
        return TraversalEnumParser.parseTraversalEnumFromContext(Pick.class, ctx);
    }

    @Override
    public Object visitTraversalStrategy(final GremlinParser.TraversalStrategyContext ctx) {
        return antlr.traversalStrategyVisitor.visitTraversalStrategy(ctx);
    }

    /**
     * Groovy range operator syntax is defined in http://groovy-lang.org/operators.html#_range_operator
     * {@inheritDoc}
     */
    @Override
    public Object visitGenericLiteralRange(final GremlinParser.GenericLiteralRangeContext ctx) {
        final int childIndexOfParameterStart = 0;
        final int childIndexOfParameterEnd = 3;
        final ParseTree startContext = ctx.getChild(childIndexOfParameterStart);
        final ParseTree endContext = ctx.getChild(childIndexOfParameterEnd);

        if (startContext instanceof GremlinParser.IntegerLiteralContext) {
            // handle integer ranges.
            final int start = Integer.valueOf(startContext.getText());
            final int end = Integer.valueOf(endContext.getText());
            return createIntegerRange(start, end, ctx.getText());
        } else {
            // handle string ranges.
            final String start = stripQuotes(startContext.getText());
            final String end = stripQuotes(endContext.getText());
            return createStringRange(start, end, ctx.getText());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object visitNullLiteral(final GremlinParser.NullLiteralContext ctx) {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object visitNanLiteral(final GremlinParser.NanLiteralContext ctx) {
        return Double.NaN;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object visitInfLiteral(final GremlinParser.InfLiteralContext ctx) {
        final String infLiteral = ctx.getText();
        return infLiteral.charAt(0) == '-' ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY;
    }

    /**
     * {@inheritDoc}
     * Generic literal collection returns a list of {@code Object}
     */
    @Override
    public Object visitGenericLiteralCollection(final GremlinParser.GenericLiteralCollectionContext ctx) {
        final List<Object> result = new ArrayList<>(ctx.getChildCount() / 2);
        for (GremlinParser.GenericLiteralContext ic : ctx.genericLiteral()) {
            result.add(antlr.genericVisitor.visitGenericLiteral(ic));
        }
        return result;
    }

    @Override
    public Object visitStringLiteralList(final GremlinParser.StringLiteralListContext ctx) {
        if (ctx == null || ctx.stringLiteralExpr() == null) {
            return new String[0];
        }
        return ctx.stringLiteralExpr().stringNullableLiteral()
                .stream()
                .filter(Objects::nonNull)
                .map(this::visitStringNullableLiteral)
                .toArray(String[]::new);
    }

    @Override
    public Object visitStringNullableLiteral(final GremlinParser.StringNullableLiteralContext ctx) {
        if (ctx.NullLiteral() != null)
            return null;
        else
            return StringEscapeUtils.unescapeJava(stripQuotes(ctx.getText()));
    }

    @Override
    public Object[] visitStringLiteralVarargs(final GremlinParser.StringLiteralVarargsContext ctx) {
        if (ctx == null) {
            return new Object[0];
        }
        return ctx.children
                .stream()
                .filter(Objects::nonNull)
                .filter(p -> p instanceof GremlinParser.StringNullableArgumentContext)
                .map(p -> (GremlinParser.StringNullableArgumentContext) p)
                .map(antlr.argumentVisitor::visitStringNullableArgument)
                .toArray(Object[]::new);
    }
}
