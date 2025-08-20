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

import org.apache.tinkerpop.gremlin.process.traversal.step.GType;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import java.util.Map;
import java.util.Objects;

public class ArgumentVisitor extends DefaultGremlinBaseVisitor<Object> {

    private final VariableResolver resolver;

    private final GremlinAntlrToJava antlr;

    public ArgumentVisitor(final VariableResolver resolver, final GremlinAntlrToJava antlr) {
        this.resolver = resolver;
        this.antlr = antlr;
    }

    /**
     * Wrapper for visit function for {@code Map} types.
     */
    public Map parseMap(final GremlinParser.GenericMapNullableArgumentContext ctx) {
        final Object literalOrVar = visitGenericMapNullableArgument(ctx);
        if (GValue.valueInstanceOf(literalOrVar, Map.class)) {
            return ((GValue<Map>) literalOrVar).get();
        } else {
            return (Map) literalOrVar;
        }
    }

    /**
     * Wrapper for visit function for list types.
     */
    public Object[] parseObjectVarargs(final GremlinParser.GenericArgumentVarargsContext ctx) {
        if (ctx == null || ctx.genericArgument() == null) {
            return new Object[0];
        }
        return ctx.genericArgument()
                .stream()
                .filter(Objects::nonNull)
                .map(antlr.argumentVisitor::visitGenericArgument)
                .toArray(Object[]::new);
    }

    /**
     * Parse a string argument varargs, and return an string array
     */
    public GValue<String>[] parseStringVarargs(final GremlinParser.StringNullableArgumentVarargsContext varargsContext) {
        if (varargsContext == null || varargsContext.stringNullableArgument() == null) {
            return new GValue[0];
        }
        return varargsContext.stringNullableArgument()
                .stream()
                .filter(Objects::nonNull)
                .map(antlr.argumentVisitor::parseString)
                .toArray(GValue[]::new);
    }

    /**
     * Wrapper to visit function for string types.
     */
    public GValue<String> parseString(final GremlinParser.StringNullableArgumentContext ctx) {
        final Object literalOrVar = visitStringNullableArgument((ctx));
        if (GValue.valueInstanceOf(literalOrVar, String.class)) {
            return (GValue<String>) literalOrVar;
        } else {
            return GValue.ofString(null, (String) literalOrVar);
        }
    }

    /**
     * Equivalent to {@link ArgumentVisitor#visitIntegerArgument(GremlinParser.IntegerArgumentContext)} except this
     * method promotes output types to either Long or GValue<Long>. (visitIntegerArgument() may produce byte, short,
     * int, or long depending on the input script)
     */
    public Object parseLong(final GremlinParser.IntegerArgumentContext ctx) {
        if (ctx.integerLiteral() != null) {
            return antlr.genericVisitor.parseIntegral(ctx.integerLiteral()).longValue();
        } else {
            final Object var = visitVariable(ctx.variable());
            if (var instanceof Number) {
                return ((Number) var).longValue();
            }
            if (GValue.valueInstanceOf(var, Long.class)) {
                return var;
            } else if (GValue.valueInstanceOf(var, Integer.class)) {
                return GValue.ofLong(((GValue<Integer>) var).getName(), ((GValue<Integer>) var).get().longValue());
            } else if (GValue.valueInstanceOf(var, Short.class)) {
                return GValue.ofLong(((GValue<Short>) var).getName(), ((GValue<Short>) var).get().longValue());
            } else if (GValue.valueInstanceOf(var, Byte.class)) {
                return GValue.ofLong(((GValue<Byte>) var).getName(), ((GValue<Byte>) var).get().longValue());
            } else {
                throw new GremlinParserException(String.format("Expected variable [%s] to resolve to an integer type, instead found: %s", ctx.variable().Identifier().getSymbol(), var.getClass().getName()));
            }
        }
    }

    @Override
    public Object visitBooleanArgument(final GremlinParser.BooleanArgumentContext ctx) {
        if (ctx.booleanLiteral() != null) {
            return antlr.genericVisitor.parseBoolean(ctx.booleanLiteral());
        } else {
            return visitVariable(ctx.variable());
        }
    }

    @Override
    public Object visitIntegerArgument(final GremlinParser.IntegerArgumentContext ctx) {
        if (ctx.integerLiteral() != null) {
            return antlr.genericVisitor.parseIntegral(ctx.integerLiteral());
        } else {
            return visitVariable(ctx.variable());
        }
    }

    @Override
    public Object visitFloatArgument(final GremlinParser.FloatArgumentContext ctx) {
        if (ctx.floatLiteral() != null) {
            return antlr.genericVisitor.parseFloating(ctx.floatLiteral());
        } else {
            return visitVariable(ctx.variable());
        }
    }

    @Override
    public Object visitStringArgument(final GremlinParser.StringArgumentContext ctx) {
        if (ctx.stringLiteral() != null) {
            return antlr.genericVisitor.parseString(ctx.stringLiteral());
        } else {
            return visitVariable(ctx.variable());
        }
    }

    @Override
    public Object visitStringNullableArgument(final GremlinParser.StringNullableArgumentContext ctx) {
        if (ctx.stringNullableLiteral() != null) {
            return antlr.genericVisitor.parseString(ctx.stringNullableLiteral());
        } else {
            return visitVariable(ctx.variable());
        }
    }

    @Override
    public Object visitStringNullableArgumentVarargs(final GremlinParser.StringNullableArgumentVarargsContext ctx) {
        if (ctx == null || ctx.stringNullableArgument() == null) {
            return new Object[0];
        }
        return ctx.stringNullableArgument()
                .stream()
                .filter(Objects::nonNull)
                .map(antlr.argumentVisitor::visitStringNullableArgument)
                .toArray();
    }

    @Override
    public Object visitDateArgument(final GremlinParser.DateArgumentContext ctx) {
        if (ctx.dateLiteral() != null) {
            return antlr.genericVisitor.parseDate(ctx.dateLiteral());
        } else {
            return visitVariable(ctx.variable());
        }
    }

    @Override
    public Object visitGenericArgument(final GremlinParser.GenericArgumentContext ctx) {
        if (ctx.genericLiteral() != null) {
            return antlr.genericVisitor.visitGenericLiteral(ctx.genericLiteral());
        } else {
            return visitVariable(ctx.variable());
        }
    }

    @Override
    public Object visitGenericMapArgument(final GremlinParser.GenericMapArgumentContext ctx) {
        if (ctx.genericMapLiteral() != null) {
            return antlr.genericVisitor.visitGenericMapLiteral(ctx.genericMapLiteral());
        } else {
            return visitVariable(ctx.variable());
        }
    }

    @Override
    public Object visitGenericMapNullableArgument(final GremlinParser.GenericMapNullableArgumentContext ctx) {
        if (ctx.genericMapNullableLiteral() != null) {
            return antlr.genericVisitor.visitGenericMapNullableLiteral(ctx.genericMapNullableLiteral());
        } else {
            return visitVariable(ctx.variable());
        }
    }

    @Override
    public Object visitVariable(final GremlinParser.VariableContext ctx) {
        return resolver.apply(ctx.getText(), ctx);
    }

    /**
     * Create a new {@code GValue} from a particular value but without the specified name. If the argument provide is
     * already a {@code GValue} then it is returned as-is.
     *
     * @param value the value of the variable
     */
    public static <V> GValue<V> asGValue(final V value) {
        if (value instanceof GValue) return (GValue) value;
        return GValue.of(null, value);
    }
}
