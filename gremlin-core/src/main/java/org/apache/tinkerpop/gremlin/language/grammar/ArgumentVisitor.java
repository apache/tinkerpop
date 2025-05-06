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

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.lang.reflect.Array;
import java.time.OffsetDateTime;
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
     * Wrapper to visit function for boolean.
     */
    public boolean parseBoolean(final GremlinParser.BooleanArgumentContext ctx) {
        return (boolean) visitBooleanArgument(ctx);
    }

    /**
     * Wrapper to visit function for integer types.
     */
    public Number parseNumber(final GremlinParser.IntegerArgumentContext ctx) {
        return (Number) visitIntegerArgument(ctx);
    }

    /**
     * Wrapper to visit function for float types.
     */
    public Number parseNumber(final GremlinParser.FloatArgumentContext ctx) {
        return (Number) visitFloatArgument(ctx);
    }

    /**
     * Wrapper to visit function for string types.
     */
    public String parseString(final GremlinParser.StringArgumentContext ctx) {
        return (String) visitStringArgument(ctx);
    }

    /**
     * Wrapper to visit function for Date type.
     */
    public OffsetDateTime parseDate(final GremlinParser.DateArgumentContext ctx) {
        return (OffsetDateTime) visitDateArgument(ctx);
    }

    /**
     * Wrapper for visit function for object types.
     */
    public Object parseObject(final GremlinParser.GenericArgumentContext ctx) {
        return visitGenericArgument(ctx);
    }

    /**
     * Wrapper for visit function for {@link Vertex} types.
     */
    public Vertex parseVertex(final GremlinParser.StructureVertexArgumentContext ctx) {
        return (Vertex) visitStructureVertexArgument(ctx);
    }

    /**
     * Wrapper for visit function for {@code Map} types.
     */
    public Map parseMap(final GremlinParser.GenericMapArgumentContext ctx) {
        return (Map) visitGenericMapArgument(ctx);
    }

    /**
     * Wrapper for visit function for {@code Map} types.
     */
    public Map parseMap(final GremlinParser.GenericMapNullableArgumentContext ctx) {
        return (Map) visitGenericMapNullableArgument(ctx);
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
     * Wrapper to visit function for string types.
     */
    public String parseString(final GremlinParser.StringNullableArgumentContext ctx) {
        return (String) visitStringNullableArgument(ctx);
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
    public Object visitStructureVertexArgument(final GremlinParser.StructureVertexArgumentContext ctx) {
        if (ctx.structureVertexLiteral() != null) {
            return antlr.structureVisitor.visitStructureVertexLiteral(ctx.structureVertexLiteral());
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

    /**
     * Parse a string literal varargs, and return a string array
     */
    public String[] parseStringVarargs(final GremlinParser.StringNullableArgumentVarargsContext varargsArgumentContext) {
        if (varargsArgumentContext == null || varargsArgumentContext.stringNullableArgument() == null) {
            return new String[0];
        }
        return varargsArgumentContext.stringNullableArgument()
                .stream()
                .filter(Objects::nonNull)
                .map(antlr.argumentVisitor::parseString)
                .toArray(String[]::new);
    }

    @Override
    public Object visitVariable(final GremlinParser.VariableContext ctx) {
        return resolver.apply(ctx.getText(), ctx);
    }
}
