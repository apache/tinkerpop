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

import org.apache.tinkerpop.gremlin.process.traversal.DT;
import org.apache.tinkerpop.gremlin.process.traversal.Merge;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.SackFunctions;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.lang.reflect.Array;
import java.time.OffsetDateTime;
import java.util.Comparator;
import java.util.Map;
import java.util.function.Function;

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
    public Object parseObject(final GremlinParser.GenericLiteralArgumentContext ctx) {
        return visitGenericLiteralArgument(ctx);
    }

    /**
     * Wrapper for visit function for {@link Direction} types.
     */
    public Direction parseDirection(final GremlinParser.TraversalDirectionArgumentContext ctx) {
        return (Direction) visitTraversalDirectionArgument(ctx);
    }

    /**
     * Wrapper for visit function for {@link Vertex} types.
     */
    public Vertex parseVertex(final GremlinParser.StructureVertexArgumentContext ctx) {
        return (Vertex) visitStructureVertexArgument(ctx);
    }

    /**
     * Wrapper for visit function for {@link Order} types.
     */
    public Order parseOrder(final GremlinParser.TraversalOrderArgumentContext ctx) {
        return (Order) visitTraversalOrderArgument(ctx);
    }

    /**
     * Wrapper for visit function for {@link Scope} types.
     */
    public Scope parseScope(final GremlinParser.TraversalScopeArgumentContext ctx) {
        return (Scope) visitTraversalScopeArgument(ctx);
    }

    /**
     * Wrapper for visit function for {@link T} types.
     */
    public T parseT(final GremlinParser.TraversalTokenArgumentContext ctx) {
        return (T) visitTraversalTokenArgument(ctx);
    }

    /**
     * Wrapper for visit function for {@link VertexProperty.Cardinality} types.
     */
    public VertexProperty.Cardinality parseCardinality(final GremlinParser.TraversalCardinalityArgumentContext ctx) {
        return (VertexProperty.Cardinality) visitTraversalCardinalityArgument(ctx);
    }

    /**
     * Wrapper for visit function for {@link Merge} types.
     */
    public Merge parseMerge(final GremlinParser.TraversalMergeArgumentContext ctx) {
        return (Merge) visitTraversalMergeArgument(ctx);
    }

    /**
     * Wrapper for visit function for {@link Pop} types.
     */
    public Pop parsePop(final GremlinParser.TraversalPopArgumentContext ctx) {
        return (Pop) visitTraversalPopArgument(ctx);
    }

    /**
     * Wrapper for visit function for {@link DT} types.
     */
    public DT parseDT(final GremlinParser.TraversalDTArgumentContext ctx) {
        return (DT) visitTraversalDTArgument(ctx);
    }

    /**
     * Wrapper for visit function for {@link Pop} types.
     */
    public Column parseColumn(final GremlinParser.TraversalColumnArgumentContext ctx) {
        return (Column) visitTraversalColumnArgument(ctx);
    }

    /**
     * Wrapper for visit function for {@code Function} types like {@link T} and {@link Column}.
     */
    public Function parseFunction(final GremlinParser.TraversalFunctionArgumentContext ctx) {
        return (Function) visitTraversalFunctionArgument(ctx);
    }

    /**
     * Wrapper for visit function for {@code Comparator} types like {@link Order}.
     */
    public Comparator parseComparator(final GremlinParser.TraversalComparatorArgumentContext ctx) {
        return (Comparator) visitTraversalComparatorArgument(ctx);
    }

    /**
     * Wrapper for visit function for {@code Map} types.
     */
    public Map parseMap(final GremlinParser.GenericLiteralMapArgumentContext ctx) {
        return (Map) visitGenericLiteralMapArgument(ctx);
    }

    /**
     * Wrapper for visit function for {@code Map} types.
     */
    public Map parseMap(final GremlinParser.GenericLiteralMapNullableArgumentContext ctx) {
        return (Map) visitGenericLiteralMapNullableArgument(ctx);
    }

    /**
     * Wrapper for visit function for list types.
     */
    public Object[] parseObjectVarargs(final GremlinParser.GenericLiteralListArgumentContext ctx) {
        if (ctx.genericLiteralList() != null) {
            return antlr.genericVisitor.parseObjectList(ctx.genericLiteralList());
        } else {
            final Object l = visitVariable(ctx.variable());
            if (null == l) {
                return null;
            } else if (l.getClass().isArray()) {
                final int length = Array.getLength(l);
                final Object[] result = new Object[length];
                for (int i = 0; i < length; i++) {
                    result[i] = Array.get(l, i);
                }
                return result;
            } else if (l instanceof Iterable) {
                return IteratorUtils.list(((Iterable<?>) l).iterator()).toArray();
            } else {
                return new Object[] { l };
            }
        }
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
    public Object visitGenericLiteralArgument(final GremlinParser.GenericLiteralArgumentContext ctx) {
        if (ctx.genericLiteral() != null) {
            return antlr.genericVisitor.visitGenericLiteral(ctx.genericLiteral());
        } else {
            return visitVariable(ctx.variable());
        }
    }

    @Override
    public Object visitGenericLiteralListArgument(final GremlinParser.GenericLiteralListArgumentContext ctx) {
        if (ctx.genericLiteralList() != null) {
            return antlr.genericVisitor.visitChildren(ctx.genericLiteralList());
        } else {
            return visitVariable(ctx.variable());
        }
    }

    @Override
    public Object visitTraversalDirectionArgument(final GremlinParser.TraversalDirectionArgumentContext ctx) {
        if (ctx.traversalDirection() != null) {
            return TraversalEnumParser.parseTraversalDirectionFromContext(ctx.traversalDirection());
        } else {
            return visitVariable(ctx.variable());
        }
    }

    @Override
    public Object visitStructureVertexArgument(final GremlinParser.StructureVertexArgumentContext ctx) {
        if (ctx.structureVertex() != null) {
            return antlr.structureVisitor.visitStructureVertex(ctx.structureVertex());
        } else {
            return visitVariable(ctx.variable());
        }
    }

    @Override
    public Object visitTraversalOrderArgument(final GremlinParser.TraversalOrderArgumentContext ctx) {
        if (ctx.traversalOrder() != null) {
            return TraversalEnumParser.parseTraversalEnumFromContext(Order.class, ctx.traversalOrder());
        } else {
            return visitVariable(ctx.variable());
        }
    }

    @Override
    public Object visitTraversalScopeArgument(final GremlinParser.TraversalScopeArgumentContext ctx) {
        if (ctx.traversalScope() != null) {
            return TraversalEnumParser.parseTraversalEnumFromContext(Scope.class, ctx.traversalScope());
        } else {
            return visitVariable(ctx.variable());
        }
    }

    @Override
    public Object visitTraversalTokenArgument(final GremlinParser.TraversalTokenArgumentContext ctx) {
        if (ctx.traversalToken() != null) {
            return TraversalEnumParser.parseTraversalEnumFromContext(T.class, ctx.traversalToken());
        } else {
            return visitVariable(ctx.variable());
        }
    }

    @Override
    public Object visitTraversalCardinalityArgument(final GremlinParser.TraversalCardinalityArgumentContext ctx) {
        if (ctx.traversalCardinality() != null) {
            return TraversalEnumParser.parseTraversalEnumFromContext(VertexProperty.Cardinality.class, ctx.traversalCardinality());
        } else {
            return visitVariable(ctx.variable());
        }
    }

    @Override
    public Object visitTraversalMergeArgument(final GremlinParser.TraversalMergeArgumentContext ctx) {
        if (ctx.traversalMerge() != null) {
            return TraversalEnumParser.parseTraversalEnumFromContext(Merge.class, ctx.traversalMerge());
        } else {
            return visitVariable(ctx.variable());
        }
    }

    @Override
    public Object visitTraversalPopArgument(final GremlinParser.TraversalPopArgumentContext ctx) {
        if (ctx.traversalPop() != null) {
            return TraversalEnumParser.parseTraversalEnumFromContext(Pop.class, ctx.traversalPop());
        } else {
            return visitVariable(ctx.variable());
        }
    }

    @Override
    public Object visitTraversalDTArgument(final GremlinParser.TraversalDTArgumentContext ctx) {
        if (ctx.traversalDT() != null) {
            return TraversalEnumParser.parseTraversalEnumFromContext(DT.class, ctx.traversalDT());
        } else {
            return visitVariable(ctx.variable());
        }
    }

    @Override
    public Object visitTraversalColumnArgument(final GremlinParser.TraversalColumnArgumentContext ctx) {
        if (ctx.traversalColumn() != null) {
            return TraversalEnumParser.parseTraversalEnumFromContext(Column.class, ctx.traversalColumn());
        } else {
            return visitVariable(ctx.variable());
        }
    }

    @Override
    public Object visitTraversalComparatorArgument(final GremlinParser.TraversalComparatorArgumentContext ctx) {
        if (ctx.traversalComparator() != null) {
            return TraversalEnumParser.parseTraversalEnumFromContext(Order.class, ctx.traversalComparator().traversalOrder());
        } else {
            return visitVariable(ctx.variable());
        }
    }

    @Override
    public Object visitTraversalFunctionArgument(final GremlinParser.TraversalFunctionArgumentContext ctx) {
        if (ctx.traversalFunction() != null) {
            final GremlinParser.TraversalFunctionContext tfc = ctx.traversalFunction();
            if (tfc.traversalToken() != null) {
                return TraversalEnumParser.parseTraversalEnumFromContext(T.class, tfc.traversalToken());
            } else if (tfc.traversalColumn() != null)
                return TraversalEnumParser.parseTraversalEnumFromContext(Column.class, tfc.traversalColumn());
            else {
                throw new GremlinParserException("Unrecognized enum for traversal function");
            }
        } else {
            return visitVariable(ctx.variable());
        }
    }

    @Override
    public Object visitTraversalBiFunctionArgument(final GremlinParser.TraversalBiFunctionArgumentContext ctx) {
        if (ctx.traversalBiFunction() != null) {
            return TraversalEnumParser.parseTraversalEnumFromContext(Operator.class, ctx.traversalBiFunction().traversalOperator());
        } else {
            return visitVariable(ctx.variable());
        }
    }

    @Override
    public Object visitGenericLiteralMapArgument(final GremlinParser.GenericLiteralMapArgumentContext ctx) {
        if (ctx.genericLiteralMap() != null) {
            return antlr.genericVisitor.visitGenericLiteralMap(ctx.genericLiteralMap());
        } else {
            return visitVariable(ctx.variable());
        }
    }

    @Override
    public Object visitGenericLiteralMapNullableArgument(final GremlinParser.GenericLiteralMapNullableArgumentContext ctx) {
        if (ctx.nullLiteral() != null) {
            return null;
        } else if (ctx.genericLiteralMap() != null) {
            return antlr.genericVisitor.visitGenericLiteralMap(ctx.genericLiteralMap());
        } else {
            return visitVariable(ctx.variable());
        }
    }

    @Override
    public Object visitTraversalSackMethodArgument(final GremlinParser.TraversalSackMethodArgumentContext ctx) {
        if (ctx.traversalSackMethod() != null) {
            return TraversalEnumParser.parseTraversalEnumFromContext(SackFunctions.Barrier.class, ctx.traversalSackMethod());
        } else {
            return visitVariable(ctx.variable());
        }
    }

    @Override
    public Object visitVariable(final GremlinParser.VariableContext ctx) {
        return resolver.apply(ctx.getText(), ctx);
    }
}
