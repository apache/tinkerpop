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
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.GType;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality;

import java.util.Arrays;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * Specific case of TraversalRootVisitor where all TraversalMethods returns
 * a GraphTraversal object.
 */
public class TraversalMethodVisitor extends TraversalRootVisitor<GraphTraversal> {
    /**
     * This object is used to append the traversal methods.
     */
    private final GraphTraversal.Admin graphTraversal;

    public TraversalMethodVisitor(final GremlinAntlrToJava antlr, final GraphTraversal graphTraversal) {
        super(antlr, graphTraversal);
        this.graphTraversal = graphTraversal.asAdmin();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Traversal visitTraversalMethod(final GremlinParser.TraversalMethodContext ctx) {
        return visitChildren(ctx);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_V(final GremlinParser.TraversalMethod_VContext ctx) {
        return this.graphTraversal.V(antlr.genericVisitor.parseObjectVarargs(ctx.genericLiteralVarargs()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_E(final GremlinParser.TraversalMethod_EContext ctx) {
        return this.graphTraversal.E(antlr.genericVisitor.parseObjectVarargs(ctx.genericLiteralVarargs()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_addV_Empty(final GremlinParser.TraversalMethod_addV_EmptyContext ctx) {
        return this.graphTraversal.addV();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_addV_String(final GremlinParser.TraversalMethod_addV_StringContext ctx) {
        final Object literalOrVar = antlr.argumentVisitor.visitStringArgument(ctx.stringArgument());
        if (GValue.valueInstanceOf(literalOrVar, GType.STRING)) {
            return this.graphTraversal.addV((GValue<String>) literalOrVar);
        } else {
            return this.graphTraversal.addV((String) literalOrVar);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_mergeV_Map(final GremlinParser.TraversalMethod_mergeV_MapContext ctx) {
        final Object literalOrVar = antlr.argumentVisitor.visitGenericLiteralMapNullableArgument(ctx.genericLiteralMapNullableArgument());
        if (GValue.valueInstanceOf(literalOrVar, GType.MAP))
            return graphTraversal.mergeV((GValue<Map<Object, Object>>) literalOrVar);
        else
            return graphTraversal.mergeV((Map) literalOrVar);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_mergeV_Traversal(final GremlinParser.TraversalMethod_mergeV_TraversalContext ctx) {
        return this.graphTraversal.mergeV(antlr.tvisitor.visitNestedTraversal(ctx.nestedTraversal()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Traversal visitTraversalMethod_mergeV_empty(final GremlinParser.TraversalMethod_mergeV_emptyContext ctx) {
        return this.graphTraversal.mergeV();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Traversal visitTraversalMethod_mergeE_empty(final GremlinParser.TraversalMethod_mergeE_emptyContext ctx) {
        return this.graphTraversal.mergeE();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_mergeE_Map(final GremlinParser.TraversalMethod_mergeE_MapContext ctx) {
        final Object literalOrVar = antlr.argumentVisitor.visitGenericLiteralMapNullableArgument(ctx.genericLiteralMapNullableArgument());
        if (GValue.valueInstanceOf(literalOrVar, GType.MAP))
            return graphTraversal.mergeE((GValue<Map<Object, Object>>) literalOrVar);
        else if (GValue.valueInstanceOf(literalOrVar, GType.UNKNOWN) && ((GValue) literalOrVar).get() == null)
            return graphTraversal.mergeE(GValue.ofMap(((GValue) literalOrVar).getName(), null));
        else
            return graphTraversal.mergeE((Map) literalOrVar);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_mergeE_Traversal(final GremlinParser.TraversalMethod_mergeE_TraversalContext ctx) {
        return this.graphTraversal.mergeE(antlr.tvisitor.visitNestedTraversal(ctx.nestedTraversal()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_addE_Traversal(final GremlinParser.TraversalMethod_addE_TraversalContext ctx) {
        return this.graphTraversal.addE(antlr.tvisitor.visitNestedTraversal(ctx.nestedTraversal()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_addV_Traversal(final GremlinParser.TraversalMethod_addV_TraversalContext ctx) {
        return this.graphTraversal.addV(antlr.tvisitor.visitNestedTraversal(ctx.nestedTraversal()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_addE_String(final GremlinParser.TraversalMethod_addE_StringContext ctx) {
        final Object literalOrVar = antlr.argumentVisitor.visitStringArgument(ctx.stringArgument());
        if (GValue.valueInstanceOf(literalOrVar, GType.STRING)) {
            return this.graphTraversal.addE((GValue<String>) literalOrVar);
        } else {
            return this.graphTraversal.addE((String) literalOrVar);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_aggregate_String(final GremlinParser.TraversalMethod_aggregate_StringContext ctx) {
        return graphTraversal.aggregate(antlr.genericVisitor.parseString(ctx.stringLiteral()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_aggregate_Scope_String(final GremlinParser.TraversalMethod_aggregate_Scope_StringContext ctx) {
        return graphTraversal.aggregate(
                TraversalEnumParser.parseTraversalEnumFromContext(Scope.class, ctx.traversalScope()),
                antlr.genericVisitor.parseString(ctx.stringLiteral()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_all_P(final GremlinParser.TraversalMethod_all_PContext ctx) {
        return graphTraversal.all(antlr.traversalPredicateVisitor.visitTraversalPredicate(ctx.traversalPredicate()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_and(final GremlinParser.TraversalMethod_andContext ctx) {
        return this.graphTraversal.and(
                antlr.tListVisitor.visitNestedTraversalList(ctx.nestedTraversalList()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_any_P(final GremlinParser.TraversalMethod_any_PContext ctx) {
        return graphTraversal.any(antlr.traversalPredicateVisitor.visitTraversalPredicate(ctx.traversalPredicate()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_as(final GremlinParser.TraversalMethod_asContext ctx) {
        if (ctx.getChildCount() == 4) {
            return graphTraversal.as(antlr.genericVisitor.parseString(ctx.stringLiteral()));
        } else {
            return graphTraversal.as(antlr.genericVisitor.parseString(ctx.stringLiteral()),
                    antlr.genericVisitor.parseStringVarargsLiterals(ctx.stringLiteralVarargsLiterals()));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_barrier_Consumer(final GremlinParser.TraversalMethod_barrier_ConsumerContext ctx) {
        return this.graphTraversal.barrier(TraversalEnumParser.parseTraversalEnumFromContext(SackFunctions.Barrier.class, ctx.getChild(2)));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_barrier_Empty(final GremlinParser.TraversalMethod_barrier_EmptyContext ctx) {
        return graphTraversal.barrier();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_barrier_int(final GremlinParser.TraversalMethod_barrier_intContext ctx) {
        return graphTraversal.barrier(antlr.genericVisitor.parseIntegral(ctx.integerLiteral()).intValue());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_both(final GremlinParser.TraversalMethod_bothContext ctx) {
        return graphTraversal.both(antlr.argumentVisitor.parseStringVarargs(ctx.stringLiteralVarargs()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_bothE(final GremlinParser.TraversalMethod_bothEContext ctx) {
        return graphTraversal.bothE(antlr.argumentVisitor.parseStringVarargs(ctx.stringLiteralVarargs()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_bothV(final GremlinParser.TraversalMethod_bothVContext ctx) {
        return graphTraversal.bothV();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_branch(final GremlinParser.TraversalMethod_branchContext ctx) {
        return this.graphTraversal.branch(antlr.tvisitor.visitNestedTraversal(ctx.nestedTraversal()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_by_Comparator(final GremlinParser.TraversalMethod_by_ComparatorContext ctx) {
        return graphTraversal.by(TraversalEnumParser.parseTraversalEnumFromContext(Order.class, ctx.getChild(2)));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_by_Empty(final GremlinParser.TraversalMethod_by_EmptyContext ctx) {
        return graphTraversal.by();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_by_Function(final GremlinParser.TraversalMethod_by_FunctionContext ctx) {
        return graphTraversal.by(TraversalEnumParser.parseTraversalFunctionFromContext(ctx.traversalFunction()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_by_Function_Comparator(final GremlinParser.TraversalMethod_by_Function_ComparatorContext ctx) {
        return graphTraversal.by(TraversalEnumParser.parseTraversalFunctionFromContext(ctx.traversalFunction()),
                TraversalEnumParser.parseTraversalEnumFromContext(Order.class, ctx.traversalComparator().traversalOrder()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_by_Order(final GremlinParser.TraversalMethod_by_OrderContext ctx) {
        return graphTraversal.by(TraversalEnumParser.parseTraversalEnumFromContext(Order.class, ctx.getChild(2)));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_by_String(final GremlinParser.TraversalMethod_by_StringContext ctx) {
        return graphTraversal.by(antlr.genericVisitor.parseString(ctx.stringLiteral()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_by_String_Comparator(final GremlinParser.TraversalMethod_by_String_ComparatorContext ctx) {
        return graphTraversal.by(antlr.genericVisitor.parseString(ctx.stringLiteral()),
                TraversalEnumParser.parseTraversalEnumFromContext(Order.class, ctx.getChild(4)));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_by_T(final GremlinParser.TraversalMethod_by_TContext ctx) {
        return graphTraversal.by(TraversalEnumParser.parseTraversalEnumFromContext(T.class, ctx.traversalToken()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_by_Traversal(final GremlinParser.TraversalMethod_by_TraversalContext ctx) {
        return this.graphTraversal.by(antlr.tvisitor.visitNestedTraversal(ctx.nestedTraversal()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_by_Traversal_Comparator(final GremlinParser.TraversalMethod_by_Traversal_ComparatorContext ctx) {
        return this.graphTraversal.by(antlr.tvisitor.visitNestedTraversal(ctx.nestedTraversal()),
                        TraversalEnumParser.parseTraversalEnumFromContext(Order.class, ctx.getChild(4)));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_cap(final GremlinParser.TraversalMethod_capContext ctx) {
        if (ctx.getChildCount() == 4) {
            return graphTraversal.cap(antlr.genericVisitor.parseString(ctx.stringLiteral()));
        } else {
            return graphTraversal.cap(antlr.genericVisitor.parseString(ctx.stringLiteral()),
                    antlr.genericVisitor.parseStringVarargsLiterals(ctx.stringLiteralVarargsLiterals()));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_choose_Function(final GremlinParser.TraversalMethod_choose_FunctionContext ctx) {
        return graphTraversal.choose(TraversalEnumParser.parseTraversalFunctionFromContext(ctx.traversalFunction()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_choose_Predicate_Traversal(final GremlinParser.TraversalMethod_choose_Predicate_TraversalContext ctx) {
        return graphTraversal.choose(antlr.traversalPredicateVisitor.visitTraversalPredicate(ctx.traversalPredicate()),
                antlr.tvisitor.visitNestedTraversal(ctx.nestedTraversal()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_choose_Predicate_Traversal_Traversal(final GremlinParser.TraversalMethod_choose_Predicate_Traversal_TraversalContext ctx) {
        return graphTraversal.choose(antlr.traversalPredicateVisitor.visitTraversalPredicate(ctx.traversalPredicate()),
                antlr.tvisitor.visitNestedTraversal(ctx.nestedTraversal(0)),
                antlr.tvisitor.visitNestedTraversal(ctx.nestedTraversal(1)));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_choose_Traversal(final GremlinParser.TraversalMethod_choose_TraversalContext ctx) {
        return this.graphTraversal.choose(antlr.tvisitor.visitNestedTraversal(ctx.nestedTraversal()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_choose_Traversal_Traversal(final GremlinParser.TraversalMethod_choose_Traversal_TraversalContext ctx) {
        return this.graphTraversal.choose(antlr.tvisitor.visitNestedTraversal(ctx.nestedTraversal(0)),
                antlr.tvisitor.visitNestedTraversal(ctx.nestedTraversal(1)));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_choose_Traversal_Traversal_Traversal(final GremlinParser.TraversalMethod_choose_Traversal_Traversal_TraversalContext ctx) {
        return this.graphTraversal.choose(antlr.tvisitor.visitNestedTraversal(ctx.nestedTraversal(0)),
                antlr.tvisitor.visitNestedTraversal(ctx.nestedTraversal(1)),
                antlr.tvisitor.visitNestedTraversal(ctx.nestedTraversal(2)));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_coalesce(final GremlinParser.TraversalMethod_coalesceContext ctx) {
        return this.graphTraversal.coalesce(
                antlr.tListVisitor.visitNestedTraversalList(ctx.nestedTraversalList()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_combine_Object(final GremlinParser.TraversalMethod_combine_ObjectContext ctx) {
        final Object literalOrVar = antlr.argumentVisitor.visitGenericLiteralArgument(ctx.genericLiteralArgument());
        if (literalOrVar instanceof GValue && ((GValue) literalOrVar).getType().isCollection())
            return graphTraversal.combine((GValue<Object>) literalOrVar);
        else
            return graphTraversal.combine(literalOrVar);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_coin(final GremlinParser.TraversalMethod_coinContext ctx) {
        final Object literalOrVar = antlr.argumentVisitor.visitFloatArgument(ctx.floatArgument());
        if (GValue.valueInstanceOfNumeric(literalOrVar))
            return graphTraversal.coin((GValue<Double>) literalOrVar);
        else
            return graphTraversal.coin(((Number) literalOrVar).doubleValue());

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_conjoin_String(final GremlinParser.TraversalMethod_conjoin_StringContext ctx) {
        final Object literalOrVar = antlr.argumentVisitor.visitStringArgument(ctx.stringArgument());
        if (literalOrVar instanceof String)
            return graphTraversal.conjoin((String) literalOrVar);
        else if (literalOrVar instanceof GValue && ((GValue) literalOrVar).getType() == GType.STRING)
            return graphTraversal.conjoin((GValue<String>) literalOrVar);
        else
            throw new IllegalArgumentException("conjoin argument must be a string");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_constant(final GremlinParser.TraversalMethod_constantContext ctx) {
        return graphTraversal
                .constant(antlr.argumentVisitor.visitGenericLiteralArgument(ctx.genericLiteralArgument()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_count_Empty(final GremlinParser.TraversalMethod_count_EmptyContext ctx) {
        return graphTraversal.count();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_count_Scope(final GremlinParser.TraversalMethod_count_ScopeContext ctx) {
        return graphTraversal.count(TraversalEnumParser.parseTraversalEnumFromContext(Scope.class, ctx.traversalScope()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_cyclicPath(final GremlinParser.TraversalMethod_cyclicPathContext ctx) {
        return graphTraversal.cyclicPath();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_dedup_Scope_String(final GremlinParser.TraversalMethod_dedup_Scope_StringContext ctx) {
        return graphTraversal.dedup(TraversalEnumParser.parseTraversalEnumFromContext(Scope.class, ctx.traversalScope()),
                antlr.genericVisitor.parseStringVarargsLiterals(ctx.stringLiteralVarargsLiterals()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_dedup_String(final GremlinParser.TraversalMethod_dedup_StringContext ctx) {
        return graphTraversal.dedup(antlr.genericVisitor.parseStringVarargsLiterals(ctx.stringLiteralVarargsLiterals()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_difference_Object(final GremlinParser.TraversalMethod_difference_ObjectContext ctx) {
        final Object literalOrVar = antlr.argumentVisitor.visitGenericLiteralArgument(ctx.genericLiteralArgument());
        if (literalOrVar instanceof GValue && ((GValue) literalOrVar).getType().isCollection())
            return graphTraversal.difference((GValue<Object>) literalOrVar);
        else
            return graphTraversal.difference(literalOrVar);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_disjunct_Object(final GremlinParser.TraversalMethod_disjunct_ObjectContext ctx) {
        final Object literalOrVar = antlr.argumentVisitor.visitGenericLiteralArgument(ctx.genericLiteralArgument());
        if (literalOrVar instanceof GValue && ((GValue) literalOrVar).getType().isCollection())
            return graphTraversal.disjunct((GValue<Object>) literalOrVar);
        else
            return graphTraversal.disjunct(literalOrVar);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_drop(final GremlinParser.TraversalMethod_dropContext ctx) {
        return graphTraversal.drop();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_emit_Empty(final GremlinParser.TraversalMethod_emit_EmptyContext ctx) {
        return graphTraversal.emit();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_emit_Predicate(final GremlinParser.TraversalMethod_emit_PredicateContext ctx) {
        return graphTraversal.emit(antlr.traversalPredicateVisitor.visitTraversalPredicate(ctx.traversalPredicate()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_emit_Traversal(final GremlinParser.TraversalMethod_emit_TraversalContext ctx) {
        return this.graphTraversal.emit(antlr.tvisitor.visitNestedTraversal(ctx.nestedTraversal()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Traversal visitTraversalMethod_fail_Empty(final GremlinParser.TraversalMethod_fail_EmptyContext ctx) {
        return this.graphTraversal.fail();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Traversal visitTraversalMethod_fail_String(final GremlinParser.TraversalMethod_fail_StringContext ctx) {
        return this.graphTraversal.fail(antlr.genericVisitor.parseString(ctx.stringLiteral()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_filter_Predicate(final GremlinParser.TraversalMethod_filter_PredicateContext ctx) {
        return graphTraversal.filter(antlr.traversalPredicateVisitor.visitTraversalPredicate(ctx.traversalPredicate()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_filter_Traversal(final GremlinParser.TraversalMethod_filter_TraversalContext ctx) {
        return this.graphTraversal.filter(antlr.tvisitor.visitNestedTraversal(ctx.nestedTraversal()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_flatMap(final GremlinParser.TraversalMethod_flatMapContext ctx) {
        return this.graphTraversal.flatMap(antlr.tvisitor.visitNestedTraversal(ctx.nestedTraversal()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_fold_Empty(final GremlinParser.TraversalMethod_fold_EmptyContext ctx) {
        return graphTraversal.fold();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_fold_Object_BiFunction(final GremlinParser.TraversalMethod_fold_Object_BiFunctionContext ctx) {
        return graphTraversal.fold(
                antlr.argumentVisitor.visitGenericLiteralArgument(ctx.genericLiteralArgument()),
                (BiFunction) TraversalEnumParser.parseTraversalEnumFromContext(Operator.class, ctx.getChild(4)));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Traversal visitTraversalMethod_from_String(final GremlinParser.TraversalMethod_from_StringContext ctx) {
        return graphTraversal.from(antlr.genericVisitor.parseString(ctx.stringLiteral()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Traversal visitTraversalMethod_from_Vertex(final GremlinParser.TraversalMethod_from_VertexContext ctx) {
        final Object literalOrVar = antlr.argumentVisitor.visitStructureVertexArgument(ctx.structureVertexArgument());
        if (GValue.valueInstanceOf(literalOrVar, GType.VERTEX))
            return graphTraversal.from((GValue<Vertex>) literalOrVar);
        else
            return graphTraversal.from((Vertex) literalOrVar);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_from_Traversal(final GremlinParser.TraversalMethod_from_TraversalContext ctx) {
        return this.graphTraversal.from(antlr.tvisitor.visitNestedTraversal(ctx.nestedTraversal()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_groupCount_Empty(final GremlinParser.TraversalMethod_groupCount_EmptyContext ctx) {
        return graphTraversal.groupCount();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_groupCount_String(final GremlinParser.TraversalMethod_groupCount_StringContext ctx) {
        return graphTraversal.groupCount(antlr.genericVisitor.parseString(ctx.stringLiteral()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_group_Empty(final GremlinParser.TraversalMethod_group_EmptyContext ctx) {
        return graphTraversal.group();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_group_String(final GremlinParser.TraversalMethod_group_StringContext ctx) {
        return graphTraversal.group(antlr.genericVisitor.parseString(ctx.stringLiteral()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_hasId_Object_Object(final GremlinParser.TraversalMethod_hasId_Object_ObjectContext ctx) {
        return graphTraversal.hasId(antlr.argumentVisitor.visitGenericLiteralArgument(ctx.genericLiteralArgument()),
                antlr.genericVisitor.parseObjectVarargs(ctx.genericLiteralVarargs()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_hasId_P(final GremlinParser.TraversalMethod_hasId_PContext ctx) {
        return graphTraversal.hasId(antlr.traversalPredicateVisitor.visitTraversalPredicate(ctx.traversalPredicate()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_hasKey_P(final GremlinParser.TraversalMethod_hasKey_PContext ctx) {
        return graphTraversal.hasKey(antlr.traversalPredicateVisitor.visitTraversalPredicate(ctx.traversalPredicate()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_hasKey_String_String(final GremlinParser.TraversalMethod_hasKey_String_StringContext ctx) {
        if (ctx.getChildCount() == 4) {
            return graphTraversal.hasKey(antlr.genericVisitor.parseString(ctx.stringNullableLiteral()));
        } else {
            return graphTraversal.hasKey(antlr.genericVisitor.parseString(ctx.stringNullableLiteral()),
                    antlr.genericVisitor.parseStringVarargsLiterals(ctx.stringLiteralVarargsLiterals()));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_hasLabel_P(final GremlinParser.TraversalMethod_hasLabel_PContext ctx) {
        return graphTraversal.hasLabel(antlr.traversalPredicateVisitor.visitTraversalPredicate(ctx.traversalPredicate()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_hasLabel_String_String(final GremlinParser.TraversalMethod_hasLabel_String_StringContext ctx) {
        if (ctx.getChildCount() == 4) {
            final Object literalOrVar = antlr.argumentVisitor.visitStringNullableArgument(ctx.stringNullableArgument());
            if (GValue.valueInstanceOf(literalOrVar, GType.STRING))
                return graphTraversal.hasLabel((GValue) literalOrVar);
            else
                return graphTraversal.hasLabel((String) literalOrVar);
        } else {
            Object literalOrVar = antlr.argumentVisitor.visitStringNullableArgument(ctx.stringNullableArgument());
            Object[] literalOrVars = antlr.genericVisitor.visitStringLiteralVarargs(ctx.stringLiteralVarargs());

            // if any are GValue then they all need to be GValue to call hasLabel
            if (literalOrVar instanceof GValue || Arrays.stream(literalOrVars).anyMatch(lov -> lov instanceof GValue)) {
                literalOrVar = ArgumentVisitor.asGValue(literalOrVar);
                literalOrVars = GValue.ensureGValues(literalOrVars);
            }

            // since we normalized above to gvalue or literal we can just test the first arg for gvalue-ness
            if (GValue.valueInstanceOf(literalOrVar, GType.STRING)) {
                final GValue[] gvalueLiteralOrVars = literalOrVars == null ? null : Arrays.stream(literalOrVars).map(o -> (GValue) o).toArray(GValue[]::new);
                return graphTraversal.hasLabel((GValue) literalOrVar, (GValue[]) gvalueLiteralOrVars);
            } else {
                // convert object array to string array
                final String[] stringLiteralOrVars = literalOrVars == null ? null : Arrays.stream(literalOrVars).map(o -> (String) o).toArray(String[]::new);
                return graphTraversal.hasLabel((String) literalOrVar, stringLiteralOrVars);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_hasNot(final GremlinParser.TraversalMethod_hasNotContext ctx) {
        return graphTraversal.hasNot(antlr.genericVisitor.parseString(ctx.stringNullableLiteral()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_hasValue_Object_Object(final GremlinParser.TraversalMethod_hasValue_Object_ObjectContext ctx) {
        return graphTraversal.hasValue(antlr.argumentVisitor.visitGenericLiteralArgument(ctx.genericLiteralArgument()),
                antlr.genericVisitor.parseObjectVarargs(ctx.genericLiteralVarargs()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_hasValue_P(final GremlinParser.TraversalMethod_hasValue_PContext ctx) {
        return graphTraversal.hasValue(antlr.traversalPredicateVisitor.visitTraversalPredicate(ctx.traversalPredicate()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_has_String(final GremlinParser.TraversalMethod_has_StringContext ctx) {
        return graphTraversal.has(antlr.genericVisitor.parseString(ctx.stringNullableLiteral()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_has_String_Object(final GremlinParser.TraversalMethod_has_String_ObjectContext ctx) {
        return graphTraversal.has(antlr.genericVisitor.parseString(ctx.stringNullableLiteral()),
                antlr.argumentVisitor.visitGenericLiteralArgument(ctx.genericLiteralArgument()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_has_String_P(final GremlinParser.TraversalMethod_has_String_PContext ctx) {
        return graphTraversal.has(antlr.genericVisitor.parseString(ctx.stringNullableLiteral()),
                antlr.traversalPredicateVisitor.visitTraversalPredicate(ctx.traversalPredicate()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_has_String_String_Object(final GremlinParser.TraversalMethod_has_String_String_ObjectContext ctx) {
        final Object literalOrVar = antlr.argumentVisitor.visitStringNullableArgument(ctx.stringNullableArgument());
        if (GValue.valueInstanceOf(literalOrVar, GType.STRING)) {
            return graphTraversal.has((GValue) literalOrVar,
                    antlr.genericVisitor.parseString(ctx.stringNullableLiteral()),
                    antlr.argumentVisitor.visitGenericLiteralArgument(ctx.genericLiteralArgument()));
        } else {
            return graphTraversal.has((String) literalOrVar,
                    antlr.genericVisitor.parseString(ctx.stringNullableLiteral()),
                    antlr.argumentVisitor.visitGenericLiteralArgument(ctx.genericLiteralArgument()));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_has_String_String_P(final GremlinParser.TraversalMethod_has_String_String_PContext ctx) {
        final Object literalOrVar = antlr.argumentVisitor.visitStringNullableArgument(ctx.stringNullableArgument());
        if (GValue.valueInstanceOf(literalOrVar, GType.STRING)) {
            return graphTraversal.has((GValue) literalOrVar,
                    antlr.genericVisitor.parseString(ctx.stringNullableLiteral()),
                    antlr.traversalPredicateVisitor.visitTraversalPredicate(ctx.traversalPredicate()));
        } else {
            return graphTraversal.has((String) literalOrVar,
                    antlr.genericVisitor.parseString(ctx.stringNullableLiteral()),
                    antlr.traversalPredicateVisitor.visitTraversalPredicate(ctx.traversalPredicate()));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_has_String_Traversal(final GremlinParser.TraversalMethod_has_String_TraversalContext ctx) {
        return graphTraversal.has(antlr.genericVisitor.parseString(ctx.stringNullableLiteral()),
                antlr.tvisitor.visitNestedTraversal(ctx.nestedTraversal()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_has_T_Object(final GremlinParser.TraversalMethod_has_T_ObjectContext ctx) {
        return graphTraversal.has(TraversalEnumParser.parseTraversalEnumFromContext(T.class, ctx.traversalToken()),
                antlr.argumentVisitor.visitGenericLiteralArgument(ctx.genericLiteralArgument()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_has_T_P(final GremlinParser.TraversalMethod_has_T_PContext ctx) {
        return graphTraversal.has(TraversalEnumParser.parseTraversalEnumFromContext(T.class, ctx.traversalToken()),
                antlr.traversalPredicateVisitor.visitTraversalPredicate(ctx.traversalPredicate()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_has_T_Traversal(final GremlinParser.TraversalMethod_has_T_TraversalContext ctx) {
        return graphTraversal.has(TraversalEnumParser.parseTraversalEnumFromContext(T.class, ctx.traversalToken()),
                antlr.tvisitor.visitNestedTraversal(ctx.nestedTraversal()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_id(final GremlinParser.TraversalMethod_idContext ctx) {
        return graphTraversal.id();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_identity(final GremlinParser.TraversalMethod_identityContext ctx) {
        return graphTraversal.identity();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_in(final GremlinParser.TraversalMethod_inContext ctx) {
        return graphTraversal.in(antlr.argumentVisitor.parseStringVarargs(ctx.stringLiteralVarargs()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_inE(final GremlinParser.TraversalMethod_inEContext ctx) {
        return graphTraversal.inE(antlr.argumentVisitor.parseStringVarargs(ctx.stringLiteralVarargs()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_intersect_Object(final GremlinParser.TraversalMethod_intersect_ObjectContext ctx) {
        final Object literalOrVar = antlr.argumentVisitor.visitGenericLiteralArgument(ctx.genericLiteralArgument());
        if (literalOrVar instanceof GValue && ((GValue) literalOrVar).getType().isCollection())
            return graphTraversal.intersect((GValue<Object>) literalOrVar);
        else
            return graphTraversal.intersect(literalOrVar);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_inV(final GremlinParser.TraversalMethod_inVContext ctx) {
        return graphTraversal.inV();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_inject(final GremlinParser.TraversalMethod_injectContext ctx) {
        return graphTraversal.inject(antlr.genericVisitor.parseObjectVarargs(ctx.genericLiteralVarargs()));
    }

    @Override
    public GraphTraversal visitTraversalMethod_index(final GremlinParser.TraversalMethod_indexContext ctx) {
        return graphTraversal.index();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_is_Object(final GremlinParser.TraversalMethod_is_ObjectContext ctx) {
        return graphTraversal.is(antlr.argumentVisitor.visitGenericLiteralArgument(ctx.genericLiteralArgument()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_is_P(final GremlinParser.TraversalMethod_is_PContext ctx) {
        return graphTraversal.is(antlr.traversalPredicateVisitor.visitTraversalPredicate(ctx.traversalPredicate()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_key(final GremlinParser.TraversalMethod_keyContext ctx) {
        return graphTraversal.key();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_label(final GremlinParser.TraversalMethod_labelContext ctx) {
        return graphTraversal.label();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_limit_Scope_long(final GremlinParser.TraversalMethod_limit_Scope_longContext ctx) {
        final Object literalOrVar = antlr.argumentVisitor.parseLong(ctx.integerArgument());
        if (GValue.valueInstanceOf(literalOrVar, GType.LONG)) {
            return graphTraversal.limit(TraversalEnumParser.parseTraversalEnumFromContext(Scope.class, ctx.traversalScope()),
                    (GValue<Long>) literalOrVar);
        } else {
            return graphTraversal.limit(TraversalEnumParser.parseTraversalEnumFromContext(Scope.class, ctx.traversalScope()),
                    (Long) literalOrVar);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_limit_long(final GremlinParser.TraversalMethod_limit_longContext ctx) {
        final Object literalOrVar = antlr.argumentVisitor.parseLong(ctx.integerArgument());
        if (GValue.valueInstanceOf(literalOrVar, GType.LONG)) {
            return graphTraversal.limit((GValue<Long>) literalOrVar);
        } else {
            return graphTraversal.limit((Long) literalOrVar);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_local(final GremlinParser.TraversalMethod_localContext ctx) {
        return graphTraversal.local(antlr.tvisitor.visitNestedTraversal(ctx.nestedTraversal()));
    }

    @Override
    public GraphTraversal visitTraversalMethod_loops_Empty(final GremlinParser.TraversalMethod_loops_EmptyContext ctx) {
        return graphTraversal.loops();
    }

    @Override
    public GraphTraversal visitTraversalMethod_loops_String(final GremlinParser.TraversalMethod_loops_StringContext ctx) {
        return graphTraversal.loops(antlr.genericVisitor.parseString(ctx.stringLiteral()));
    }

    @Override
    public GraphTraversal visitTraversalMethod_repeat_String_Traversal(final GremlinParser.TraversalMethod_repeat_String_TraversalContext ctx) {
        return graphTraversal.repeat((antlr.genericVisitor.parseString(ctx.stringLiteral())),
                antlr.tvisitor.visitNestedTraversal(ctx.nestedTraversal()));
    }

    @Override
    public GraphTraversal visitTraversalMethod_repeat_Traversal(final GremlinParser.TraversalMethod_repeat_TraversalContext ctx) {
        return this.graphTraversal.repeat(antlr.tvisitor.visitNestedTraversal(ctx.nestedTraversal()));
    }

    @Override
    public GraphTraversal visitTraversalMethod_read(final GremlinParser.TraversalMethod_readContext ctx) {
        return graphTraversal.read();
    }

    @Override
    public GraphTraversal visitTraversalMethod_write(final GremlinParser.TraversalMethod_writeContext ctx) {
        return graphTraversal.write();
    }

    @Override
    public GraphTraversal visitTraversalMethod_with_String(final GremlinParser.TraversalMethod_with_StringContext ctx) {
        if (ctx.withOptionKeys() != null) {
            return graphTraversal.with((String) WithOptionsVisitor.instance().visitWithOptionKeys(ctx.withOptionKeys()));
        } else {
            return graphTraversal.with(antlr.genericVisitor.parseString(ctx.stringLiteral()));
        }
    }

    @Override
    public GraphTraversal visitTraversalMethod_with_String_Object(final GremlinParser.TraversalMethod_with_String_ObjectContext ctx) {
        final String k;
        if (ctx.withOptionKeys() != null) {
            k = (String) WithOptionsVisitor.instance().visitWithOptionKeys(ctx.withOptionKeys());
        } else {
            k = antlr.genericVisitor.parseString(ctx.stringLiteral());
        }

        final Object o;
        if (ctx.withOptionsValues() != null) {
            o = WithOptionsVisitor.instance().visitWithOptionsValues(ctx.withOptionsValues());
        } else if (ctx.ioOptionsValues() != null) {
            o = WithOptionsVisitor.instance().visitIoOptionsValues(ctx.ioOptionsValues());
        } else {
            o = antlr.argumentVisitor.visitGenericLiteralArgument(ctx.genericLiteralArgument());
        }
        return graphTraversal.with(k, o);
    }

    @Override
    public GraphTraversal visitTraversalMethod_shortestPath(final GremlinParser.TraversalMethod_shortestPathContext ctx) {
        return graphTraversal.shortestPath();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_map(final GremlinParser.TraversalMethod_mapContext ctx) {
        return this.graphTraversal.map(antlr.tvisitor.visitNestedTraversal(ctx.nestedTraversal()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_match(final GremlinParser.TraversalMethod_matchContext ctx) {
        return this.graphTraversal.match(
                antlr.tListVisitor.visitNestedTraversalList(ctx.nestedTraversalList()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_max_Empty(final GremlinParser.TraversalMethod_max_EmptyContext ctx) {
        return graphTraversal.max();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_max_Scope(final GremlinParser.TraversalMethod_max_ScopeContext ctx) {
        return graphTraversal.max(TraversalEnumParser.parseTraversalEnumFromContext(Scope.class, ctx.traversalScope()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_mean_Empty(final GremlinParser.TraversalMethod_mean_EmptyContext ctx) {
        return graphTraversal.mean();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_mean_Scope(final GremlinParser.TraversalMethod_mean_ScopeContext ctx) {
        return graphTraversal.mean(TraversalEnumParser.parseTraversalEnumFromContext(Scope.class, ctx.traversalScope()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_merge_Object(final GremlinParser.TraversalMethod_merge_ObjectContext ctx) {
        final Object literalOrVar = antlr.argumentVisitor.visitGenericLiteralArgument(ctx.genericLiteralArgument());
        if (literalOrVar instanceof GValue && ((GValue) literalOrVar).getType().isCollection())
            return graphTraversal.merge((GValue<Object>) literalOrVar);
        else
            return graphTraversal.merge(literalOrVar);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_min_Empty(final GremlinParser.TraversalMethod_min_EmptyContext ctx) {
        return graphTraversal.min();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_min_Scope(final GremlinParser.TraversalMethod_min_ScopeContext ctx) {
        return graphTraversal.min(TraversalEnumParser.parseTraversalEnumFromContext(Scope.class, ctx.traversalScope()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_none_P(final GremlinParser.TraversalMethod_none_PContext ctx) {
        return graphTraversal.none(antlr.traversalPredicateVisitor.visitTraversalPredicate(ctx.traversalPredicate()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_not(final GremlinParser.TraversalMethod_notContext ctx) {
        return this.graphTraversal.not(antlr.tvisitor.visitNestedTraversal(ctx.nestedTraversal()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_option_Object_Traversal(final GremlinParser.TraversalMethod_option_Object_TraversalContext ctx) {
        final Object literalOrVar = antlr.argumentVisitor.visitGenericLiteralArgument(ctx.genericLiteralArgument());
        if (literalOrVar instanceof GValue)
            return graphTraversal.option((GValue) literalOrVar, antlr.tvisitor.visitNestedTraversal(ctx.nestedTraversal()));
        else
            return graphTraversal.option(literalOrVar, antlr.tvisitor.visitNestedTraversal(ctx.nestedTraversal()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_option_Traversal(final GremlinParser.TraversalMethod_option_TraversalContext ctx) {
        return this.graphTraversal.option(antlr.tvisitor.visitNestedTraversal(ctx.nestedTraversal()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_option_Merge_Map(final GremlinParser.TraversalMethod_option_Merge_MapContext ctx) {
        final Object literalOrVar = antlr.argumentVisitor.visitGenericLiteralMapNullableArgument(ctx.genericLiteralMapNullableArgument());
        if (GValue.valueInstanceOf(literalOrVar, GType.MAP))
            return graphTraversal.option(TraversalEnumParser.parseTraversalEnumFromContext(Merge.class, ctx.traversalMerge()), (GValue<Map>) literalOrVar);
        else
            return graphTraversal.option(TraversalEnumParser.parseTraversalEnumFromContext(Merge.class, ctx.traversalMerge()), (Map) literalOrVar);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_option_Merge_Traversal(final GremlinParser.TraversalMethod_option_Merge_TraversalContext ctx) {
        return this.graphTraversal.option(TraversalEnumParser.parseTraversalEnumFromContext(Merge.class, ctx.traversalMerge()),
                antlr.tvisitor.visitNestedTraversal(ctx.nestedTraversal()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Traversal visitTraversalMethod_option_Merge_Map_Cardinality(final GremlinParser.TraversalMethod_option_Merge_Map_CardinalityContext ctx) {
        if (ctx.nullableGenericLiteralMap().nullLiteral() != null) {
            return this.graphTraversal.option(TraversalEnumParser.parseTraversalEnumFromContext(Merge.class, ctx.traversalMerge()), (Map) null);
        }
        return graphTraversal.option(TraversalEnumParser.parseTraversalEnumFromContext(Merge.class, ctx.traversalMerge()),
                antlr.genericVisitor.parseMap(ctx.nullableGenericLiteralMap().genericLiteralMap()),
                TraversalEnumParser.parseTraversalEnumFromContext(Cardinality.class, ctx.traversalCardinality()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_optional(final GremlinParser.TraversalMethod_optionalContext ctx) {
        return this.graphTraversal.optional(antlr.tvisitor.visitNestedTraversal(ctx.nestedTraversal()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_or(final GremlinParser.TraversalMethod_orContext ctx) {
        return this.graphTraversal.or(
                antlr.tListVisitor.visitNestedTraversalList(ctx.nestedTraversalList()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_order_Empty(final GremlinParser.TraversalMethod_order_EmptyContext ctx) {
        return graphTraversal.order();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_order_Scope(final GremlinParser.TraversalMethod_order_ScopeContext ctx) {
        return graphTraversal.order(TraversalEnumParser.parseTraversalEnumFromContext(Scope.class, ctx.traversalScope()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_otherV(final GremlinParser.TraversalMethod_otherVContext ctx) {
        return graphTraversal.otherV();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_out(final GremlinParser.TraversalMethod_outContext ctx) {
        return graphTraversal.out(antlr.argumentVisitor.parseStringVarargs(ctx.stringLiteralVarargs()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_outE(final GremlinParser.TraversalMethod_outEContext ctx) {
        return graphTraversal.outE(antlr.argumentVisitor.parseStringVarargs(ctx.stringLiteralVarargs()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_outV(final GremlinParser.TraversalMethod_outVContext ctx) {
        return graphTraversal.outV();
    }

    @Override
    public Traversal visitTraversalMethod_connectedComponent(final GremlinParser.TraversalMethod_connectedComponentContext ctx) {
        return graphTraversal.connectedComponent();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_pageRank_Empty(final GremlinParser.TraversalMethod_pageRank_EmptyContext ctx) {
        return graphTraversal.pageRank();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_pageRank_double(final GremlinParser.TraversalMethod_pageRank_doubleContext ctx) {
        return graphTraversal.pageRank(((Number) antlr.argumentVisitor.visitFloatArgument(ctx.floatArgument())).doubleValue());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_path(final GremlinParser.TraversalMethod_pathContext ctx) {
        return graphTraversal.path();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_peerPressure(final GremlinParser.TraversalMethod_peerPressureContext ctx) {
        return graphTraversal.peerPressure();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_product_Object(final GremlinParser.TraversalMethod_product_ObjectContext ctx) {
        final Object literalOrVar = antlr.argumentVisitor.visitGenericLiteralArgument(ctx.genericLiteralArgument());
        if (literalOrVar instanceof GValue && ((GValue) literalOrVar).getType().isCollection())
            return graphTraversal.product((GValue<Object>) literalOrVar);
        else
            return graphTraversal.product(literalOrVar);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_profile_Empty(final GremlinParser.TraversalMethod_profile_EmptyContext ctx) {
        return graphTraversal.profile();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_profile_String(final GremlinParser.TraversalMethod_profile_StringContext ctx) {
        return graphTraversal.profile(antlr.genericVisitor.parseString(ctx.stringLiteral()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_project(final GremlinParser.TraversalMethod_projectContext ctx) {
        if (ctx.getChildCount() == 4) {
            return graphTraversal.project(antlr.genericVisitor.parseString(ctx.stringLiteral()));
        } else {
            return graphTraversal.project(antlr.genericVisitor.parseString(ctx.stringLiteral()),
                    antlr.genericVisitor.parseStringVarargsLiterals(ctx.stringLiteralVarargsLiterals()));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_properties(final GremlinParser.TraversalMethod_propertiesContext ctx) {
        return graphTraversal.properties(antlr.genericVisitor.parseStringVarargsLiterals(ctx.stringLiteralVarargsLiterals()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_elementMap(final GremlinParser.TraversalMethod_elementMapContext ctx) {
        return graphTraversal.elementMap(antlr.genericVisitor.parseStringVarargsLiterals(ctx.stringLiteralVarargsLiterals()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_propertyMap(final GremlinParser.TraversalMethod_propertyMapContext ctx) {
        return graphTraversal.propertyMap(antlr.genericVisitor.parseStringVarargsLiterals(ctx.stringLiteralVarargsLiterals()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_property_Cardinality_Object_Object_Object(final GremlinParser.TraversalMethod_property_Cardinality_Object_Object_ObjectContext ctx) {
        return graphTraversal.property(TraversalEnumParser.parseTraversalEnumFromContext(Cardinality.class, ctx.traversalCardinality()),
                antlr.argumentVisitor.visitGenericLiteralArgument(ctx.genericLiteralArgument(0)),
                antlr.argumentVisitor.visitGenericLiteralArgument(ctx.genericLiteralArgument(1)),
                antlr.genericVisitor.parseObjectVarargs(ctx.genericLiteralVarargs()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_property_Object_Object_Object(final GremlinParser.TraversalMethod_property_Object_Object_ObjectContext ctx) {
        if (ctx.getChildCount() == 6) {
            return graphTraversal.property(antlr.argumentVisitor.visitGenericLiteralArgument(ctx.genericLiteralArgument(0)),
                    antlr.argumentVisitor.visitGenericLiteralArgument(ctx.genericLiteralArgument(1)));
        } else {
            return graphTraversal.property(antlr.argumentVisitor.visitGenericLiteralArgument(ctx.genericLiteralArgument(0)),
                    antlr.argumentVisitor.visitGenericLiteralArgument(ctx.genericLiteralArgument(1)),
                    antlr.genericVisitor.parseObjectVarargs(ctx.genericLiteralVarargs()));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Traversal visitTraversalMethod_property_Cardinality_Object(final GremlinParser.TraversalMethod_property_Cardinality_ObjectContext  ctx) {
        return graphTraversal.property(TraversalEnumParser.parseTraversalEnumFromContext(Cardinality.class, ctx.traversalCardinality()),
                antlr.argumentVisitor.parseMap(ctx.genericLiteralMapNullableArgument()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Traversal visitTraversalMethod_property_Object(final GremlinParser.TraversalMethod_property_ObjectContext ctx) {
        return graphTraversal.property(antlr.argumentVisitor.parseMap(ctx.genericLiteralMapNullableArgument()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_range_Scope_long_long(final GremlinParser.TraversalMethod_range_Scope_long_longContext ctx) {
        Object low = antlr.argumentVisitor.parseLong(ctx.integerArgument(0));
        Object high = antlr.argumentVisitor.parseLong(ctx.integerArgument(1));
        if (GValue.valueInstanceOf(low, GType.LONG) || GValue.valueInstanceOf(high, GType.LONG)) {
            if (!GValue.valueInstanceOf(low, GType.LONG)) {
                low = GValue.ofLong(null, (Long) low);
            }
            if (!GValue.valueInstanceOf(high, GType.LONG)) {
                high = GValue.ofLong(null, (Long) high);
            }
            return graphTraversal.range(TraversalEnumParser.parseTraversalEnumFromContext(Scope.class, ctx.traversalScope()),
                    (GValue<Long>) low, (GValue<Long>) high);
        } else {
            return graphTraversal.range(TraversalEnumParser.parseTraversalEnumFromContext(Scope.class, ctx.traversalScope()),
                    (Long) low, (Long) high);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_range_long_long(final GremlinParser.TraversalMethod_range_long_longContext ctx) {
        Object low = antlr.argumentVisitor.parseLong(ctx.integerArgument(0));
        Object high = antlr.argumentVisitor.parseLong(ctx.integerArgument(1));
        if (GValue.valueInstanceOf(low, GType.LONG) || GValue.valueInstanceOf(high, GType.LONG)) {
            if (!GValue.valueInstanceOf(low, GType.LONG)) {
                low = GValue.ofLong(null, (Long) low);
            }
            if (!GValue.valueInstanceOf(high, GType.LONG)) {
                high = GValue.ofLong(null, (Long) high);
            }
            return graphTraversal.range((GValue<Long>) low, (GValue<Long>) high);
        } else {
            return graphTraversal.range((Long) low, (Long) high);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_sack_BiFunction(final GremlinParser.TraversalMethod_sack_BiFunctionContext ctx) {
        return graphTraversal.sack(TraversalEnumParser.parseTraversalEnumFromContext(Operator.class, ctx.getChild(2)));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_sack_Empty(final GremlinParser.TraversalMethod_sack_EmptyContext ctx) {
        return graphTraversal.sack();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_sample_Scope_int(final GremlinParser.TraversalMethod_sample_Scope_intContext ctx) {
        return graphTraversal.sample(TraversalEnumParser.parseTraversalEnumFromContext(Scope.class, ctx.traversalScope()),
                antlr.genericVisitor.parseIntegral(ctx.integerLiteral()).intValue());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_sample_int(final GremlinParser.TraversalMethod_sample_intContext ctx) {
        return graphTraversal.sample(antlr.genericVisitor.parseIntegral(ctx.integerLiteral()).intValue());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_select_Column(final GremlinParser.TraversalMethod_select_ColumnContext ctx) {
        return graphTraversal.select(TraversalEnumParser.parseTraversalEnumFromContext(Column.class, ctx.traversalColumn()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_select_Pop_String(final GremlinParser.TraversalMethod_select_Pop_StringContext ctx) {
        return graphTraversal.select(TraversalEnumParser.parseTraversalEnumFromContext(Pop.class, ctx.traversalPop()),
                antlr.genericVisitor.parseString(ctx.stringLiteral()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_select_Pop_String_String_String(final GremlinParser.TraversalMethod_select_Pop_String_String_StringContext ctx) {
        return graphTraversal.select(TraversalEnumParser.parseTraversalEnumFromContext(Pop.class, ctx.traversalPop()),
                antlr.genericVisitor.parseString(ctx.stringLiteral(0)),
                antlr.genericVisitor.parseString(ctx.stringLiteral(1)),
                antlr.genericVisitor.parseStringVarargsLiterals(ctx.stringLiteralVarargsLiterals()));
    }

    @Override
    public GraphTraversal visitTraversalMethod_select_Pop_Traversal(final GremlinParser.TraversalMethod_select_Pop_TraversalContext ctx) {
        return graphTraversal.select(TraversalEnumParser.parseTraversalEnumFromContext(Pop.class, ctx.traversalPop()),
                antlr.tvisitor.visitNestedTraversal(ctx.nestedTraversal()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_select_String(final GremlinParser.TraversalMethod_select_StringContext ctx) {
        return graphTraversal.select(antlr.genericVisitor.parseString(ctx.stringLiteral()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_select_String_String_String(final GremlinParser.TraversalMethod_select_String_String_StringContext ctx) {
        return graphTraversal.select(antlr.genericVisitor.parseString(ctx.stringLiteral(0)),
                antlr.genericVisitor.parseString(ctx.stringLiteral(1)),
                antlr.genericVisitor.parseStringVarargsLiterals(ctx.stringLiteralVarargsLiterals()));
    }

    @Override
    public GraphTraversal visitTraversalMethod_select_Traversal(final GremlinParser.TraversalMethod_select_TraversalContext ctx) {
        return graphTraversal.select(antlr.tvisitor.visitNestedTraversal(ctx.nestedTraversal()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_sideEffect(final GremlinParser.TraversalMethod_sideEffectContext ctx) {
        return this.graphTraversal.sideEffect(antlr.tvisitor.visitNestedTraversal(ctx.nestedTraversal()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_simplePath(final GremlinParser.TraversalMethod_simplePathContext ctx) {
        return graphTraversal.simplePath();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_skip_Scope_long(final GremlinParser.TraversalMethod_skip_Scope_longContext ctx) {
        final Object literalOrVar = antlr.argumentVisitor.parseLong(ctx.integerArgument());
        if (GValue.valueInstanceOf(literalOrVar, GType.LONG)) {
            return graphTraversal.skip(TraversalEnumParser.parseTraversalEnumFromContext(Scope.class, ctx.traversalScope()),
                    (GValue<Long>) literalOrVar);
        } else {
            return graphTraversal.skip(TraversalEnumParser.parseTraversalEnumFromContext(Scope.class, ctx.traversalScope()),
                    (Long) literalOrVar);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_skip_long(final GremlinParser.TraversalMethod_skip_longContext ctx) {
        final Object literalOrVar = antlr.argumentVisitor.parseLong(ctx.integerArgument());
        if (GValue.valueInstanceOf(literalOrVar, GType.LONG)) {
            return graphTraversal.skip((GValue<Long>) literalOrVar);
        } else {
            return graphTraversal.skip((Long) literalOrVar);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_store(final GremlinParser.TraversalMethod_storeContext ctx) {
        return graphTraversal.store(antlr.genericVisitor.parseString(ctx.stringLiteral()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_subgraph(final GremlinParser.TraversalMethod_subgraphContext ctx) {
        return graphTraversal.subgraph(antlr.genericVisitor.parseString(ctx.stringLiteral()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_sum_Empty(final GremlinParser.TraversalMethod_sum_EmptyContext ctx) {
        return graphTraversal.sum();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_sum_Scope(final GremlinParser.TraversalMethod_sum_ScopeContext ctx) {
        return graphTraversal.sum(TraversalEnumParser.parseTraversalEnumFromContext(Scope.class, ctx.traversalScope()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_tail_Empty(final GremlinParser.TraversalMethod_tail_EmptyContext ctx) {
        return graphTraversal.tail();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_tail_Scope(final GremlinParser.TraversalMethod_tail_ScopeContext ctx) {
        return graphTraversal.tail(TraversalEnumParser.parseTraversalEnumFromContext(Scope.class, ctx.traversalScope()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_tail_Scope_long(final GremlinParser.TraversalMethod_tail_Scope_longContext ctx) {
        final Object literalOrVar = antlr.argumentVisitor.parseLong(ctx.integerArgument());
        if (GValue.valueInstanceOf(literalOrVar, GType.LONG)) {
            return graphTraversal.tail(TraversalEnumParser.parseTraversalEnumFromContext(Scope.class, ctx.traversalScope()),
                    (GValue<Long>) literalOrVar);
        } else {
            return graphTraversal.tail(TraversalEnumParser.parseTraversalEnumFromContext(Scope.class, ctx.traversalScope()),
                    (Long) literalOrVar);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_tail_long(final GremlinParser.TraversalMethod_tail_longContext ctx) {
        final Object literalOrVar = antlr.argumentVisitor.parseLong(ctx.integerArgument());
        if (GValue.valueInstanceOf(literalOrVar, GType.LONG)) {
            return graphTraversal.tail((GValue<Long>) literalOrVar);
        } else {
            return graphTraversal.tail((Long) literalOrVar);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_timeLimit(final GremlinParser.TraversalMethod_timeLimitContext ctx) {
        return graphTraversal.timeLimit(antlr.genericVisitor.parseIntegral(ctx.integerLiteral()).longValue());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_times(final GremlinParser.TraversalMethod_timesContext ctx) {
        return graphTraversal.times(antlr.genericVisitor.parseIntegral(ctx.integerLiteral()).intValue());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_toE(final GremlinParser.TraversalMethod_toEContext ctx) {
        return graphTraversal.toE(TraversalEnumParser.parseTraversalEnumFromContext(Direction.class, ctx.traversalDirection()),
                antlr.argumentVisitor.parseStringVarargs(ctx.stringLiteralVarargs()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_toV(final GremlinParser.TraversalMethod_toVContext ctx) {
        return graphTraversal.toV(TraversalEnumParser.parseTraversalEnumFromContext(Direction.class, ctx.traversalDirection()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_to_Direction_String(final GremlinParser.TraversalMethod_to_Direction_StringContext ctx) {
        return graphTraversal.to(TraversalEnumParser.parseTraversalDirectionFromContext(ctx.traversalDirection()),
                antlr.argumentVisitor.parseStringVarargs(ctx.stringLiteralVarargs()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Traversal visitTraversalMethod_to_String(final GremlinParser.TraversalMethod_to_StringContext ctx) {
        return graphTraversal.to(antlr.genericVisitor.parseString(ctx.stringLiteral()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Traversal visitTraversalMethod_to_Vertex(final GremlinParser.TraversalMethod_to_VertexContext ctx) {
        final Object literalOrVar = antlr.argumentVisitor.visitStructureVertexArgument(ctx.structureVertexArgument());
        if (GValue.valueInstanceOf(literalOrVar, GType.VERTEX))
            return graphTraversal.to((GValue<Vertex>) literalOrVar);
        else
            return graphTraversal.to((Vertex) literalOrVar);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_to_Traversal(final GremlinParser.TraversalMethod_to_TraversalContext ctx) {
        return this.graphTraversal.to(antlr.tvisitor.visitNestedTraversal(ctx.nestedTraversal()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_tree_Empty(final GremlinParser.TraversalMethod_tree_EmptyContext ctx) {
        return graphTraversal.tree();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_tree_String(final GremlinParser.TraversalMethod_tree_StringContext ctx) {
        return graphTraversal.tree(antlr.genericVisitor.parseString(ctx.stringLiteral()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_unfold(final GremlinParser.TraversalMethod_unfoldContext ctx) {
        return graphTraversal.unfold();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_union(final GremlinParser.TraversalMethod_unionContext ctx) {
        return this.graphTraversal.union(
                antlr.tListVisitor.visitNestedTraversalList(ctx.nestedTraversalList()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_until_Predicate(final GremlinParser.TraversalMethod_until_PredicateContext ctx) {
        return graphTraversal.until(antlr.traversalPredicateVisitor.visitTraversalPredicate(ctx.traversalPredicate()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_until_Traversal(final GremlinParser.TraversalMethod_until_TraversalContext ctx) {
        return this.graphTraversal.until(antlr.tvisitor.visitNestedTraversal(ctx.nestedTraversal()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_value(final GremlinParser.TraversalMethod_valueContext ctx) {
        return graphTraversal.value();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_valueMap_String(final GremlinParser.TraversalMethod_valueMap_StringContext ctx) {
        return graphTraversal.valueMap(antlr.genericVisitor.parseStringVarargsLiterals(ctx.stringLiteralVarargsLiterals()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_valueMap_boolean_String(final GremlinParser.TraversalMethod_valueMap_boolean_StringContext ctx) {
        if (ctx.getChildCount() == 4) {
            return graphTraversal.valueMap(antlr.genericVisitor.parseBoolean(ctx.booleanLiteral()));
        } else {
            return graphTraversal.valueMap(antlr.genericVisitor.parseBoolean(ctx.booleanLiteral()),
                    antlr.genericVisitor.parseStringVarargsLiterals(ctx.stringLiteralVarargsLiterals()));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_values(final GremlinParser.TraversalMethod_valuesContext ctx) {
        return graphTraversal.values(antlr.genericVisitor.parseStringVarargsLiterals(ctx.stringLiteralVarargsLiterals()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_where_P(final GremlinParser.TraversalMethod_where_PContext ctx) {
        return graphTraversal.where(antlr.traversalPredicateVisitor.visitTraversalPredicate(ctx.traversalPredicate()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_where_String_P(final GremlinParser.TraversalMethod_where_String_PContext ctx) {
        return graphTraversal.where(antlr.genericVisitor.parseString(ctx.stringLiteral()),
                antlr.traversalPredicateVisitor.visitTraversalPredicate(ctx.traversalPredicate()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_where_Traversal(final GremlinParser.TraversalMethod_where_TraversalContext ctx) {
        return this.graphTraversal.where(antlr.tvisitor.visitNestedTraversal(ctx.nestedTraversal()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_math(final GremlinParser.TraversalMethod_mathContext ctx) {
        return graphTraversal.math(antlr.genericVisitor.parseString(ctx.stringLiteral()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Traversal visitTraversalMethod_option_Predicate_Traversal(final GremlinParser.TraversalMethod_option_Predicate_TraversalContext ctx) {
        return graphTraversal.option(antlr.traversalPredicateVisitor.visitTraversalPredicate(ctx.traversalPredicate()),
                antlr.tvisitor.visitNestedTraversal(ctx.nestedTraversal()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Traversal visitTraversalMethod_element(final GremlinParser.TraversalMethod_elementContext ctx) {
        return graphTraversal.element();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Traversal visitTraversalMethod_call_string(final GremlinParser.TraversalMethod_call_stringContext ctx) {
        return graphTraversal.call(antlr.genericVisitor.parseString(ctx.stringLiteral()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Traversal visitTraversalMethod_call_string_map(final GremlinParser.TraversalMethod_call_string_mapContext ctx) {
        final Object literalOrVar = antlr.argumentVisitor.visitGenericLiteralMapArgument(ctx.genericLiteralMapArgument());
        if (GValue.valueInstanceOf(literalOrVar, GType.MAP))
            return graphTraversal.call(antlr.genericVisitor.parseString(ctx.stringLiteral()), (GValue<Map>) literalOrVar);
        else
            return graphTraversal.call(antlr.genericVisitor.parseString(ctx.stringLiteral()), (Map) literalOrVar);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Traversal visitTraversalMethod_call_string_traversal(final GremlinParser.TraversalMethod_call_string_traversalContext ctx) {
        return graphTraversal.call(antlr.genericVisitor.parseString(ctx.stringLiteral()),
                antlr.tvisitor.visitNestedTraversal(ctx.nestedTraversal()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Traversal visitTraversalMethod_call_string_map_traversal(final GremlinParser.TraversalMethod_call_string_map_traversalContext ctx) {
        final Object literalOrVar = antlr.argumentVisitor.visitGenericLiteralMapArgument(ctx.genericLiteralMapArgument());
        if (GValue.valueInstanceOf(literalOrVar, GType.MAP))
            return graphTraversal.call(antlr.genericVisitor.parseString(ctx.stringLiteral()), (GValue<Map>) literalOrVar, antlr.tvisitor.visitNestedTraversal(ctx.nestedTraversal()));
        else
            return graphTraversal.call(antlr.genericVisitor.parseString(ctx.stringLiteral()), (Map) literalOrVar, antlr.tvisitor.visitNestedTraversal(ctx.nestedTraversal()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_concat_Traversal_Traversal(final GremlinParser.TraversalMethod_concat_Traversal_TraversalContext ctx) {
        if (ctx.getChildCount() == 4) {
            return this.graphTraversal.concat(antlr.tvisitor.visitNestedTraversal(ctx.nestedTraversal()));
        }
        return this.graphTraversal.concat(antlr.tvisitor.visitNestedTraversal(ctx.nestedTraversal()),
                antlr.tListVisitor.visitNestedTraversalList(ctx.nestedTraversalList()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_concat_String(final GremlinParser.TraversalMethod_concat_StringContext ctx) {
        return graphTraversal.concat(antlr.genericVisitor.parseStringVarargsLiterals(ctx.stringLiteralVarargsLiterals()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_format_String(GremlinParser.TraversalMethod_format_StringContext ctx)  {
        return graphTraversal.format(antlr.genericVisitor.parseString(ctx.stringLiteral()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_asString_Empty(final GremlinParser.TraversalMethod_asString_EmptyContext ctx) {
        return graphTraversal.asString();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_asString_Scope(final GremlinParser.TraversalMethod_asString_ScopeContext ctx) {
        return graphTraversal.asString(TraversalEnumParser.parseTraversalEnumFromContext(Scope.class, ctx.traversalScope()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_toUpper_Empty(final GremlinParser.TraversalMethod_toUpper_EmptyContext ctx) {
        return graphTraversal.toUpper();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_toUpper_Scope(final GremlinParser.TraversalMethod_toUpper_ScopeContext ctx) {
        return graphTraversal.toUpper(TraversalEnumParser.parseTraversalEnumFromContext(Scope.class, ctx.traversalScope()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_toLower_Empty(final GremlinParser.TraversalMethod_toLower_EmptyContext ctx) {
        return graphTraversal.toLower();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_toLower_Scope(final GremlinParser.TraversalMethod_toLower_ScopeContext ctx) {
        return graphTraversal.toLower(TraversalEnumParser.parseTraversalEnumFromContext(Scope.class, ctx.traversalScope()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_length_Empty(final GremlinParser.TraversalMethod_length_EmptyContext ctx) {
        return graphTraversal.length();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_length_Scope(final GremlinParser.TraversalMethod_length_ScopeContext ctx) {
        return graphTraversal.length(TraversalEnumParser.parseTraversalEnumFromContext(Scope.class, ctx.traversalScope()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_trim_Empty(final GremlinParser.TraversalMethod_trim_EmptyContext ctx) {
        return graphTraversal.trim();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_trim_Scope(final GremlinParser.TraversalMethod_trim_ScopeContext ctx) {
        return graphTraversal.trim(TraversalEnumParser.parseTraversalEnumFromContext(Scope.class, ctx.traversalScope()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_lTrim_Empty(final GremlinParser.TraversalMethod_lTrim_EmptyContext ctx) {
        return graphTraversal.lTrim();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_lTrim_Scope(final GremlinParser.TraversalMethod_lTrim_ScopeContext ctx) {
        return graphTraversal.lTrim(TraversalEnumParser.parseTraversalEnumFromContext(Scope.class, ctx.traversalScope()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_rTrim_Empty(final GremlinParser.TraversalMethod_rTrim_EmptyContext ctx) {
        return graphTraversal.rTrim();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_rTrim_Scope(final GremlinParser.TraversalMethod_rTrim_ScopeContext ctx) {
        return graphTraversal.rTrim(TraversalEnumParser.parseTraversalEnumFromContext(Scope.class, ctx.traversalScope()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_reverse_Empty(final GremlinParser.TraversalMethod_reverse_EmptyContext ctx) {
        return graphTraversal.reverse();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_replace_String_String(final GremlinParser.TraversalMethod_replace_String_StringContext ctx) {
        return graphTraversal.replace(antlr.genericVisitor.parseString(ctx.stringNullableLiteral(0)),
                antlr.genericVisitor.parseString(ctx.stringNullableLiteral(1)));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_replace_Scope_String_String(final GremlinParser.TraversalMethod_replace_Scope_String_StringContext ctx) {
        return graphTraversal.replace(TraversalEnumParser.parseTraversalEnumFromContext(Scope.class, ctx.traversalScope()),
                antlr.genericVisitor.parseString(ctx.stringNullableLiteral(0)),
                antlr.genericVisitor.parseString(ctx.stringNullableLiteral(1)));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_split_String(final GremlinParser.TraversalMethod_split_StringContext ctx) {
        return graphTraversal.split(antlr.genericVisitor.parseString(ctx.stringNullableLiteral()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_split_Scope_String(final GremlinParser.TraversalMethod_split_Scope_StringContext ctx) {
        return graphTraversal.split(TraversalEnumParser.parseTraversalEnumFromContext(Scope.class, ctx.traversalScope()),
                antlr.genericVisitor.parseString(ctx.stringNullableLiteral()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_substring_int(final GremlinParser.TraversalMethod_substring_intContext ctx) {
        return graphTraversal.substring(antlr.genericVisitor.parseIntegral(ctx.integerLiteral()).intValue());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_substring_Scope_int(final GremlinParser.TraversalMethod_substring_Scope_intContext ctx) {
        return graphTraversal.substring(TraversalEnumParser.parseTraversalEnumFromContext(Scope.class, ctx.traversalScope()),
                antlr.genericVisitor.parseIntegral(ctx.integerLiteral()).intValue());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_substring_int_int(final GremlinParser.TraversalMethod_substring_int_intContext ctx) {
        return graphTraversal.substring(antlr.genericVisitor.parseIntegral(ctx.integerLiteral(0)).intValue(),
                antlr.genericVisitor.parseIntegral(ctx.integerLiteral(1)).intValue());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_substring_Scope_int_int(final GremlinParser.TraversalMethod_substring_Scope_int_intContext ctx) {
        return graphTraversal.substring(TraversalEnumParser.parseTraversalEnumFromContext(Scope.class, ctx.traversalScope()),
                antlr.genericVisitor.parseIntegral(ctx.integerLiteral(0)).intValue(),
                antlr.genericVisitor.parseIntegral(ctx.integerLiteral(1)).intValue());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_asDate(final GremlinParser.TraversalMethod_asDateContext ctx) {
        return graphTraversal.asDate();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_dateAdd(final GremlinParser.TraversalMethod_dateAddContext ctx) {
        return graphTraversal.dateAdd(TraversalEnumParser.parseTraversalEnumFromContext(DT.class, ctx.traversalDT()),
                antlr.genericVisitor.parseIntegral(ctx.integerLiteral()).intValue());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_dateDiff_Traversal(final GremlinParser.TraversalMethod_dateDiff_TraversalContext ctx) {
        return graphTraversal.dateDiff(antlr.tvisitor.visitNestedTraversal(ctx.nestedTraversal()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_dateDiff_Date(final GremlinParser.TraversalMethod_dateDiff_DateContext ctx) {
        return graphTraversal.dateDiff(antlr.genericVisitor.parseDate(ctx.dateLiteral()));
    }


    public GraphTraversal[] getNestedTraversalList(final GremlinParser.NestedTraversalListContext ctx) {
        return ctx.nestedTraversalExpr().nestedTraversal()
                .stream()
                .map(this::visitNestedTraversal)
                .toArray(GraphTraversal[]::new);
    }
}
