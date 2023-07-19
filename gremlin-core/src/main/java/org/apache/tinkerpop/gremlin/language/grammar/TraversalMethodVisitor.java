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

import org.apache.tinkerpop.gremlin.process.traversal.Merge;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality;

import java.util.LinkedHashMap;
import java.util.Map;

import java.util.function.BiFunction;

import static org.apache.tinkerpop.gremlin.process.traversal.SackFunctions.Barrier.normSack;

/**
 * Specific case of TraversalRootVisitor where all TraversalMethods returns
 * a GraphTraversal object.
 */
public class TraversalMethodVisitor extends TraversalRootVisitor<GraphTraversal> {
    /**
     * This object is used to append the traversal methods.
     */
    private final GraphTraversal graphTraversal;

    public TraversalMethodVisitor(final GremlinAntlrToJava antlr, final GraphTraversal graphTraversal) {
        super(antlr, graphTraversal);
        this.graphTraversal = graphTraversal;
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
        return this.graphTraversal.addV(antlr.argumentVisitor.parseString(ctx.stringArgument()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_mergeV_Map(final GremlinParser.TraversalMethod_mergeV_MapContext ctx) {
        return this.graphTraversal.mergeV(antlr.argumentVisitor.parseMap(ctx.genericLiteralMapNullableArgument()));
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
        return this.graphTraversal.mergeE(antlr.argumentVisitor.parseMap(ctx.genericLiteralMapNullableArgument()));
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
        return this.graphTraversal.addE(antlr.argumentVisitor.parseString(ctx.stringArgument()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_aggregate_String(final GremlinParser.TraversalMethod_aggregate_StringContext ctx) {
        return graphTraversal.aggregate(antlr.argumentVisitor.parseString(ctx.stringArgument()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_aggregate_Scope_String(final GremlinParser.TraversalMethod_aggregate_Scope_StringContext ctx) {
        return graphTraversal.aggregate(
                antlr.argumentVisitor.parseScope(ctx.traversalScopeArgument()),
                antlr.argumentVisitor.parseString(ctx.stringArgument()));
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
    public GraphTraversal visitTraversalMethod_as(final GremlinParser.TraversalMethod_asContext ctx) {
        if (ctx.getChildCount() == 4) {
            return graphTraversal.as(antlr.argumentVisitor.parseString(ctx.stringArgument()));
        } else {
            return graphTraversal.as(antlr.argumentVisitor.parseString(ctx.stringArgument()),
                    antlr.genericVisitor.parseStringVarargs(ctx.stringLiteralVarargs()));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_barrier_Consumer(final GremlinParser.TraversalMethod_barrier_ConsumerContext ctx) {
        // normSack is a special consumer enum type defined in org.apache.tinkerpop.gremlin.process.traversal.SackFunctions.Barrier
        // it is not used in any other traversal methods.
        return this.graphTraversal.barrier(normSack);
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
        return graphTraversal.barrier(antlr.argumentVisitor.parseNumber(ctx.integerArgument()).intValue());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_both(final GremlinParser.TraversalMethod_bothContext ctx) {
        return graphTraversal.both(antlr.genericVisitor.parseStringVarargs(ctx.stringLiteralVarargs()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_bothE(final GremlinParser.TraversalMethod_bothEContext ctx) {
        return graphTraversal.bothE(antlr.genericVisitor.parseStringVarargs(ctx.stringLiteralVarargs()));
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
        return graphTraversal.by(antlr.argumentVisitor.parseFunction(ctx.traversalFunctionArgument()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_by_Function_Comparator(final GremlinParser.TraversalMethod_by_Function_ComparatorContext ctx) {
        return graphTraversal.by(antlr.argumentVisitor.parseFunction(ctx.traversalFunctionArgument()),
                antlr.argumentVisitor.parseComparator(ctx.traversalComparatorArgument()));
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
        return graphTraversal.by(antlr.argumentVisitor.parseString(ctx.stringArgument()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_by_String_Comparator(final GremlinParser.TraversalMethod_by_String_ComparatorContext ctx) {
        return graphTraversal.by(antlr.argumentVisitor.parseString(ctx.stringArgument()),
                TraversalEnumParser.parseTraversalEnumFromContext(Order.class, ctx.getChild(4)));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_by_T(final GremlinParser.TraversalMethod_by_TContext ctx) {
        return graphTraversal.by(antlr.argumentVisitor.parseT(ctx.traversalTokenArgument()));
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
            return graphTraversal.cap(antlr.argumentVisitor.parseString(ctx.stringArgument()));
        } else {
            return graphTraversal.cap(antlr.argumentVisitor.parseString(ctx.stringArgument()),
                    antlr.genericVisitor.parseStringVarargs(ctx.stringLiteralVarargs()));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_choose_Function(final GremlinParser.TraversalMethod_choose_FunctionContext ctx) {
        return graphTraversal.choose(antlr.argumentVisitor.parseFunction(ctx.traversalFunctionArgument()));
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
    public GraphTraversal visitTraversalMethod_coin(final GremlinParser.TraversalMethod_coinContext ctx) {
        return graphTraversal.coin(((Number) antlr.argumentVisitor.visitFloatArgument(ctx.floatArgument())).doubleValue());
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
        return graphTraversal.count(antlr.argumentVisitor.parseScope(ctx.traversalScopeArgument()));
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
        return graphTraversal.dedup(antlr.argumentVisitor.parseScope(ctx.traversalScopeArgument()),
                antlr.genericVisitor.parseStringVarargs(ctx.stringLiteralVarargs()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_dedup_String(final GremlinParser.TraversalMethod_dedup_StringContext ctx) {
        return graphTraversal.dedup(antlr.genericVisitor.parseStringVarargs(ctx.stringLiteralVarargs()));
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
        return this.graphTraversal.fail(antlr.argumentVisitor.parseString(ctx.stringArgument()));
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
    public GraphTraversal visitTraversalMethod_from_String(final GremlinParser.TraversalMethod_from_StringContext ctx) {
        return graphTraversal.from(antlr.argumentVisitor.parseString(ctx.stringArgument()));
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
        return graphTraversal.groupCount(antlr.argumentVisitor.parseString(ctx.stringArgument()));
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
        return graphTraversal.group(antlr.argumentVisitor.parseString(ctx.stringArgument()));
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
            return graphTraversal.hasKey(antlr.argumentVisitor.parseString(ctx.stringNullableArgument()));
        } else {
            return graphTraversal.hasKey(antlr.argumentVisitor.parseString(ctx.stringNullableArgument()),
                    antlr.genericVisitor.parseStringVarargs(ctx.stringLiteralVarargs()));
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
            return graphTraversal.hasLabel(antlr.argumentVisitor.parseString(ctx.stringNullableArgument()));
        } else {
            return graphTraversal.hasLabel(antlr.argumentVisitor.parseString(ctx.stringNullableArgument()),
                    antlr.genericVisitor.parseStringVarargs(ctx.stringLiteralVarargs()));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_hasNot(final GremlinParser.TraversalMethod_hasNotContext ctx) {
        return graphTraversal.hasNot(antlr.argumentVisitor.parseString(ctx.stringNullableArgument()));
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
        return graphTraversal.has(antlr.argumentVisitor.parseString(ctx.stringNullableArgument()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_has_String_Object(final GremlinParser.TraversalMethod_has_String_ObjectContext ctx) {
        return graphTraversal.has(antlr.argumentVisitor.parseString(ctx.stringNullableArgument()),
                antlr.argumentVisitor.visitGenericLiteralArgument(ctx.genericLiteralArgument()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_has_String_P(final GremlinParser.TraversalMethod_has_String_PContext ctx) {
        return graphTraversal.has(antlr.argumentVisitor.parseString(ctx.stringNullableArgument()),
                antlr.traversalPredicateVisitor.visitTraversalPredicate(ctx.traversalPredicate()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_has_String_String_Object(final GremlinParser.TraversalMethod_has_String_String_ObjectContext ctx) {
        return graphTraversal.has(antlr.argumentVisitor.parseString(ctx.stringNullableArgument(0)),
                antlr.argumentVisitor.parseString(ctx.stringNullableArgument(1)),
                antlr.argumentVisitor.visitGenericLiteralArgument(ctx.genericLiteralArgument()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_has_String_String_P(final GremlinParser.TraversalMethod_has_String_String_PContext ctx) {
        return graphTraversal.has(antlr.argumentVisitor.parseString(ctx.stringNullableArgument(0)),
                antlr.argumentVisitor.parseString(ctx.stringNullableArgument(1)),
                antlr.traversalPredicateVisitor.visitTraversalPredicate(ctx.traversalPredicate()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_has_String_Traversal(final GremlinParser.TraversalMethod_has_String_TraversalContext ctx) {
        return graphTraversal.has(antlr.argumentVisitor.parseString(ctx.stringNullableArgument()),
                antlr.tvisitor.visitNestedTraversal(ctx.nestedTraversal()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_has_T_Object(final GremlinParser.TraversalMethod_has_T_ObjectContext ctx) {
        return graphTraversal.has(antlr.argumentVisitor.parseT(ctx.traversalTokenArgument()),
                antlr.argumentVisitor.visitGenericLiteralArgument(ctx.genericLiteralArgument()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_has_T_P(final GremlinParser.TraversalMethod_has_T_PContext ctx) {
        return graphTraversal.has(antlr.argumentVisitor.parseT(ctx.traversalTokenArgument()),
                antlr.traversalPredicateVisitor.visitTraversalPredicate(ctx.traversalPredicate()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_has_T_Traversal(final GremlinParser.TraversalMethod_has_T_TraversalContext ctx) {
        return graphTraversal.has(antlr.argumentVisitor.parseT(ctx.traversalTokenArgument()),
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
        return graphTraversal.in(antlr.genericVisitor.parseStringVarargs(ctx.stringLiteralVarargs()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_inE(final GremlinParser.TraversalMethod_inEContext ctx) {
        return graphTraversal.inE(antlr.genericVisitor.parseStringVarargs(ctx.stringLiteralVarargs()));
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
        return graphTraversal.limit(antlr.argumentVisitor.parseScope(ctx.traversalScopeArgument()),
                antlr.argumentVisitor.parseNumber(ctx.integerArgument()).longValue());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_limit_long(final GremlinParser.TraversalMethod_limit_longContext ctx) {
        return graphTraversal.limit(antlr.argumentVisitor.parseNumber(ctx.integerArgument()).longValue());
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
        return graphTraversal.loops(antlr.argumentVisitor.parseString(ctx.stringArgument()));
    }

    @Override
    public GraphTraversal visitTraversalMethod_repeat_String_Traversal(final GremlinParser.TraversalMethod_repeat_String_TraversalContext ctx) {
        return graphTraversal.repeat((antlr.argumentVisitor.parseString(ctx.stringArgument())),
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
            return graphTraversal.with(antlr.argumentVisitor.parseString(ctx.stringArgument()));
        }
    }

    @Override
    public GraphTraversal visitTraversalMethod_with_String_Object(final GremlinParser.TraversalMethod_with_String_ObjectContext ctx) {
        final String k;
        if (ctx.withOptionKeys() != null) {
            k = (String) WithOptionsVisitor.instance().visitWithOptionKeys(ctx.withOptionKeys());
        } else {
            k = antlr.argumentVisitor.parseString(ctx.stringArgument());
        }

        final Object o;
        if (ctx.withOptionsValues() != null) {
            o = WithOptionsVisitor.instance().visitWithOptionsValues(ctx.withOptionsValues());
        } else if (ctx.ioOptionsValues() != null) {
            o = WithOptionsVisitor.instance().visitIoOptionsValues(ctx.ioOptionsValues());
        } else {
            o = antlr.argumentVisitor.parseObject(ctx.genericLiteralArgument());
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
        return graphTraversal.max(antlr.argumentVisitor.parseScope(ctx.traversalScopeArgument()));
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
        return graphTraversal.mean(antlr.argumentVisitor.parseScope(ctx.traversalScopeArgument()));
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
        return graphTraversal.min(antlr.argumentVisitor.parseScope(ctx.traversalScopeArgument()));
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
        return graphTraversal.option(antlr.argumentVisitor.visitGenericLiteralArgument(ctx.genericLiteralArgument()),
                antlr.tvisitor.visitNestedTraversal(ctx.nestedTraversal()));
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
        return graphTraversal.option(antlr.argumentVisitor.parseMerge(ctx.traversalMergeArgument()),
                (Map) antlr.argumentVisitor.visitGenericLiteralMapNullableArgument(ctx.genericLiteralMapNullableArgument()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_option_Merge_Traversal(final GremlinParser.TraversalMethod_option_Merge_TraversalContext ctx) {
        return this.graphTraversal.option(antlr.argumentVisitor.parseMerge(ctx.traversalMergeArgument()),
                antlr.tvisitor.visitNestedTraversal(ctx.nestedTraversal()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Traversal visitTraversalMethod_option_Merge_Map_Cardinality(final GremlinParser.TraversalMethod_option_Merge_Map_CardinalityContext ctx) {
        if (ctx.genericLiteralMapNullableArgument().nullLiteral() != null) {
            return this.graphTraversal.option(antlr.argumentVisitor.parseMerge(ctx.traversalMergeArgument()), (Map) null);
        }

        return graphTraversal.option(antlr.argumentVisitor.parseMerge(ctx.traversalMergeArgument()),
                (Map) new GenericLiteralVisitor(antlr).visitGenericLiteralMap(ctx.genericLiteralMapNullableArgument().genericLiteralMap()),
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
        return graphTraversal.order(antlr.argumentVisitor.parseScope(ctx.traversalScopeArgument()));
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
        return graphTraversal.out(antlr.genericVisitor.parseStringVarargs(ctx.stringLiteralVarargs()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_outE(final GremlinParser.TraversalMethod_outEContext ctx) {
        return graphTraversal.outE(antlr.genericVisitor.parseStringVarargs(ctx.stringLiteralVarargs()));
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
    public GraphTraversal visitTraversalMethod_profile_Empty(final GremlinParser.TraversalMethod_profile_EmptyContext ctx) {
        return graphTraversal.profile();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_profile_String(final GremlinParser.TraversalMethod_profile_StringContext ctx) {
        return graphTraversal.profile(antlr.argumentVisitor.parseString(ctx.stringArgument()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_project(final GremlinParser.TraversalMethod_projectContext ctx) {
        if (ctx.getChildCount() == 4) {
            return graphTraversal.project(antlr.argumentVisitor.parseString(ctx.stringArgument()));
        } else {
            return graphTraversal.project(antlr.argumentVisitor.parseString(ctx.stringArgument()),
                    antlr.genericVisitor.parseStringVarargs(ctx.stringLiteralVarargs()));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_properties(final GremlinParser.TraversalMethod_propertiesContext ctx) {
        return graphTraversal.properties(antlr.genericVisitor.parseStringVarargs(ctx.stringLiteralVarargs()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_elementMap(final GremlinParser.TraversalMethod_elementMapContext ctx) {
        return graphTraversal.elementMap(antlr.genericVisitor.parseStringVarargs(ctx.stringLiteralVarargs()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_propertyMap(final GremlinParser.TraversalMethod_propertyMapContext ctx) {
        return graphTraversal.propertyMap(antlr.genericVisitor.parseStringVarargs(ctx.stringLiteralVarargs()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_property_Cardinality_Object_Object_Object(final GremlinParser.TraversalMethod_property_Cardinality_Object_Object_ObjectContext ctx) {
        return graphTraversal.property(antlr.argumentVisitor.parseCardinality(ctx.traversalCardinalityArgument()),
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
        return graphTraversal.property(Cardinality.list, antlr.argumentVisitor.parseMap(ctx.genericLiteralMapNullableArgument()));
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
        return graphTraversal.range(antlr.argumentVisitor.parseScope(ctx.traversalScopeArgument()),
                antlr.argumentVisitor.parseNumber(ctx.integerArgument(0)).longValue(),
                antlr.argumentVisitor.parseNumber(ctx.integerArgument(1)).longValue());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_range_long_long(final GremlinParser.TraversalMethod_range_long_longContext ctx) {
        return graphTraversal.range(antlr.argumentVisitor.parseNumber(ctx.integerArgument(0)).longValue(),
               antlr.argumentVisitor.parseNumber(ctx.integerArgument(1)).longValue());
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
        return graphTraversal.sample(antlr.argumentVisitor.parseScope(ctx.traversalScopeArgument()),
                antlr.argumentVisitor.parseNumber(ctx.integerArgument()).intValue());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_sample_int(final GremlinParser.TraversalMethod_sample_intContext ctx) {
        return graphTraversal.sample(antlr.argumentVisitor.parseNumber(ctx.integerArgument()).intValue());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_select_Column(final GremlinParser.TraversalMethod_select_ColumnContext ctx) {
        return graphTraversal.select(antlr.argumentVisitor.parseColumn(ctx.traversalColumnArgument()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_select_Pop_String(final GremlinParser.TraversalMethod_select_Pop_StringContext ctx) {
        return graphTraversal.select(antlr.argumentVisitor.parsePop(ctx.traversalPopArgument()),
                antlr.argumentVisitor.parseString(ctx.stringArgument()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_select_Pop_String_String_String(final GremlinParser.TraversalMethod_select_Pop_String_String_StringContext ctx) {
        return graphTraversal.select(antlr.argumentVisitor.parsePop(ctx.traversalPopArgument()),
                antlr.argumentVisitor.parseString(ctx.stringArgument(0)),
                antlr.argumentVisitor.parseString(ctx.stringArgument(1)),
                antlr.genericVisitor.parseStringVarargs(ctx.stringLiteralVarargs()));
    }

    @Override
    public GraphTraversal visitTraversalMethod_select_Pop_Traversal(final GremlinParser.TraversalMethod_select_Pop_TraversalContext ctx) {
        return graphTraversal.select(antlr.argumentVisitor.parsePop(ctx.traversalPopArgument()),
                antlr.tvisitor.visitNestedTraversal(ctx.nestedTraversal()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_select_String(final GremlinParser.TraversalMethod_select_StringContext ctx) {
        return graphTraversal.select(antlr.argumentVisitor.parseString(ctx.stringArgument()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_select_String_String_String(final GremlinParser.TraversalMethod_select_String_String_StringContext ctx) {
        return graphTraversal.select(antlr.argumentVisitor.parseString(ctx.stringArgument(0)),
                antlr.argumentVisitor.parseString(ctx.stringArgument(1)),
                antlr.genericVisitor.parseStringVarargs(ctx.stringLiteralVarargs()));
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
        return graphTraversal.skip(antlr.argumentVisitor.parseScope(ctx.traversalScopeArgument()),
                antlr.argumentVisitor.parseNumber(ctx.integerArgument()).longValue());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_skip_long(final GremlinParser.TraversalMethod_skip_longContext ctx) {
        return graphTraversal.skip(antlr.argumentVisitor.parseNumber(ctx.integerArgument()).longValue());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_store(final GremlinParser.TraversalMethod_storeContext ctx) {
        return graphTraversal.store(antlr.argumentVisitor.parseString(ctx.stringArgument()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_subgraph(final GremlinParser.TraversalMethod_subgraphContext ctx) {
        return graphTraversal.subgraph(antlr.argumentVisitor.parseString(ctx.stringArgument()));
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
        return graphTraversal.sum(antlr.argumentVisitor.parseScope(ctx.traversalScopeArgument()));
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
        return graphTraversal.tail(antlr.argumentVisitor.parseScope(ctx.traversalScopeArgument()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_tail_Scope_long(final GremlinParser.TraversalMethod_tail_Scope_longContext ctx) {
        return graphTraversal.tail(antlr.argumentVisitor.parseScope(ctx.traversalScopeArgument()),
                antlr.argumentVisitor.parseNumber(ctx.integerArgument()).longValue());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_tail_long(final GremlinParser.TraversalMethod_tail_longContext ctx) {
        return graphTraversal.tail(antlr.argumentVisitor.parseNumber(ctx.integerArgument()).longValue());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_timeLimit(final GremlinParser.TraversalMethod_timeLimitContext ctx) {
        return graphTraversal.timeLimit(antlr.argumentVisitor.parseNumber(ctx.integerArgument()).longValue());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_times(final GremlinParser.TraversalMethod_timesContext ctx) {
        return graphTraversal.times(antlr.argumentVisitor.parseNumber(ctx.integerArgument()).intValue());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_toE(final GremlinParser.TraversalMethod_toEContext ctx) {
        return graphTraversal.toE(antlr.argumentVisitor.parseDirection(ctx.traversalDirectionArgument()),
                antlr.genericVisitor.parseStringVarargs(ctx.stringLiteralVarargs()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_toV(final GremlinParser.TraversalMethod_toVContext ctx) {
        return graphTraversal.toV(antlr.argumentVisitor.parseDirection(ctx.traversalDirectionArgument()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_to_Direction_String(final GremlinParser.TraversalMethod_to_Direction_StringContext ctx) {
        return graphTraversal.to(antlr.argumentVisitor.parseDirection(ctx.traversalDirectionArgument()),
                antlr.genericVisitor.parseStringVarargs(ctx.stringLiteralVarargs()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_to_String(final GremlinParser.TraversalMethod_to_StringContext ctx) {
        return graphTraversal.to(antlr.argumentVisitor.parseString(ctx.stringArgument()));
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
        return graphTraversal.tree(antlr.argumentVisitor.parseString(ctx.stringArgument()));
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
        return graphTraversal.valueMap(antlr.genericVisitor.parseStringVarargs(ctx.stringLiteralVarargs()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_valueMap_boolean_String(final GremlinParser.TraversalMethod_valueMap_boolean_StringContext ctx) {
        if (ctx.getChildCount() == 4) {
            return graphTraversal.valueMap((boolean) antlr.argumentVisitor.visitBooleanArgument(ctx.booleanArgument()));
        } else {
            return graphTraversal.valueMap((boolean) antlr.argumentVisitor.visitBooleanArgument(ctx.booleanArgument()),
                    antlr.genericVisitor.parseStringVarargs(ctx.stringLiteralVarargs()));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_values(final GremlinParser.TraversalMethod_valuesContext ctx) {
        return graphTraversal.values(antlr.genericVisitor.parseStringVarargs(ctx.stringLiteralVarargs()));
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
        return graphTraversal.where(antlr.argumentVisitor.parseString(ctx.stringArgument()),
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
        return graphTraversal.math(antlr.argumentVisitor.parseString(ctx.stringArgument()));
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
    public Traversal visitTraversalMethod_from_Vertex(final GremlinParser.TraversalMethod_from_VertexContext ctx) {
        return graphTraversal.from(antlr.argumentVisitor.parseVertex(ctx.structureVertexArgument()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Traversal visitTraversalMethod_to_Vertex(final GremlinParser.TraversalMethod_to_VertexContext ctx) {
        return graphTraversal.to(antlr.argumentVisitor.parseVertex(ctx.structureVertexArgument()));
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
        return graphTraversal.call(antlr.argumentVisitor.parseString(ctx.stringArgument()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Traversal visitTraversalMethod_call_string_map(final GremlinParser.TraversalMethod_call_string_mapContext ctx) {
        return graphTraversal.call(antlr.argumentVisitor.parseString(ctx.stringArgument()),
                                   antlr.argumentVisitor.parseMap(ctx.genericLiteralMapArgument()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Traversal visitTraversalMethod_call_string_traversal(final GremlinParser.TraversalMethod_call_string_traversalContext ctx) {
        return graphTraversal.call(antlr.argumentVisitor.parseString(ctx.stringArgument()),
                antlr.tvisitor.visitNestedTraversal(ctx.nestedTraversal()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Traversal visitTraversalMethod_call_string_map_traversal(final GremlinParser.TraversalMethod_call_string_map_traversalContext ctx) {
        return graphTraversal.call(antlr.argumentVisitor.parseString(ctx.stringArgument()),
                antlr.argumentVisitor.parseMap(ctx.genericLiteralMapArgument()),
                antlr.tvisitor.visitNestedTraversal(ctx.nestedTraversal()));
    }

    public GraphTraversal[] getNestedTraversalList(final GremlinParser.NestedTraversalListContext ctx) {
        return ctx.nestedTraversalExpr().nestedTraversal()
                .stream()
                .map(this::visitNestedTraversal)
                .toArray(GraphTraversal[]::new);
    }
}
