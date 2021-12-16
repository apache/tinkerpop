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

import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality;

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
        if (ctx.genericLiteralList().getChildCount() != 0) {
            return this.graphTraversal.V(GenericLiteralVisitor.getGenericLiteralList(ctx.genericLiteralList()));
        } else {
            return this.graphTraversal.V();
        }
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
        return this.graphTraversal.addV(GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral()));
    }

    @Override
    public GraphTraversal visitTraversalMethod_addE_Traversal(final GremlinParser.TraversalMethod_addE_TraversalContext ctx) {
        return this.graphTraversal.addE(antlr.tvisitor.visitNestedTraversal(ctx.nestedTraversal()));
    }

    @Override
    public GraphTraversal visitTraversalMethod_addV_Traversal(final GremlinParser.TraversalMethod_addV_TraversalContext ctx) {
        return this.graphTraversal.addV(antlr.tvisitor.visitNestedTraversal(ctx.nestedTraversal()));
    }

    @Override
    public GraphTraversal visitTraversalMethod_addE_String(final GremlinParser.TraversalMethod_addE_StringContext ctx) {
        final int childIndexOfParameterEdgeLabel = 2;
        final GremlinParser.StringLiteralContext stringLiteralContext =
                (GremlinParser.StringLiteralContext) (ctx.getChild(childIndexOfParameterEdgeLabel));
        return this.graphTraversal.addE(GenericLiteralVisitor.getStringLiteral(stringLiteralContext));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_aggregate_String(final GremlinParser.TraversalMethod_aggregate_StringContext ctx) {
        return graphTraversal.aggregate(GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_aggregate_Scope_String(final GremlinParser.TraversalMethod_aggregate_Scope_StringContext ctx) {
        return graphTraversal.aggregate(
                TraversalEnumParser.parseTraversalEnumFromContext(Scope.class, ctx.getChild(2)),
                GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral()));
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
            return graphTraversal.as(GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral()));
        } else {
            return graphTraversal.as(GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral()),
                    GenericLiteralVisitor.getStringLiteralList(ctx.stringLiteralList()));
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
        return graphTraversal.barrier(Integer.valueOf(ctx.integerLiteral().getText()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_both(final GremlinParser.TraversalMethod_bothContext ctx) {
        return graphTraversal.both(GenericLiteralVisitor.getStringLiteralList(ctx.stringLiteralList()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_bothE(final GremlinParser.TraversalMethod_bothEContext ctx) {
        return graphTraversal.bothE(GenericLiteralVisitor.getStringLiteralList(ctx.stringLiteralList()));
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
        return graphTraversal.by(TraversalFunctionVisitor.getInstance().visitTraversalFunction(ctx.traversalFunction()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_by_Function_Comparator(final GremlinParser.TraversalMethod_by_Function_ComparatorContext ctx) {
        return graphTraversal.by(TraversalFunctionVisitor.getInstance().visitTraversalFunction(ctx.traversalFunction()),
                TraversalEnumParser.parseTraversalEnumFromContext(Order.class, ctx.getChild(4)));
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
        return graphTraversal.by(GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_by_String_Comparator(final GremlinParser.TraversalMethod_by_String_ComparatorContext ctx) {
        return graphTraversal.by(GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral()),
                TraversalEnumParser.parseTraversalEnumFromContext(Order.class, ctx.getChild(4)));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_by_T(final GremlinParser.TraversalMethod_by_TContext ctx) {
        return graphTraversal.by(TraversalEnumParser.parseTraversalEnumFromContext(T.class, ctx.getChild(2)));
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
            return graphTraversal.cap(GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral()));
        } else {
            return graphTraversal.cap(GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral()),
                    GenericLiteralVisitor.getStringLiteralList(ctx.stringLiteralList()));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_choose_Function(final GremlinParser.TraversalMethod_choose_FunctionContext ctx) {
        return graphTraversal.choose(TraversalFunctionVisitor.getInstance().visitTraversalFunction(ctx.traversalFunction()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_choose_Predicate_Traversal(final GremlinParser.TraversalMethod_choose_Predicate_TraversalContext ctx) {
        return graphTraversal.choose(TraversalPredicateVisitor.getInstance().visitTraversalPredicate(ctx.traversalPredicate()),
                antlr.tvisitor.visitNestedTraversal(ctx.nestedTraversal()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_choose_Predicate_Traversal_Traversal(final GremlinParser.TraversalMethod_choose_Predicate_Traversal_TraversalContext ctx) {
        return graphTraversal.choose(TraversalPredicateVisitor.getInstance().visitTraversalPredicate(ctx.traversalPredicate()),
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
        return graphTraversal.coin(Double.valueOf(ctx.floatLiteral().getText()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_constant(final GremlinParser.TraversalMethod_constantContext ctx) {
        return graphTraversal
                .constant(GenericLiteralVisitor.getInstance().visitGenericLiteral(ctx.genericLiteral()));
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
        return graphTraversal.count(TraversalEnumParser.parseTraversalEnumFromContext(Scope.class, ctx.getChild(2)));
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
        return graphTraversal.dedup(TraversalEnumParser.parseTraversalEnumFromContext(Scope.class, ctx.getChild(2)),
                GenericLiteralVisitor.getStringLiteralList(ctx.stringLiteralList()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_dedup_String(final GremlinParser.TraversalMethod_dedup_StringContext ctx) {
        return graphTraversal.dedup(GenericLiteralVisitor.getStringLiteralList(ctx.stringLiteralList()));
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
        return graphTraversal.emit(TraversalPredicateVisitor.getInstance().visitTraversalPredicate(ctx.traversalPredicate()));
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
    public GraphTraversal visitTraversalMethod_filter_Predicate(final GremlinParser.TraversalMethod_filter_PredicateContext ctx) {
        return graphTraversal.filter(TraversalPredicateVisitor.getInstance().visitTraversalPredicate(ctx.traversalPredicate()));
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
                GenericLiteralVisitor.getInstance().visitGenericLiteral(ctx.genericLiteral()),
                (BiFunction) TraversalEnumParser.parseTraversalEnumFromContext(Operator.class, ctx.getChild(4)));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_from_String(final GremlinParser.TraversalMethod_from_StringContext ctx) {
        return graphTraversal.from(GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral()));
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
        return graphTraversal.groupCount(GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral()));
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
        return graphTraversal.group(GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_hasId_Object_Object(final GremlinParser.TraversalMethod_hasId_Object_ObjectContext ctx) {
        return graphTraversal.hasId(GenericLiteralVisitor.getInstance().visitGenericLiteral(ctx.genericLiteral()),
                GenericLiteralVisitor.getGenericLiteralList(ctx.genericLiteralList()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_hasId_P(final GremlinParser.TraversalMethod_hasId_PContext ctx) {
        return graphTraversal.hasId(TraversalPredicateVisitor.getInstance().visitTraversalPredicate(ctx.traversalPredicate()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_hasKey_P(final GremlinParser.TraversalMethod_hasKey_PContext ctx) {
        return graphTraversal.hasKey(TraversalPredicateVisitor.getInstance().visitTraversalPredicate(ctx.traversalPredicate()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_hasKey_String_String(final GremlinParser.TraversalMethod_hasKey_String_StringContext ctx) {
        if (ctx.getChildCount() == 4) {
            return graphTraversal.hasKey(GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral()));
        } else {
            return graphTraversal.hasKey(GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral()),
                    GenericLiteralVisitor.getStringLiteralList(ctx.stringLiteralList()));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_hasLabel_P(final GremlinParser.TraversalMethod_hasLabel_PContext ctx) {
        return graphTraversal.hasLabel(TraversalPredicateVisitor.getInstance().visitTraversalPredicate(ctx.traversalPredicate()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_hasLabel_String_String(final GremlinParser.TraversalMethod_hasLabel_String_StringContext ctx) {
        if (ctx.getChildCount() == 4) {
            return graphTraversal.hasLabel(GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral()));
        } else {
            return graphTraversal.hasLabel(GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral()),
                    GenericLiteralVisitor.getStringLiteralList(ctx.stringLiteralList()));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_hasNot(final GremlinParser.TraversalMethod_hasNotContext ctx) {
        return graphTraversal.hasNot(GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_hasValue_Object_Object(final GremlinParser.TraversalMethod_hasValue_Object_ObjectContext ctx) {
        return graphTraversal.hasValue(GenericLiteralVisitor.getInstance().visitGenericLiteral(ctx.genericLiteral()),
                GenericLiteralVisitor.getGenericLiteralList(ctx.genericLiteralList()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_hasValue_P(final GremlinParser.TraversalMethod_hasValue_PContext ctx) {
        return graphTraversal.hasValue(TraversalPredicateVisitor.getInstance().visitTraversalPredicate(ctx.traversalPredicate()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_has_String(final GremlinParser.TraversalMethod_has_StringContext ctx) {
        return graphTraversal.has(GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_has_String_Object(final GremlinParser.TraversalMethod_has_String_ObjectContext ctx) {
        return graphTraversal.has(GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral()),
                new GenericLiteralVisitor(antlr).visitGenericLiteral(ctx.genericLiteral()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_has_String_P(final GremlinParser.TraversalMethod_has_String_PContext ctx) {
        return graphTraversal.has(GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral()),
                TraversalPredicateVisitor.getInstance().visitTraversalPredicate(ctx.traversalPredicate()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_has_String_String_Object(final GremlinParser.TraversalMethod_has_String_String_ObjectContext ctx) {
        return graphTraversal.has(GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral(0)),
                GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral(1)),
                GenericLiteralVisitor.getInstance().visitGenericLiteral(ctx.genericLiteral()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_has_String_String_P(final GremlinParser.TraversalMethod_has_String_String_PContext ctx) {
        return graphTraversal.has(GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral(0)),
                GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral(1)),
                TraversalPredicateVisitor.getInstance().visitTraversalPredicate(ctx.traversalPredicate()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_has_String_Traversal(final GremlinParser.TraversalMethod_has_String_TraversalContext ctx) {
        return graphTraversal.has(GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral()),
                antlr.tvisitor.visitNestedTraversal(ctx.nestedTraversal()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_has_T_Object(final GremlinParser.TraversalMethod_has_T_ObjectContext ctx) {
        return graphTraversal.has(TraversalEnumParser.parseTraversalEnumFromContext(T.class, ctx.getChild(2)),
                new GenericLiteralVisitor(antlr).visitGenericLiteral(ctx.genericLiteral()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_has_T_P(final GremlinParser.TraversalMethod_has_T_PContext ctx) {
        return graphTraversal.has(TraversalEnumParser.parseTraversalEnumFromContext(T.class, ctx.getChild(2)),
                TraversalPredicateVisitor.getInstance().visitTraversalPredicate(ctx.traversalPredicate()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_has_T_Traversal(final GremlinParser.TraversalMethod_has_T_TraversalContext ctx) {
        return graphTraversal.has(TraversalEnumParser.parseTraversalEnumFromContext(T.class, ctx.getChild(2)),
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
        return graphTraversal.in(GenericLiteralVisitor.getStringLiteralList(ctx.stringLiteralList()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_inE(final GremlinParser.TraversalMethod_inEContext ctx) {
        return graphTraversal.inE(GenericLiteralVisitor.getStringLiteralList(ctx.stringLiteralList()));
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
        return graphTraversal.inject(GenericLiteralVisitor.getGenericLiteralList(ctx.genericLiteralList()));
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
        return graphTraversal.is(GenericLiteralVisitor.getInstance().visitGenericLiteral(ctx.genericLiteral()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_is_P(final GremlinParser.TraversalMethod_is_PContext ctx) {
        return graphTraversal.is(TraversalPredicateVisitor.getInstance().visitTraversalPredicate(ctx.traversalPredicate()));
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
        return graphTraversal.limit(TraversalEnumParser.parseTraversalEnumFromContext(Scope.class, ctx.getChild(2)),
                Integer.valueOf(ctx.integerLiteral().getText()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_limit_long(final GremlinParser.TraversalMethod_limit_longContext ctx) {
        return graphTraversal.limit(Integer.valueOf(ctx.integerLiteral().getText()));
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
        return graphTraversal.loops(GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral()));
    }

    @Override
    public GraphTraversal visitTraversalMethod_repeat_String_Traversal(final GremlinParser.TraversalMethod_repeat_String_TraversalContext ctx) {
        return graphTraversal.repeat((GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral())),
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
        return graphTraversal.with(GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral()));
    }

    @Override
    public GraphTraversal visitTraversalMethod_with_String_Object(final GremlinParser.TraversalMethod_with_String_ObjectContext ctx) {
        return graphTraversal.with(GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral()),
                new GenericLiteralVisitor(antlr).visitGenericLiteral(ctx.genericLiteral()));
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
        return graphTraversal.max(TraversalEnumParser.parseTraversalEnumFromContext(Scope.class, ctx.getChild(2)));
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
        return graphTraversal.mean(TraversalEnumParser.parseTraversalEnumFromContext(Scope.class, ctx.getChild(2)));
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
        return graphTraversal.min(TraversalEnumParser.parseTraversalEnumFromContext(Scope.class, ctx.getChild(2)));
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
        return graphTraversal.option(new GenericLiteralVisitor(antlr).visitGenericLiteral(ctx.genericLiteral()),
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
        return graphTraversal.order(TraversalEnumParser.parseTraversalEnumFromContext(Scope.class, ctx.getChild(2)));
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
        return graphTraversal.out(GenericLiteralVisitor.getStringLiteralList(ctx.stringLiteralList()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_outE(final GremlinParser.TraversalMethod_outEContext ctx) {
        return graphTraversal.outE(GenericLiteralVisitor.getStringLiteralList(ctx.stringLiteralList()));
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
        return graphTraversal.pageRank(Double.valueOf(ctx.floatLiteral().getText()));
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
        return graphTraversal.profile(GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_project(final GremlinParser.TraversalMethod_projectContext ctx) {
        if (ctx.getChildCount() == 4) {
            return graphTraversal.project(GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral()));
        } else {
            return graphTraversal.project(GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral()),
                    GenericLiteralVisitor.getStringLiteralList(ctx.stringLiteralList()));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_properties(final GremlinParser.TraversalMethod_propertiesContext ctx) {
        return graphTraversal.properties(GenericLiteralVisitor.getStringLiteralList(ctx.stringLiteralList()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_elementMap(final GremlinParser.TraversalMethod_elementMapContext ctx) {
        return graphTraversal.elementMap(GenericLiteralVisitor.getStringLiteralList(ctx.stringLiteralList()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_propertyMap(final GremlinParser.TraversalMethod_propertyMapContext ctx) {
        return graphTraversal.propertyMap(GenericLiteralVisitor.getStringLiteralList(ctx.stringLiteralList()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_property_Cardinality_Object_Object_Object(final GremlinParser.TraversalMethod_property_Cardinality_Object_Object_ObjectContext ctx) {
        return graphTraversal.property(TraversalEnumParser.parseTraversalEnumFromContext(VertexProperty.Cardinality.class, ctx.getChild(2)),
                GenericLiteralVisitor.getInstance().visitGenericLiteral(ctx.genericLiteral(0)),
                GenericLiteralVisitor.getInstance().visitGenericLiteral(ctx.genericLiteral(1)),
                GenericLiteralVisitor.getGenericLiteralList(ctx.genericLiteralList()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_property_Object_Object_Object(final GremlinParser.TraversalMethod_property_Object_Object_ObjectContext ctx) {
        return graphTraversal.property(new GenericLiteralVisitor(antlr).visitGenericLiteral(ctx.genericLiteral(0)),
                new GenericLiteralVisitor(antlr).visitGenericLiteral(ctx.genericLiteral(1)),
                GenericLiteralVisitor.getGenericLiteralList(ctx.genericLiteralList()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Traversal visitTraversalMethod_property_Cardinality_Object(
            TraversalMethod_property_Cardinality_ObjectContext ctx) {
        return graphTraversal.property(Cardinality.list, new GenericLiteralVisitor(antlr).visitGenericLiteralMap(ctx.genericLiteralMap()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Traversal visitTraversalMethod_property_Object(TraversalMethod_property_ObjectContext ctx) {
        return graphTraversal.property(new GenericLiteralVisitor(antlr).visitGenericLiteralMap(ctx.genericLiteralMap()));
    }


    @Override
    public GraphTraversal TraversalMethod_property_Object(
            final GremlinParser.TraversalMethod_property_ObjectContext ctx) {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal TraversalMethod_property_Cardinality_Object(
            final GremlinParser.TraversalMethod_property_Cardinality_ObjectContext ctx) {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_range_Scope_long_long(final GremlinParser.TraversalMethod_range_Scope_long_longContext ctx) {
        return graphTraversal.range(TraversalEnumParser.parseTraversalEnumFromContext(Scope.class, ctx.getChild(2)),
                Integer.valueOf(ctx.integerLiteral(0).getText()),
                Integer.valueOf(ctx.integerLiteral(1).getText()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_range_long_long(final GremlinParser.TraversalMethod_range_long_longContext ctx) {
        return graphTraversal.range(Integer.valueOf(ctx.integerLiteral(0).getText()),
                Integer.valueOf(ctx.integerLiteral(1).getText()));
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
        return graphTraversal.sample(TraversalEnumParser.parseTraversalEnumFromContext(Scope.class, ctx.getChild(2)),
                Integer.valueOf(ctx.integerLiteral().getText()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_sample_int(final GremlinParser.TraversalMethod_sample_intContext ctx) {
        return graphTraversal.sample(Integer.valueOf(ctx.integerLiteral().getText()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_select_Column(final GremlinParser.TraversalMethod_select_ColumnContext ctx) {
        return graphTraversal.select(TraversalEnumParser.parseTraversalEnumFromContext(Column.class, ctx.getChild(2)));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_select_Pop_String(final GremlinParser.TraversalMethod_select_Pop_StringContext ctx) {
        return graphTraversal.select(TraversalEnumParser.parseTraversalEnumFromContext(Pop.class, ctx.getChild(2)),
                GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_select_Pop_String_String_String(final GremlinParser.TraversalMethod_select_Pop_String_String_StringContext ctx) {
        return graphTraversal.select(TraversalEnumParser.parseTraversalEnumFromContext(Pop.class, ctx.getChild(2)),
                GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral(0)),
                GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral(1)),
                GenericLiteralVisitor.getStringLiteralList(ctx.stringLiteralList()));
    }

    @Override
    public GraphTraversal visitTraversalMethod_select_Pop_Traversal(final GremlinParser.TraversalMethod_select_Pop_TraversalContext ctx) {
        return graphTraversal.select(TraversalEnumParser.parseTraversalEnumFromContext(Pop.class, ctx.getChild(2)),
                antlr.tvisitor.visitNestedTraversal(ctx.nestedTraversal()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_select_String(final GremlinParser.TraversalMethod_select_StringContext ctx) {
        return graphTraversal.select(GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_select_String_String_String(final GremlinParser.TraversalMethod_select_String_String_StringContext ctx) {
        return graphTraversal.select(GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral(0)),
                GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral(1)),
                GenericLiteralVisitor.getStringLiteralList(ctx.stringLiteralList()));
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
        return graphTraversal.skip(TraversalEnumParser.parseTraversalEnumFromContext(Scope.class, ctx.getChild(2)),
                Integer.valueOf(ctx.integerLiteral().getText()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_skip_long(final GremlinParser.TraversalMethod_skip_longContext ctx) {
        return graphTraversal.skip(Integer.valueOf(ctx.integerLiteral().getText()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_store(final GremlinParser.TraversalMethod_storeContext ctx) {
        return graphTraversal.store(GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_subgraph(final GremlinParser.TraversalMethod_subgraphContext ctx) {
        return graphTraversal.subgraph(GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral()));
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
        return graphTraversal.sum(TraversalEnumParser.parseTraversalEnumFromContext(Scope.class, ctx.getChild(2)));
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
        return graphTraversal.tail(TraversalEnumParser.parseTraversalEnumFromContext(Scope.class, ctx.getChild(2)));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_tail_Scope_long(final GremlinParser.TraversalMethod_tail_Scope_longContext ctx) {
        return graphTraversal.tail(TraversalEnumParser.parseTraversalEnumFromContext(Scope.class, ctx.getChild(2)),
                Integer.valueOf(ctx.integerLiteral().getText()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_tail_long(final GremlinParser.TraversalMethod_tail_longContext ctx) {
        return graphTraversal.tail(Integer.valueOf(ctx.integerLiteral().getText()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_timeLimit(final GremlinParser.TraversalMethod_timeLimitContext ctx) {
        return graphTraversal.timeLimit(Integer.valueOf(ctx.integerLiteral().getText()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_times(final GremlinParser.TraversalMethod_timesContext ctx) {
        return graphTraversal.times(Integer.valueOf(ctx.integerLiteral().getText()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_toE(final GremlinParser.TraversalMethod_toEContext ctx) {
        return graphTraversal.toE(TraversalEnumParser.parseTraversalEnumFromContext(Direction.class, ctx.getChild(2)),
                GenericLiteralVisitor.getStringLiteralList(ctx.stringLiteralList()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_toV(final GremlinParser.TraversalMethod_toVContext ctx) {
        return graphTraversal.toV(TraversalEnumParser.parseTraversalEnumFromContext(Direction.class, ctx.getChild(2)));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_to_Direction_String(final GremlinParser.TraversalMethod_to_Direction_StringContext ctx) {
        return graphTraversal.to(TraversalEnumParser.parseTraversalEnumFromContext(Direction.class, ctx.getChild(2)),
                GenericLiteralVisitor.getStringLiteralList(ctx.stringLiteralList()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_to_String(final GremlinParser.TraversalMethod_to_StringContext ctx) {
        return graphTraversal.to(GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral()));
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
        return graphTraversal.tree(GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral()));
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
        return graphTraversal.until(TraversalPredicateVisitor.getInstance().visitTraversalPredicate(ctx.traversalPredicate()));
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
        return graphTraversal.valueMap(GenericLiteralVisitor.getStringLiteralList(ctx.stringLiteralList()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_valueMap_boolean_String(final GremlinParser.TraversalMethod_valueMap_boolean_StringContext ctx) {
        if (ctx.getChildCount() == 4) {
            return graphTraversal.valueMap(Boolean.valueOf(ctx.booleanLiteral().getText()));
        } else {
            return graphTraversal.valueMap(Boolean.valueOf(ctx.booleanLiteral().getText()),
                    GenericLiteralVisitor.getStringLiteralList(ctx.stringLiteralList()));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_values(final GremlinParser.TraversalMethod_valuesContext ctx) {
        return graphTraversal.values(GenericLiteralVisitor.getStringLiteralList(ctx.stringLiteralList()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_where_P(final GremlinParser.TraversalMethod_where_PContext ctx) {
        return graphTraversal.where(TraversalPredicateVisitor.getInstance().visitTraversalPredicate(ctx.traversalPredicate()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphTraversal visitTraversalMethod_where_String_P(final GremlinParser.TraversalMethod_where_String_PContext ctx) {
        return graphTraversal.where(GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral()),
                TraversalPredicateVisitor.getInstance().visitTraversalPredicate(ctx.traversalPredicate()));
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
        return graphTraversal.math(GenericLiteralVisitor.getStringLiteral(ctx.stringLiteral()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Traversal visitTraversalMethod_option_Predicate_Traversal(final GremlinParser.TraversalMethod_option_Predicate_TraversalContext ctx) {
        return graphTraversal.option(TraversalPredicateVisitor.getInstance().visitTraversalPredicate(ctx.traversalPredicate()),
                antlr.tvisitor.visitNestedTraversal(ctx.nestedTraversal()));
    }

    public GraphTraversal[] getNestedTraversalList(final GremlinParser.NestedTraversalListContext ctx) {
        return ctx.nestedTraversalExpr().nestedTraversal()
                .stream()
                .map(this::visitNestedTraversal)
                .toArray(GraphTraversal[]::new);
    }
}
