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
import org.antlr.v4.runtime.tree.AbstractParseTreeVisitor;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.tinkerpop.gremlin.language.grammar.GremlinParser;
import org.apache.tinkerpop.gremlin.language.grammar.GremlinVisitor;
import org.apache.tinkerpop.gremlin.process.traversal.DT;
import org.apache.tinkerpop.gremlin.process.traversal.Merge;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Pick;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.SackFunctions;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.TextP;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality;

import java.util.HashSet;
import java.util.Set;

/**
 * A Gremlin to Gremlin translator. Makes no changes to input except:
 * <ul>
 *     <li>Normalizes whitespace</li>
 *     <li>Normalize numeric suffixes to lower case</li>
 *     <li>Makes anonymous traversals explicit with double underscore</li>
 *     <li>Makes enums explicit with their proper name</li>
 * </ul>
 */
public class TranslateVisitor extends AbstractParseTreeVisitor<Void> implements GremlinVisitor<Void> {

    protected final String graphTraversalSourceName;

    protected final StringBuilder sb = new StringBuilder();

    protected final Set<String> parameters = new HashSet<>();

    public TranslateVisitor() {
        this("g");
    }

    public TranslateVisitor(final String graphTraversalSourceName) {
        this.graphTraversalSourceName = graphTraversalSourceName;
    }

    public String getTranslated() {
        return sb.toString();
    }

    public Set<String> getParameters() {
        return parameters;
    }

    protected String processGremlinSymbol(final String step) {
        return step;
    }

    protected void appendArgumentSeparator() {
        sb.append(", ");
    }

    protected void appendStepSeparator() {
        sb.append(".");
    }

    protected void appendStepOpen() {
        sb.append("(");
    }

    protected void appendStepClose() {
        sb.append(")");
    }

    protected static String removeFirstAndLastCharacters(final String text) {
        return text != null && !text.isEmpty() ? text.substring(1, text.length() - 1) : "";
    }

    @Override
    public Void visitQueryList(final GremlinParser.QueryListContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitQuery(final GremlinParser.QueryContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitEmptyQuery(final GremlinParser.EmptyQueryContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalSource(final GremlinParser.TraversalSourceContext ctx) {
        // replace "g" with whatever the user wanted to use as the graph traversal source name
        sb.append(graphTraversalSourceName);

        // child counts more than 1 means there is a step separator and a traversal source method
        if (ctx.getChildCount() > 1) {
            if (ctx.getChild(0).getChildCount() > 1) {
                appendStepSeparator();
                visitTraversalSourceSelfMethod((GremlinParser.TraversalSourceSelfMethodContext) ctx.getChild(0).getChild(2));
            }
            appendStepSeparator();
            visitTraversalSourceSelfMethod((GremlinParser.TraversalSourceSelfMethodContext) ctx.getChild(2));
        }
        return null;
    }

    @Override
    public Void visitTransactionPart(final GremlinParser.TransactionPartContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitRootTraversal(final GremlinParser.RootTraversalContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalSourceSelfMethod(final GremlinParser.TraversalSourceSelfMethodContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalSourceSelfMethod_withBulk(final GremlinParser.TraversalSourceSelfMethod_withBulkContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalSourceSelfMethod_withPath(final GremlinParser.TraversalSourceSelfMethod_withPathContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalSourceSelfMethod_withSack(final GremlinParser.TraversalSourceSelfMethod_withSackContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalSourceSelfMethod_withSideEffect(final GremlinParser.TraversalSourceSelfMethod_withSideEffectContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalSourceSelfMethod_withStrategies(final GremlinParser.TraversalSourceSelfMethod_withStrategiesContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalSourceSelfMethod_with(final GremlinParser.TraversalSourceSelfMethod_withContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalSourceSpawnMethod(final GremlinParser.TraversalSourceSpawnMethodContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalSourceSpawnMethod_addE(final GremlinParser.TraversalSourceSpawnMethod_addEContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalSourceSpawnMethod_addV(final GremlinParser.TraversalSourceSpawnMethod_addVContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalSourceSpawnMethod_E(final GremlinParser.TraversalSourceSpawnMethod_EContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalSourceSpawnMethod_V(final GremlinParser.TraversalSourceSpawnMethod_VContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalSourceSpawnMethod_inject(final GremlinParser.TraversalSourceSpawnMethod_injectContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalSourceSpawnMethod_io(final GremlinParser.TraversalSourceSpawnMethod_ioContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalSourceSpawnMethod_mergeV_Map(final GremlinParser.TraversalSourceSpawnMethod_mergeV_MapContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalSourceSpawnMethod_mergeV_Traversal(final GremlinParser.TraversalSourceSpawnMethod_mergeV_TraversalContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalSourceSpawnMethod_mergeE_Map(final GremlinParser.TraversalSourceSpawnMethod_mergeE_MapContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalSourceSpawnMethod_mergeE_Traversal(final GremlinParser.TraversalSourceSpawnMethod_mergeE_TraversalContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalSourceSpawnMethod_call_empty(final GremlinParser.TraversalSourceSpawnMethod_call_emptyContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalSourceSpawnMethod_call_string(final GremlinParser.TraversalSourceSpawnMethod_call_stringContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalSourceSpawnMethod_call_string_map(final GremlinParser.TraversalSourceSpawnMethod_call_string_mapContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalSourceSpawnMethod_call_string_traversal(final GremlinParser.TraversalSourceSpawnMethod_call_string_traversalContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalSourceSpawnMethod_call_string_map_traversal(final GremlinParser.TraversalSourceSpawnMethod_call_string_map_traversalContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalSourceSpawnMethod_union(final GremlinParser.TraversalSourceSpawnMethod_unionContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitChainedTraversal(final GremlinParser.ChainedTraversalContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitChainedParentOfGraphTraversal(final GremlinParser.ChainedParentOfGraphTraversalContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitNestedTraversal(final GremlinParser.NestedTraversalContext ctx) {
        if (ctx.ANON_TRAVERSAL_ROOT() == null)
            appendAnonymousSpawn();
        return visitChildren(ctx);
    }

    @Override
    public Void visitTerminatedTraversal(final GremlinParser.TerminatedTraversalContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod(final GremlinParser.TraversalMethodContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_V(final GremlinParser.TraversalMethod_VContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_E(final GremlinParser.TraversalMethod_EContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_addE_String(final GremlinParser.TraversalMethod_addE_StringContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_addE_Traversal(final GremlinParser.TraversalMethod_addE_TraversalContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_addV_Empty(final GremlinParser.TraversalMethod_addV_EmptyContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_addV_String(final GremlinParser.TraversalMethod_addV_StringContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_addV_Traversal(final GremlinParser.TraversalMethod_addV_TraversalContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_mergeV_empty(final GremlinParser.TraversalMethod_mergeV_emptyContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_mergeV_Map(final GremlinParser.TraversalMethod_mergeV_MapContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_mergeV_Traversal(final GremlinParser.TraversalMethod_mergeV_TraversalContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_mergeE_empty(final GremlinParser.TraversalMethod_mergeE_emptyContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_mergeE_Map(final GremlinParser.TraversalMethod_mergeE_MapContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_mergeE_Traversal(final GremlinParser.TraversalMethod_mergeE_TraversalContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_aggregate_Scope_String(final GremlinParser.TraversalMethod_aggregate_Scope_StringContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_aggregate_String(final GremlinParser.TraversalMethod_aggregate_StringContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_all_P(final GremlinParser.TraversalMethod_all_PContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_and(final GremlinParser.TraversalMethod_andContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_any_P(final GremlinParser.TraversalMethod_any_PContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_as(final GremlinParser.TraversalMethod_asContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_barrier_Consumer(final GremlinParser.TraversalMethod_barrier_ConsumerContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_barrier_Empty(final GremlinParser.TraversalMethod_barrier_EmptyContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_barrier_int(final GremlinParser.TraversalMethod_barrier_intContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_both(final GremlinParser.TraversalMethod_bothContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_bothE(final GremlinParser.TraversalMethod_bothEContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_bothV(final GremlinParser.TraversalMethod_bothVContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_branch(final GremlinParser.TraversalMethod_branchContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_by_Comparator(final GremlinParser.TraversalMethod_by_ComparatorContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_by_Empty(final GremlinParser.TraversalMethod_by_EmptyContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_by_Function(final GremlinParser.TraversalMethod_by_FunctionContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_by_Function_Comparator(final GremlinParser.TraversalMethod_by_Function_ComparatorContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_by_Order(final GremlinParser.TraversalMethod_by_OrderContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_by_String(final GremlinParser.TraversalMethod_by_StringContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_by_String_Comparator(final GremlinParser.TraversalMethod_by_String_ComparatorContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_by_T(final GremlinParser.TraversalMethod_by_TContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_by_Traversal(final GremlinParser.TraversalMethod_by_TraversalContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_by_Traversal_Comparator(final GremlinParser.TraversalMethod_by_Traversal_ComparatorContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_cap(final GremlinParser.TraversalMethod_capContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_choose_Function(final GremlinParser.TraversalMethod_choose_FunctionContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_choose_Predicate_Traversal(final GremlinParser.TraversalMethod_choose_Predicate_TraversalContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_choose_Predicate_Traversal_Traversal(final GremlinParser.TraversalMethod_choose_Predicate_Traversal_TraversalContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_choose_Traversal(final GremlinParser.TraversalMethod_choose_TraversalContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_choose_Traversal_Traversal(final GremlinParser.TraversalMethod_choose_Traversal_TraversalContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_choose_Traversal_Traversal_Traversal(final GremlinParser.TraversalMethod_choose_Traversal_Traversal_TraversalContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_coalesce(final GremlinParser.TraversalMethod_coalesceContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_coin(final GremlinParser.TraversalMethod_coinContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_combine_Object(final GremlinParser.TraversalMethod_combine_ObjectContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_connectedComponent(final GremlinParser.TraversalMethod_connectedComponentContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_constant(final GremlinParser.TraversalMethod_constantContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_count_Empty(final GremlinParser.TraversalMethod_count_EmptyContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_count_Scope(final GremlinParser.TraversalMethod_count_ScopeContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_cyclicPath(final GremlinParser.TraversalMethod_cyclicPathContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_dedup_Scope_String(final GremlinParser.TraversalMethod_dedup_Scope_StringContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_dedup_String(final GremlinParser.TraversalMethod_dedup_StringContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_difference_Object(final GremlinParser.TraversalMethod_difference_ObjectContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_disjunct_Object(final GremlinParser.TraversalMethod_disjunct_ObjectContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_drop(final GremlinParser.TraversalMethod_dropContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_elementMap(final GremlinParser.TraversalMethod_elementMapContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_emit_Empty(final GremlinParser.TraversalMethod_emit_EmptyContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_emit_Predicate(final GremlinParser.TraversalMethod_emit_PredicateContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_emit_Traversal(final GremlinParser.TraversalMethod_emit_TraversalContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_filter_Predicate(final GremlinParser.TraversalMethod_filter_PredicateContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_filter_Traversal(final GremlinParser.TraversalMethod_filter_TraversalContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_flatMap(final GremlinParser.TraversalMethod_flatMapContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_fold_Empty(final GremlinParser.TraversalMethod_fold_EmptyContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_fold_Object_BiFunction(final GremlinParser.TraversalMethod_fold_Object_BiFunctionContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_from_String(final GremlinParser.TraversalMethod_from_StringContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_from_Vertex(final GremlinParser.TraversalMethod_from_VertexContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_from_Traversal(final GremlinParser.TraversalMethod_from_TraversalContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_group_Empty(final GremlinParser.TraversalMethod_group_EmptyContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_group_String(final GremlinParser.TraversalMethod_group_StringContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_groupCount_Empty(final GremlinParser.TraversalMethod_groupCount_EmptyContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_groupCount_String(final GremlinParser.TraversalMethod_groupCount_StringContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_has_String(final GremlinParser.TraversalMethod_has_StringContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_has_String_Object(final GremlinParser.TraversalMethod_has_String_ObjectContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_has_String_P(final GremlinParser.TraversalMethod_has_String_PContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_has_String_String_Object(final GremlinParser.TraversalMethod_has_String_String_ObjectContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_has_String_String_P(final GremlinParser.TraversalMethod_has_String_String_PContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_has_String_Traversal(final GremlinParser.TraversalMethod_has_String_TraversalContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_has_T_Object(final GremlinParser.TraversalMethod_has_T_ObjectContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_has_T_P(final GremlinParser.TraversalMethod_has_T_PContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_has_T_Traversal(final GremlinParser.TraversalMethod_has_T_TraversalContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_hasId_Object_Object(final GremlinParser.TraversalMethod_hasId_Object_ObjectContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_hasId_P(final GremlinParser.TraversalMethod_hasId_PContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_hasKey_P(final GremlinParser.TraversalMethod_hasKey_PContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_hasKey_String_String(final GremlinParser.TraversalMethod_hasKey_String_StringContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_hasLabel_P(final GremlinParser.TraversalMethod_hasLabel_PContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_hasLabel_String_String(final GremlinParser.TraversalMethod_hasLabel_String_StringContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_hasNot(final GremlinParser.TraversalMethod_hasNotContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_hasValue_Object_Object(final GremlinParser.TraversalMethod_hasValue_Object_ObjectContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_hasValue_P(final GremlinParser.TraversalMethod_hasValue_PContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_id(final GremlinParser.TraversalMethod_idContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_identity(final GremlinParser.TraversalMethod_identityContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_in(final GremlinParser.TraversalMethod_inContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_inE(final GremlinParser.TraversalMethod_inEContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_intersect_Object(final GremlinParser.TraversalMethod_intersect_ObjectContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_inV(final GremlinParser.TraversalMethod_inVContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_index(final GremlinParser.TraversalMethod_indexContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_inject(final GremlinParser.TraversalMethod_injectContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_is_Object(final GremlinParser.TraversalMethod_is_ObjectContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_is_P(final GremlinParser.TraversalMethod_is_PContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_conjoin_String(final GremlinParser.TraversalMethod_conjoin_StringContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_key(final GremlinParser.TraversalMethod_keyContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_label(final GremlinParser.TraversalMethod_labelContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_limit_Scope_long(final GremlinParser.TraversalMethod_limit_Scope_longContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_limit_long(final GremlinParser.TraversalMethod_limit_longContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_local(final GremlinParser.TraversalMethod_localContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_loops_Empty(final GremlinParser.TraversalMethod_loops_EmptyContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_loops_String(final GremlinParser.TraversalMethod_loops_StringContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_map(final GremlinParser.TraversalMethod_mapContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_match(final GremlinParser.TraversalMethod_matchContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_math(final GremlinParser.TraversalMethod_mathContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_max_Empty(final GremlinParser.TraversalMethod_max_EmptyContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_max_Scope(final GremlinParser.TraversalMethod_max_ScopeContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_mean_Empty(final GremlinParser.TraversalMethod_mean_EmptyContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_mean_Scope(final GremlinParser.TraversalMethod_mean_ScopeContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_merge_Object(final GremlinParser.TraversalMethod_merge_ObjectContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_min_Empty(final GremlinParser.TraversalMethod_min_EmptyContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_min_Scope(final GremlinParser.TraversalMethod_min_ScopeContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_none_P(final GremlinParser.TraversalMethod_none_PContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_not(final GremlinParser.TraversalMethod_notContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_option_Predicate_Traversal(final GremlinParser.TraversalMethod_option_Predicate_TraversalContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_option_Merge_Map(final GremlinParser.TraversalMethod_option_Merge_MapContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_option_Merge_Map_Cardinality(final GremlinParser.TraversalMethod_option_Merge_Map_CardinalityContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_option_Object_Traversal(final GremlinParser.TraversalMethod_option_Object_TraversalContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_option_Traversal(final GremlinParser.TraversalMethod_option_TraversalContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_option_Merge_Traversal(final GremlinParser.TraversalMethod_option_Merge_TraversalContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_optional(final GremlinParser.TraversalMethod_optionalContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_or(final GremlinParser.TraversalMethod_orContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_order_Empty(final GremlinParser.TraversalMethod_order_EmptyContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_order_Scope(final GremlinParser.TraversalMethod_order_ScopeContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_otherV(final GremlinParser.TraversalMethod_otherVContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_out(final GremlinParser.TraversalMethod_outContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_outE(final GremlinParser.TraversalMethod_outEContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_outV(final GremlinParser.TraversalMethod_outVContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_pageRank_Empty(final GremlinParser.TraversalMethod_pageRank_EmptyContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_pageRank_double(final GremlinParser.TraversalMethod_pageRank_doubleContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_path(final GremlinParser.TraversalMethod_pathContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_peerPressure(final GremlinParser.TraversalMethod_peerPressureContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_product_Object(final GremlinParser.TraversalMethod_product_ObjectContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_profile_Empty(final GremlinParser.TraversalMethod_profile_EmptyContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_profile_String(final GremlinParser.TraversalMethod_profile_StringContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_project(final GremlinParser.TraversalMethod_projectContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_properties(final GremlinParser.TraversalMethod_propertiesContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_property_Cardinality_Object_Object_Object(final GremlinParser.TraversalMethod_property_Cardinality_Object_Object_ObjectContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_property_Object_Object_Object(final GremlinParser.TraversalMethod_property_Object_Object_ObjectContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_property_Object(final GremlinParser.TraversalMethod_property_ObjectContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_property_Cardinality_Object(final GremlinParser.TraversalMethod_property_Cardinality_ObjectContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_propertyMap(final GremlinParser.TraversalMethod_propertyMapContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_range_Scope_long_long(final GremlinParser.TraversalMethod_range_Scope_long_longContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_range_long_long(final GremlinParser.TraversalMethod_range_long_longContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_read(final GremlinParser.TraversalMethod_readContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_repeat_String_Traversal(final GremlinParser.TraversalMethod_repeat_String_TraversalContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_repeat_Traversal(final GremlinParser.TraversalMethod_repeat_TraversalContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_reverse_Empty(final GremlinParser.TraversalMethod_reverse_EmptyContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_sack_BiFunction(final GremlinParser.TraversalMethod_sack_BiFunctionContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_sack_Empty(final GremlinParser.TraversalMethod_sack_EmptyContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_sample_Scope_int(final GremlinParser.TraversalMethod_sample_Scope_intContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_sample_int(final GremlinParser.TraversalMethod_sample_intContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_select_Column(final GremlinParser.TraversalMethod_select_ColumnContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_select_Pop_String(final GremlinParser.TraversalMethod_select_Pop_StringContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_select_Pop_String_String_String(final GremlinParser.TraversalMethod_select_Pop_String_String_StringContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_select_Pop_Traversal(final GremlinParser.TraversalMethod_select_Pop_TraversalContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_select_String(final GremlinParser.TraversalMethod_select_StringContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_select_String_String_String(final GremlinParser.TraversalMethod_select_String_String_StringContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_select_Traversal(final GremlinParser.TraversalMethod_select_TraversalContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_shortestPath(final GremlinParser.TraversalMethod_shortestPathContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_sideEffect(final GremlinParser.TraversalMethod_sideEffectContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_simplePath(final GremlinParser.TraversalMethod_simplePathContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_skip_Scope_long(final GremlinParser.TraversalMethod_skip_Scope_longContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_skip_long(final GremlinParser.TraversalMethod_skip_longContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_store(final GremlinParser.TraversalMethod_storeContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_subgraph(final GremlinParser.TraversalMethod_subgraphContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_sum_Empty(final GremlinParser.TraversalMethod_sum_EmptyContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_sum_Scope(final GremlinParser.TraversalMethod_sum_ScopeContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_tail_Empty(final GremlinParser.TraversalMethod_tail_EmptyContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_tail_Scope(final GremlinParser.TraversalMethod_tail_ScopeContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_tail_Scope_long(final GremlinParser.TraversalMethod_tail_Scope_longContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_tail_long(final GremlinParser.TraversalMethod_tail_longContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_fail_Empty(final GremlinParser.TraversalMethod_fail_EmptyContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_fail_String(final GremlinParser.TraversalMethod_fail_StringContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_timeLimit(final GremlinParser.TraversalMethod_timeLimitContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_times(final GremlinParser.TraversalMethod_timesContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_to_Direction_String(final GremlinParser.TraversalMethod_to_Direction_StringContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_to_String(final GremlinParser.TraversalMethod_to_StringContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_to_Vertex(final GremlinParser.TraversalMethod_to_VertexContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_to_Traversal(final GremlinParser.TraversalMethod_to_TraversalContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_toE(final GremlinParser.TraversalMethod_toEContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_toV(final GremlinParser.TraversalMethod_toVContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_tree_Empty(final GremlinParser.TraversalMethod_tree_EmptyContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_tree_String(final GremlinParser.TraversalMethod_tree_StringContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_unfold(final GremlinParser.TraversalMethod_unfoldContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_union(final GremlinParser.TraversalMethod_unionContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_until_Predicate(final GremlinParser.TraversalMethod_until_PredicateContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_until_Traversal(final GremlinParser.TraversalMethod_until_TraversalContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_value(final GremlinParser.TraversalMethod_valueContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_valueMap_String(final GremlinParser.TraversalMethod_valueMap_StringContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_valueMap_boolean_String(final GremlinParser.TraversalMethod_valueMap_boolean_StringContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_values(final GremlinParser.TraversalMethod_valuesContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_where_P(final GremlinParser.TraversalMethod_where_PContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_where_String_P(final GremlinParser.TraversalMethod_where_String_PContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_where_Traversal(final GremlinParser.TraversalMethod_where_TraversalContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_with_String(final GremlinParser.TraversalMethod_with_StringContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_with_String_Object(final GremlinParser.TraversalMethod_with_String_ObjectContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_write(final GremlinParser.TraversalMethod_writeContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_element(final GremlinParser.TraversalMethod_elementContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_call_string(final GremlinParser.TraversalMethod_call_stringContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_call_string_map(final GremlinParser.TraversalMethod_call_string_mapContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_call_string_traversal(final GremlinParser.TraversalMethod_call_string_traversalContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_call_string_map_traversal(final GremlinParser.TraversalMethod_call_string_map_traversalContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_concat_Traversal_Traversal(final GremlinParser.TraversalMethod_concat_Traversal_TraversalContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_concat_String(final GremlinParser.TraversalMethod_concat_StringContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_asString_Empty(final GremlinParser.TraversalMethod_asString_EmptyContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_asString_Scope(final GremlinParser.TraversalMethod_asString_ScopeContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_format_String(final GremlinParser.TraversalMethod_format_StringContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_toUpper_Empty(final GremlinParser.TraversalMethod_toUpper_EmptyContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_toUpper_Scope(final GremlinParser.TraversalMethod_toUpper_ScopeContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_toLower_Empty(final GremlinParser.TraversalMethod_toLower_EmptyContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_toLower_Scope(final GremlinParser.TraversalMethod_toLower_ScopeContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_length_Empty(final GremlinParser.TraversalMethod_length_EmptyContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_length_Scope(final GremlinParser.TraversalMethod_length_ScopeContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_trim_Empty(final GremlinParser.TraversalMethod_trim_EmptyContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_trim_Scope(final GremlinParser.TraversalMethod_trim_ScopeContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_lTrim_Empty(final GremlinParser.TraversalMethod_lTrim_EmptyContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_lTrim_Scope(final GremlinParser.TraversalMethod_lTrim_ScopeContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_rTrim_Empty(final GremlinParser.TraversalMethod_rTrim_EmptyContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_rTrim_Scope(final GremlinParser.TraversalMethod_rTrim_ScopeContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_replace_String_String(final GremlinParser.TraversalMethod_replace_String_StringContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_replace_Scope_String_String(final GremlinParser.TraversalMethod_replace_Scope_String_StringContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_split_String(final GremlinParser.TraversalMethod_split_StringContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_split_Scope_String(final GremlinParser.TraversalMethod_split_Scope_StringContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_substring_int(final GremlinParser.TraversalMethod_substring_intContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_substring_Scope_int(final GremlinParser.TraversalMethod_substring_Scope_intContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_substring_int_int(final GremlinParser.TraversalMethod_substring_int_intContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_substring_Scope_int_int(final GremlinParser.TraversalMethod_substring_Scope_int_intContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_asDate(final GremlinParser.TraversalMethod_asDateContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_dateAdd(final GremlinParser.TraversalMethod_dateAddContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_dateDiff_Traversal(final GremlinParser.TraversalMethod_dateDiff_TraversalContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalMethod_dateDiff_Date(final GremlinParser.TraversalMethod_dateDiff_DateContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitStructureVertex(final GremlinParser.StructureVertexContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalStrategy(final GremlinParser.TraversalStrategyContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalScope(final GremlinParser.TraversalScopeContext ctx) {
        appendExplicitNaming(ctx.getText(), Scope.class.getSimpleName());
        return null;
    }

    @Override
    public Void visitTraversalToken(final GremlinParser.TraversalTokenContext ctx) {
        appendExplicitNaming(ctx.getText(), T.class.getSimpleName());
        return null;
    }

    @Override
    public Void visitTraversalMerge(final GremlinParser.TraversalMergeContext ctx) {
        appendExplicitNaming(ctx.getText(), Merge.class.getSimpleName());
        return null;
    }

    @Override
    public Void visitTraversalOrder(final GremlinParser.TraversalOrderContext ctx) {
        appendExplicitNaming(ctx.getText(), Order.class.getSimpleName());
        return null;
    }

    @Override
    public Void visitTraversalBarrier(final GremlinParser.TraversalBarrierContext ctx) {
        appendExplicitNaming(ctx.getText(), SackFunctions.Barrier.class.getSimpleName());
        return null;
    }

    @Override
    public Void visitTraversalDirection(final GremlinParser.TraversalDirectionContext ctx) {
        appendExplicitNaming(ctx.getText(), Direction.class.getSimpleName());
        return null;
    }

    @Override
    public Void visitTraversalCardinality(final GremlinParser.TraversalCardinalityContext ctx) {
        // handle the enum style of cardinality if there is one child, otherwise it's the function call style
        if (ctx.getChildCount() == 1)
            appendExplicitNaming(ctx.getText(), Cardinality.class.getSimpleName());
        else {
            appendExplicitNaming(ctx.getChild(0).getText(), Cardinality.class.getSimpleName());
            appendStepOpen();
            visit(ctx.getChild(2));
            appendStepClose();
        }

        return null;
    }

    @Override
    public Void visitTraversalColumn(final GremlinParser.TraversalColumnContext ctx) {
        appendExplicitNaming(ctx.getText(), Column.class.getSimpleName());
        return null;
    }

    @Override
    public Void visitTraversalPop(final GremlinParser.TraversalPopContext ctx) {
        appendExplicitNaming(ctx.getText(), Pop.class.getSimpleName());
        return null;
    }

    @Override
    public Void visitTraversalOperator(final GremlinParser.TraversalOperatorContext ctx) {
        appendExplicitNaming(ctx.getText(), Operator.class.getSimpleName());
        return null;
    }

    @Override
    public Void visitTraversalPick(final GremlinParser.TraversalPickContext ctx) {
        appendExplicitNaming(ctx.getText(), Pick.class.getSimpleName());
        return null;
    }

    @Override
    public Void visitTraversalDT(final GremlinParser.TraversalDTContext ctx) {
        appendExplicitNaming(ctx.getText(), DT.class.getSimpleName());
        return null;
    }

    @Override
    public Void visitTraversalPredicate(final GremlinParser.TraversalPredicateContext ctx) {
        switch(ctx.getChildCount()) {
            case 1:
                // handle simple predicate
                visit(ctx.getChild(0));
                break;
            case 5:
                // handle negate of P
                visit(ctx.getChild(0));
                sb.append(".").append(processGremlinSymbol("negate")).append("()");
                break;
            case 6:
                // handle and/or predicates
                final int childIndexOfParameterOperator = 2;
                final int childIndexOfCaller = 0;
                final int childIndexOfArgument = 4;

                visit(ctx.getChild(childIndexOfCaller));
                sb.append(".").append(processGremlinSymbol(ctx.getChild(childIndexOfParameterOperator).getText())).append("(");
                visit(ctx.getChild(childIndexOfArgument));
                sb.append(")");
                break;
        }
        return null;
    }

    @Override
    public Void visitTraversalTerminalMethod(final GremlinParser.TraversalTerminalMethodContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalSackMethod(final GremlinParser.TraversalSackMethodContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalSelfMethod(final GremlinParser.TraversalSelfMethodContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalComparator(final GremlinParser.TraversalComparatorContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalFunction(final GremlinParser.TraversalFunctionContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalBiFunction(final GremlinParser.TraversalBiFunctionContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalPredicate_eq(final GremlinParser.TraversalPredicate_eqContext ctx) {
        visitP(ctx, P.class, "eq");
        return null;
    }

    @Override
    public Void visitTraversalPredicate_neq(final GremlinParser.TraversalPredicate_neqContext ctx) {
        visitP(ctx, P.class, "neq");
        return null;
    }

    protected void visitP(final ParserRuleContext ctx, final Class<?> clazzOfP, final String methodName) {
        sb.append(clazzOfP.getSimpleName());
        appendStepSeparator();
        sb.append(processGremlinSymbol(methodName));
        appendStepOpen();

        // if there are commas then there are multiple arguments that are being handled literally e.g. between
        final long commas = ctx.children.stream().filter(t -> t instanceof TerminalNode && t.getText().equals(",")).count();
        if (commas > 0) {
            // the number of commas indicates how many arguments there are. for each argument, visit the child
            for (int ix = 0; ix < (commas * 2) + 1; ix+=2) {
                visit(ctx.getChild(ix + 2));
                if (ix < commas) {
                    appendArgumentSeparator();
                }
            }
        } else {
            // there is only one argument, visit the child
            if (ctx.getChildCount() > 3)
                visit(ctx.getChild(2));
        }
        appendStepClose();
    }

    @Override
    public Void visitTraversalPredicate_lt(final GremlinParser.TraversalPredicate_ltContext ctx) {
        visitP(ctx, P.class, "lt");
        return null;
    }

    @Override
    public Void visitTraversalPredicate_lte(final GremlinParser.TraversalPredicate_lteContext ctx) {
        visitP(ctx, P.class, "lte");
        return null;
    }

    @Override
    public Void visitTraversalPredicate_gt(final GremlinParser.TraversalPredicate_gtContext ctx) {
        visitP(ctx, P.class, "gt");
        return null;
    }

    @Override
    public Void visitTraversalPredicate_gte(final GremlinParser.TraversalPredicate_gteContext ctx) {
        visitP(ctx, P.class, "gte");
        return null;
    }

    @Override
    public Void visitTraversalPredicate_inside(final GremlinParser.TraversalPredicate_insideContext ctx) {
        visitP(ctx, P.class, "inside");
        return null;
    }

    @Override
    public Void visitTraversalPredicate_outside(final GremlinParser.TraversalPredicate_outsideContext ctx) {
        visitP(ctx, P.class, "outside");
        return null;
    }

    @Override
    public Void visitTraversalPredicate_between(final GremlinParser.TraversalPredicate_betweenContext ctx) {
        visitP(ctx, P.class, "between");
        return null;
    }

    @Override
    public Void visitTraversalPredicate_within(final GremlinParser.TraversalPredicate_withinContext ctx) {
        visitP(ctx, P.class, "within");
        return null;
    }

    @Override
    public Void visitTraversalPredicate_without(final GremlinParser.TraversalPredicate_withoutContext ctx) {
        visitP(ctx, P.class, "without");
        return null;
    }

    @Override
    public Void visitTraversalPredicate_not(final GremlinParser.TraversalPredicate_notContext ctx) {
        visitP(ctx, P.class, "not");
        return null;
    }

    @Override
    public Void visitTraversalPredicate_containing(final GremlinParser.TraversalPredicate_containingContext ctx) {
        visitP(ctx, TextP.class, "containing");
        return null;
    }

    @Override
    public Void visitTraversalPredicate_notContaining(final GremlinParser.TraversalPredicate_notContainingContext ctx) {
        visitP(ctx, TextP.class, "notContaining");
        return null;
    }

    @Override
    public Void visitTraversalPredicate_startingWith(final GremlinParser.TraversalPredicate_startingWithContext ctx) {
        visitP(ctx, TextP.class, "startingWith");
        return null;
    }

    @Override
    public Void visitTraversalPredicate_notStartingWith(final GremlinParser.TraversalPredicate_notStartingWithContext ctx) {
        visitP(ctx, TextP.class, "notStartingWith");
        return null;
    }

    @Override
    public Void visitTraversalPredicate_endingWith(final GremlinParser.TraversalPredicate_endingWithContext ctx) {
        visitP(ctx, TextP.class, "endingWith");
        return null;
    }

    @Override
    public Void visitTraversalPredicate_notEndingWith(final GremlinParser.TraversalPredicate_notEndingWithContext ctx) {
        visitP(ctx, TextP.class, "notEndingWith");
        return null;
    }

    @Override
    public Void visitTraversalPredicate_regex(final GremlinParser.TraversalPredicate_regexContext ctx) {
        visitP(ctx, TextP.class, "regex");
        return null;
    }

    @Override
    public Void visitTraversalPredicate_notRegex(final GremlinParser.TraversalPredicate_notRegexContext ctx) {
        visitP(ctx, TextP.class, "notRegex");
        return null;
    }

    @Override
    public Void visitTraversalTerminalMethod_explain(final GremlinParser.TraversalTerminalMethod_explainContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalTerminalMethod_hasNext(final GremlinParser.TraversalTerminalMethod_hasNextContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalTerminalMethod_iterate(final GremlinParser.TraversalTerminalMethod_iterateContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalTerminalMethod_tryNext(final GremlinParser.TraversalTerminalMethod_tryNextContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalTerminalMethod_next(final GremlinParser.TraversalTerminalMethod_nextContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalTerminalMethod_toList(final GremlinParser.TraversalTerminalMethod_toListContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalTerminalMethod_toSet(final GremlinParser.TraversalTerminalMethod_toSetContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalTerminalMethod_toBulkSet(final GremlinParser.TraversalTerminalMethod_toBulkSetContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalSelfMethod_discard(final GremlinParser.TraversalSelfMethod_discardContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitWithOptionKeys(final GremlinParser.WithOptionKeysContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitConnectedComponentConstants(final GremlinParser.ConnectedComponentConstantsContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitPageRankConstants(final GremlinParser.PageRankConstantsContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitPeerPressureConstants(final GremlinParser.PeerPressureConstantsContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitShortestPathConstants(final GremlinParser.ShortestPathConstantsContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitWithOptionsValues(final GremlinParser.WithOptionsValuesContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitIoOptionsKeys(final GremlinParser.IoOptionsKeysContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitIoOptionsValues(final GremlinParser.IoOptionsValuesContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitConnectedComponentConstants_component(final GremlinParser.ConnectedComponentConstants_componentContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitConnectedComponentConstants_edges(final GremlinParser.ConnectedComponentConstants_edgesContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitConnectedComponentConstants_propertyName(final GremlinParser.ConnectedComponentConstants_propertyNameContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitPageRankConstants_edges(final GremlinParser.PageRankConstants_edgesContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitPageRankConstants_times(final GremlinParser.PageRankConstants_timesContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitPageRankConstants_propertyName(final GremlinParser.PageRankConstants_propertyNameContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitPeerPressureConstants_edges(final GremlinParser.PeerPressureConstants_edgesContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitPeerPressureConstants_times(final GremlinParser.PeerPressureConstants_timesContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitPeerPressureConstants_propertyName(final GremlinParser.PeerPressureConstants_propertyNameContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitShortestPathConstants_target(final GremlinParser.ShortestPathConstants_targetContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitShortestPathConstants_edges(final GremlinParser.ShortestPathConstants_edgesContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitShortestPathConstants_distance(final GremlinParser.ShortestPathConstants_distanceContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitShortestPathConstants_maxDistance(final GremlinParser.ShortestPathConstants_maxDistanceContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitShortestPathConstants_includeEdges(final GremlinParser.ShortestPathConstants_includeEdgesContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitWithOptionsConstants_tokens(final GremlinParser.WithOptionsConstants_tokensContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitWithOptionsConstants_none(final GremlinParser.WithOptionsConstants_noneContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitWithOptionsConstants_ids(final GremlinParser.WithOptionsConstants_idsContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitWithOptionsConstants_labels(final GremlinParser.WithOptionsConstants_labelsContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitWithOptionsConstants_keys(final GremlinParser.WithOptionsConstants_keysContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitWithOptionsConstants_values(final GremlinParser.WithOptionsConstants_valuesContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitWithOptionsConstants_all(final GremlinParser.WithOptionsConstants_allContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitWithOptionsConstants_indexer(final GremlinParser.WithOptionsConstants_indexerContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitWithOptionsConstants_list(final GremlinParser.WithOptionsConstants_listContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitWithOptionsConstants_map(final GremlinParser.WithOptionsConstants_mapContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitIoOptionsConstants_reader(final GremlinParser.IoOptionsConstants_readerContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitIoOptionsConstants_writer(final GremlinParser.IoOptionsConstants_writerContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitIoOptionsConstants_gryo(final GremlinParser.IoOptionsConstants_gryoContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitIoOptionsConstants_graphson(final GremlinParser.IoOptionsConstants_graphsonContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitIoOptionsConstants_graphml(final GremlinParser.IoOptionsConstants_graphmlContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitConnectedComponentStringConstant(final GremlinParser.ConnectedComponentStringConstantContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitPageRankStringConstant(final GremlinParser.PageRankStringConstantContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitPeerPressureStringConstant(final GremlinParser.PeerPressureStringConstantContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitShortestPathStringConstant(final GremlinParser.ShortestPathStringConstantContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitWithOptionsStringConstant(final GremlinParser.WithOptionsStringConstantContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitIoOptionsStringConstant(final GremlinParser.IoOptionsStringConstantContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitBooleanArgument(final GremlinParser.BooleanArgumentContext ctx) {
        if (ctx.booleanLiteral() != null)
            visitBooleanLiteral(ctx.booleanLiteral());
        else
            visitVariable(ctx.variable());

        return null;
    }

    @Override
    public Void visitIntegerArgument(final GremlinParser.IntegerArgumentContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitFloatArgument(final GremlinParser.FloatArgumentContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitStringArgument(final GremlinParser.StringArgumentContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitStringNullableArgument(final GremlinParser.StringNullableArgumentContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitDateArgument(final GremlinParser.DateArgumentContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitGenericLiteralArgument(final GremlinParser.GenericLiteralArgumentContext ctx) {
        if (ctx.genericLiteral() != null)
            visitGenericLiteral(ctx.genericLiteral());
        else
            visitVariable(ctx.variable());

        return null;
    }

    @Override
    public Void visitGenericLiteralListArgument(final GremlinParser.GenericLiteralListArgumentContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitGenericLiteralMapArgument(final GremlinParser.GenericLiteralMapArgumentContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitGenericLiteralMapNullableArgument(final GremlinParser.GenericLiteralMapNullableArgumentContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitNullableGenericLiteralMap(final GremlinParser.NullableGenericLiteralMapContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitStructureVertexArgument(final GremlinParser.StructureVertexArgumentContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalStrategyList(final GremlinParser.TraversalStrategyListContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitTraversalStrategyExpr(final GremlinParser.TraversalStrategyExprContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitNestedTraversalList(final GremlinParser.NestedTraversalListContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitNestedTraversalExpr(final GremlinParser.NestedTraversalExprContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitGenericLiteralVarargs(final GremlinParser.GenericLiteralVarargsContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitGenericLiteralList(final GremlinParser.GenericLiteralListContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitGenericLiteralExpr(final GremlinParser.GenericLiteralExprContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitGenericLiteralRange(final GremlinParser.GenericLiteralRangeContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitGenericLiteralCollection(final GremlinParser.GenericLiteralCollectionContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitGenericLiteralSet(GremlinParser.GenericLiteralSetContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitStringLiteralVarargs(final GremlinParser.StringLiteralVarargsContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitStringLiteralVarargsLiterals(final GremlinParser.StringLiteralVarargsLiteralsContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitStringLiteralList(final GremlinParser.StringLiteralListContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitStringLiteralExpr(final GremlinParser.StringLiteralExprContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitGenericLiteral(final GremlinParser.GenericLiteralContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitGenericLiteralMap(final GremlinParser.GenericLiteralMapContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitMapEntry(final GremlinParser.MapEntryContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitStringLiteral(final GremlinParser.StringLiteralContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitStringNullableLiteral(final GremlinParser.StringNullableLiteralContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitIntegerLiteral(final GremlinParser.IntegerLiteralContext ctx) {
        sb.append(ctx.getText().toLowerCase());
        return null;
    }

    @Override
    public Void visitFloatLiteral(final GremlinParser.FloatLiteralContext ctx) {
        sb.append(ctx.getText().toLowerCase());
        return null;
    }

    @Override
    public Void visitNumericLiteral(final GremlinParser.NumericLiteralContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitBooleanLiteral(final GremlinParser.BooleanLiteralContext ctx) {
        sb.append(ctx.getText());
        return null;
    }

    @Override
    public Void visitDateLiteral(final GremlinParser.DateLiteralContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitNullLiteral(final GremlinParser.NullLiteralContext ctx) {
        sb.append(ctx.getText());
        return null;
    }

    @Override
    public Void visitNanLiteral(final GremlinParser.NanLiteralContext ctx) {
        sb.append(ctx.getText());
        return null;
    }

    @Override
    public Void visitInfLiteral(final GremlinParser.InfLiteralContext ctx) {
        sb.append(ctx.getText());
        return null;
    }

    @Override
    public Void visitVariable(final GremlinParser.VariableContext ctx) {
        final String var = ctx.getText();
        sb.append(var);
        parameters.add(var);
        return null;
    }

    @Override
    public Void visitTraversalSourceSelfMethod_withoutStrategies(final GremlinParser.TraversalSourceSelfMethod_withoutStrategiesContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitConfiguration(final GremlinParser.ConfigurationContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitClassTypeList(final GremlinParser.ClassTypeListContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitClassTypeExpr(final GremlinParser.ClassTypeExprContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitClassType(final GremlinParser.ClassTypeContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Void visitKeyword(final GremlinParser.KeywordContext ctx) {
        final String keyword = ctx.getText();

        // translate differently based on the context of the keyword's parent.
        if (ctx.getParent() instanceof GremlinParser.MapEntryContext || ctx.getParent() instanceof GremlinParser.ConfigurationContext) {
            // if the keyword is a key in a map, then it's a string literal essentially
            sb.append(keyword);
        } else {
            // in all other cases it's used more like "new Class()"
            sb.append(keyword).append(" ");
        }

        return null;
    }

    @Override
    public Void visitTerminal(final TerminalNode node) {
        // skip EOF node
        if (null == node || node.getSymbol().getType() == -1) return null;

        final String terminal = node.getSymbol().getText();
        switch (terminal) {
            case "(":
                appendStepOpen();
                break;
            case ")":
                appendStepClose();
                break;
            case ",":
                appendArgumentSeparator();
                break;
            case ".":
                appendStepSeparator();
                break;
            case "new":
                // seems we still sometimes interpret this as a TerminalNode like when newing up a class
                // which is optional syntax and will go away in the future.
                sb.append("new");
                if (!(node.getParent() instanceof GremlinParser.MapEntryContext))
                    sb.append(" "); // includes a space for when not use in context of a Map entry key...one off
                break;
            default:
                sb.append(processGremlinSymbol(terminal));
        }
        return null;
    }

    protected void appendExplicitNaming(final String txt, final String prefix) {
        if (!txt.startsWith(prefix + ".")) {
            sb.append(processGremlinSymbol(prefix)).append(".");
            sb.append(processGremlinSymbol(txt));
        } else {
            final String[] split = txt.split("\\.");
            sb.append(processGremlinSymbol(split[0])).append(".");
            sb.append(processGremlinSymbol(split[1]));
        }
    }

    protected void appendAnonymousSpawn() {
        sb.append("__.");
    }
}
