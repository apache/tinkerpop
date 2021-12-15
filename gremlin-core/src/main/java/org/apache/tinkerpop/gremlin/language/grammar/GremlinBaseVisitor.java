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

import org.antlr.v4.runtime.tree.AbstractParseTreeVisitor;
import org.antlr.v4.runtime.tree.ParseTree;

/**
 * This class provides implementation of {@link GremlinVisitor}, where each method will throw
 * {@code UnsupportedOperationException}. All the visitor class will extends this class, so that if there is method
 * that are not manually implemented, and called, an exception will be thrown to help us catch bugs.
 *
 * @param <T> The return type of the visit operation. Use {@link Void} for
 * operations with no return type.
 */
public class GremlinBaseVisitor<T> extends AbstractParseTreeVisitor<T> implements GremlinVisitor<T> {
	private void notImplemented(ParseTree ctx) {
		final String className = (ctx != null)? ctx.getClass().getName() : "";
		throw new UnsupportedOperationException("Method not implemented for context class " + className);
	}

	@Override public T visitQueryList(final GremlinParser.QueryListContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitQuery(final GremlinParser.QueryContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitEmptyQuery(final GremlinParser.EmptyQueryContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalSource(final GremlinParser.TraversalSourceContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTransactionPart(final GremlinParser.TransactionPartContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitRootTraversal(final GremlinParser.RootTraversalContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalSourceSelfMethod(final GremlinParser.TraversalSourceSelfMethodContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalSourceSelfMethod_withBulk(final GremlinParser.TraversalSourceSelfMethod_withBulkContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalSourceSelfMethod_withPath(final GremlinParser.TraversalSourceSelfMethod_withPathContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalSourceSelfMethod_withSack(final GremlinParser.TraversalSourceSelfMethod_withSackContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalSourceSelfMethod_withSideEffect(final GremlinParser.TraversalSourceSelfMethod_withSideEffectContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalSourceSelfMethod_withStrategies(final GremlinParser.TraversalSourceSelfMethod_withStrategiesContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalSourceSelfMethod_with(final GremlinParser.TraversalSourceSelfMethod_withContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalSourceSpawnMethod(final GremlinParser.TraversalSourceSpawnMethodContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalSourceSpawnMethod_addE(final GremlinParser.TraversalSourceSpawnMethod_addEContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalSourceSpawnMethod_addV(final GremlinParser.TraversalSourceSpawnMethod_addVContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalSourceSpawnMethod_E(final GremlinParser.TraversalSourceSpawnMethod_EContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalSourceSpawnMethod_V(final GremlinParser.TraversalSourceSpawnMethod_VContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override
	public T visitTraversalSourceSpawnMethod_inject(final GremlinParser.TraversalSourceSpawnMethod_injectContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override
	public T visitTraversalSourceSpawnMethod_io(final GremlinParser.TraversalSourceSpawnMethod_ioContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitChainedTraversal(final GremlinParser.ChainedTraversalContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override
	public T visitChainedParentOfGraphTraversal(final GremlinParser.ChainedParentOfGraphTraversalContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitNestedTraversal(final GremlinParser.NestedTraversalContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTerminatedTraversal(final GremlinParser.TerminatedTraversalContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod(final GremlinParser.TraversalMethodContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_V(final GremlinParser.TraversalMethod_VContext ctx) { notImplemented(ctx); return null; }

	/**
	 * {@inheritDoc}
	 */
	@Override
	public T visitTraversalMethod_addE_String(final GremlinParser.TraversalMethod_addE_StringContext ctx) {
		notImplemented(ctx); return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public T visitTraversalMethod_addE_Traversal(final GremlinParser.TraversalMethod_addE_TraversalContext ctx) {
		notImplemented(ctx); return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_addV_Empty(final GremlinParser.TraversalMethod_addV_EmptyContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_addV_String(final GremlinParser.TraversalMethod_addV_StringContext ctx) { notImplemented(ctx); return null; }

	/**
	 * {@inheritDoc}
	 */
	@Override
	public T visitTraversalMethod_addV_Traversal(final GremlinParser.TraversalMethod_addV_TraversalContext ctx) {
		notImplemented(ctx); return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_aggregate_String(final GremlinParser.TraversalMethod_aggregate_StringContext ctx) { notImplemented(ctx); return null; }

	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_aggregate_Scope_String(final GremlinParser.TraversalMethod_aggregate_Scope_StringContext ctx) { notImplemented(ctx); return null; }

	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_and(final GremlinParser.TraversalMethod_andContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_as(final GremlinParser.TraversalMethod_asContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_barrier_Consumer(final GremlinParser.TraversalMethod_barrier_ConsumerContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_barrier_Empty(final GremlinParser.TraversalMethod_barrier_EmptyContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_barrier_int(final GremlinParser.TraversalMethod_barrier_intContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_both(final GremlinParser.TraversalMethod_bothContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_bothE(final GremlinParser.TraversalMethod_bothEContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_bothV(final GremlinParser.TraversalMethod_bothVContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_branch(final GremlinParser.TraversalMethod_branchContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_by_Comparator(final GremlinParser.TraversalMethod_by_ComparatorContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_by_Empty(final GremlinParser.TraversalMethod_by_EmptyContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_by_Function(final GremlinParser.TraversalMethod_by_FunctionContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_by_Function_Comparator(final GremlinParser.TraversalMethod_by_Function_ComparatorContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_by_Order(final GremlinParser.TraversalMethod_by_OrderContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_by_String(final GremlinParser.TraversalMethod_by_StringContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_by_String_Comparator(final GremlinParser.TraversalMethod_by_String_ComparatorContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_by_T(final GremlinParser.TraversalMethod_by_TContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_by_Traversal(final GremlinParser.TraversalMethod_by_TraversalContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_by_Traversal_Comparator(final GremlinParser.TraversalMethod_by_Traversal_ComparatorContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_cap(final GremlinParser.TraversalMethod_capContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_choose_Function(final GremlinParser.TraversalMethod_choose_FunctionContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_choose_Predicate_Traversal(final GremlinParser.TraversalMethod_choose_Predicate_TraversalContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_choose_Predicate_Traversal_Traversal(final GremlinParser.TraversalMethod_choose_Predicate_Traversal_TraversalContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_choose_Traversal(final GremlinParser.TraversalMethod_choose_TraversalContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_choose_Traversal_Traversal(final GremlinParser.TraversalMethod_choose_Traversal_TraversalContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_choose_Traversal_Traversal_Traversal(final GremlinParser.TraversalMethod_choose_Traversal_Traversal_TraversalContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_coalesce(final GremlinParser.TraversalMethod_coalesceContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_coin(final GremlinParser.TraversalMethod_coinContext ctx) { notImplemented(ctx); return null; }

	@Override
	public T visitTraversalMethod_connectedComponent(final GremlinParser.TraversalMethod_connectedComponentContext ctx) {
		notImplemented(ctx); return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_constant(final GremlinParser.TraversalMethod_constantContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_count_Empty(final GremlinParser.TraversalMethod_count_EmptyContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_count_Scope(final GremlinParser.TraversalMethod_count_ScopeContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_cyclicPath(final GremlinParser.TraversalMethod_cyclicPathContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_dedup_Scope_String(final GremlinParser.TraversalMethod_dedup_Scope_StringContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_dedup_String(final GremlinParser.TraversalMethod_dedup_StringContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_drop(final GremlinParser.TraversalMethod_dropContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override
	public T visitTraversalMethod_elementMap(final GremlinParser.TraversalMethod_elementMapContext ctx) {
		return null;
	}
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_emit_Empty(final GremlinParser.TraversalMethod_emit_EmptyContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_emit_Predicate(final GremlinParser.TraversalMethod_emit_PredicateContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_emit_Traversal(final GremlinParser.TraversalMethod_emit_TraversalContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_filter_Predicate(final GremlinParser.TraversalMethod_filter_PredicateContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_filter_Traversal(final GremlinParser.TraversalMethod_filter_TraversalContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_flatMap(final GremlinParser.TraversalMethod_flatMapContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_fold_Empty(final GremlinParser.TraversalMethod_fold_EmptyContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_fold_Object_BiFunction(final GremlinParser.TraversalMethod_fold_Object_BiFunctionContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_from_String(final GremlinParser.TraversalMethod_from_StringContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_from_Traversal(final GremlinParser.TraversalMethod_from_TraversalContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_group_Empty(final GremlinParser.TraversalMethod_group_EmptyContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_group_String(final GremlinParser.TraversalMethod_group_StringContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_groupCount_Empty(final GremlinParser.TraversalMethod_groupCount_EmptyContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_groupCount_String(final GremlinParser.TraversalMethod_groupCount_StringContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_has_String(final GremlinParser.TraversalMethod_has_StringContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_has_String_Object(final GremlinParser.TraversalMethod_has_String_ObjectContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_has_String_P(final GremlinParser.TraversalMethod_has_String_PContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_has_String_String_Object(final GremlinParser.TraversalMethod_has_String_String_ObjectContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_has_String_String_P(final GremlinParser.TraversalMethod_has_String_String_PContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_has_String_Traversal(final GremlinParser.TraversalMethod_has_String_TraversalContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_has_T_Object(final GremlinParser.TraversalMethod_has_T_ObjectContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_has_T_P(final GremlinParser.TraversalMethod_has_T_PContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_has_T_Traversal(final GremlinParser.TraversalMethod_has_T_TraversalContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_hasId_Object_Object(final GremlinParser.TraversalMethod_hasId_Object_ObjectContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_hasId_P(final GremlinParser.TraversalMethod_hasId_PContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_hasKey_P(final GremlinParser.TraversalMethod_hasKey_PContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_hasKey_String_String(final GremlinParser.TraversalMethod_hasKey_String_StringContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_hasLabel_P(final GremlinParser.TraversalMethod_hasLabel_PContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_hasLabel_String_String(final GremlinParser.TraversalMethod_hasLabel_String_StringContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_hasNot(final GremlinParser.TraversalMethod_hasNotContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_hasValue_Object_Object(final GremlinParser.TraversalMethod_hasValue_Object_ObjectContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_hasValue_P(final GremlinParser.TraversalMethod_hasValue_PContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_id(final GremlinParser.TraversalMethod_idContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_identity(final GremlinParser.TraversalMethod_identityContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_in(final GremlinParser.TraversalMethod_inContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_inE(final GremlinParser.TraversalMethod_inEContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_inV(final GremlinParser.TraversalMethod_inVContext ctx) { notImplemented(ctx); return null; }

	@Override
	public T visitTraversalMethod_index(final GremlinParser.TraversalMethod_indexContext ctx) {
		notImplemented(ctx); return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_inject(final GremlinParser.TraversalMethod_injectContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_is_Object(final GremlinParser.TraversalMethod_is_ObjectContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_is_P(final GremlinParser.TraversalMethod_is_PContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_key(final GremlinParser.TraversalMethod_keyContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_label(final GremlinParser.TraversalMethod_labelContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_limit_Scope_long(final GremlinParser.TraversalMethod_limit_Scope_longContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_limit_long(final GremlinParser.TraversalMethod_limit_longContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_local(final GremlinParser.TraversalMethod_localContext ctx) { notImplemented(ctx); return null; }

	@Override
	public T visitTraversalMethod_loops_Empty(final GremlinParser.TraversalMethod_loops_EmptyContext ctx) {
		notImplemented(ctx); return null;
	}

	@Override
	public T visitTraversalMethod_loops_String(final GremlinParser.TraversalMethod_loops_StringContext ctx) {
		notImplemented(ctx); return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_map(final GremlinParser.TraversalMethod_mapContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_match(final GremlinParser.TraversalMethod_matchContext ctx) { notImplemented(ctx); return null; }

	/**
	 * {@inheritDoc}
	 */
	@Override
	public T visitTraversalMethod_math(final GremlinParser.TraversalMethod_mathContext ctx) {
		notImplemented(ctx); return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_max_Empty(final GremlinParser.TraversalMethod_max_EmptyContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_max_Scope(final GremlinParser.TraversalMethod_max_ScopeContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_mean_Empty(final GremlinParser.TraversalMethod_mean_EmptyContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_mean_Scope(final GremlinParser.TraversalMethod_mean_ScopeContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_min_Empty(final GremlinParser.TraversalMethod_min_EmptyContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_min_Scope(final GremlinParser.TraversalMethod_min_ScopeContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_not(final GremlinParser.TraversalMethod_notContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_option_Object_Traversal(final GremlinParser.TraversalMethod_option_Object_TraversalContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_option_Traversal(final GremlinParser.TraversalMethod_option_TraversalContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_optional(final GremlinParser.TraversalMethod_optionalContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_or(final GremlinParser.TraversalMethod_orContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_order_Empty(final GremlinParser.TraversalMethod_order_EmptyContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_order_Scope(final GremlinParser.TraversalMethod_order_ScopeContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_otherV(final GremlinParser.TraversalMethod_otherVContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_out(final GremlinParser.TraversalMethod_outContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_outE(final GremlinParser.TraversalMethod_outEContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_outV(final GremlinParser.TraversalMethod_outVContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_pageRank_Empty(final GremlinParser.TraversalMethod_pageRank_EmptyContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_pageRank_double(final GremlinParser.TraversalMethod_pageRank_doubleContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_path(final GremlinParser.TraversalMethod_pathContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_peerPressure(final GremlinParser.TraversalMethod_peerPressureContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_profile_Empty(final GremlinParser.TraversalMethod_profile_EmptyContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_profile_String(final GremlinParser.TraversalMethod_profile_StringContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_project(final GremlinParser.TraversalMethod_projectContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_properties(final GremlinParser.TraversalMethod_propertiesContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_property_Cardinality_Object_Object_Object(final GremlinParser.TraversalMethod_property_Cardinality_Object_Object_ObjectContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_property_Object_Object_Object(final GremlinParser.TraversalMethod_property_Object_Object_ObjectContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_property_Cardinality_Object(final GremlinParser.TraversalMethod_property_Cardinality_ObjectContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_property_Object(final GremlinParser.TraversalMethod_property_ObjectContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_propertyMap(final GremlinParser.TraversalMethod_propertyMapContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_range_Scope_long_long(final GremlinParser.TraversalMethod_range_Scope_long_longContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_range_long_long(final GremlinParser.TraversalMethod_range_long_longContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override
	public T visitTraversalMethod_read(final GremlinParser.TraversalMethod_readContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override
	public T visitTraversalMethod_repeat_String_Traversal(final GremlinParser.TraversalMethod_repeat_String_TraversalContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override
	public T visitTraversalMethod_repeat_Traversal(final GremlinParser.TraversalMethod_repeat_TraversalContext ctx) {
		notImplemented(ctx); return null;
	}
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_sack_BiFunction(final GremlinParser.TraversalMethod_sack_BiFunctionContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_sack_Empty(final GremlinParser.TraversalMethod_sack_EmptyContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_sample_Scope_int(final GremlinParser.TraversalMethod_sample_Scope_intContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_sample_int(final GremlinParser.TraversalMethod_sample_intContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_select_Column(final GremlinParser.TraversalMethod_select_ColumnContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_select_Pop_String(final GremlinParser.TraversalMethod_select_Pop_StringContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_select_Pop_String_String_String(final GremlinParser.TraversalMethod_select_Pop_String_String_StringContext ctx) { notImplemented(ctx); return null; }

	@Override
	public T visitTraversalMethod_select_Pop_Traversal(final GremlinParser.TraversalMethod_select_Pop_TraversalContext ctx) {
		notImplemented(ctx); return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_select_String(final GremlinParser.TraversalMethod_select_StringContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_select_String_String_String(final GremlinParser.TraversalMethod_select_String_String_StringContext ctx) { notImplemented(ctx); return null; }

	@Override
	public T visitTraversalMethod_select_Traversal(final GremlinParser.TraversalMethod_select_TraversalContext ctx) {
		notImplemented(ctx); return null;
	}

	@Override
	public T visitTraversalMethod_shortestPath(final GremlinParser.TraversalMethod_shortestPathContext ctx) {
		notImplemented(ctx); return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_sideEffect(final GremlinParser.TraversalMethod_sideEffectContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_simplePath(final GremlinParser.TraversalMethod_simplePathContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_skip_Scope_long(final GremlinParser.TraversalMethod_skip_Scope_longContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_skip_long(final GremlinParser.TraversalMethod_skip_longContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_store(final GremlinParser.TraversalMethod_storeContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_subgraph(final GremlinParser.TraversalMethod_subgraphContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_sum_Empty(final GremlinParser.TraversalMethod_sum_EmptyContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_sum_Scope(final GremlinParser.TraversalMethod_sum_ScopeContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_tail_Empty(final GremlinParser.TraversalMethod_tail_EmptyContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_tail_Scope(final GremlinParser.TraversalMethod_tail_ScopeContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_tail_Scope_long(final GremlinParser.TraversalMethod_tail_Scope_longContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_tail_long(final GremlinParser.TraversalMethod_tail_longContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_timeLimit(final GremlinParser.TraversalMethod_timeLimitContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_times(final GremlinParser.TraversalMethod_timesContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_to_Direction_String(final GremlinParser.TraversalMethod_to_Direction_StringContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_to_String(final GremlinParser.TraversalMethod_to_StringContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_to_Traversal(final GremlinParser.TraversalMethod_to_TraversalContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_toE(final GremlinParser.TraversalMethod_toEContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_toV(final GremlinParser.TraversalMethod_toVContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_tree_Empty(final GremlinParser.TraversalMethod_tree_EmptyContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_tree_String(final GremlinParser.TraversalMethod_tree_StringContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_unfold(final GremlinParser.TraversalMethod_unfoldContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_union(final GremlinParser.TraversalMethod_unionContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_until_Predicate(final GremlinParser.TraversalMethod_until_PredicateContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_until_Traversal(final GremlinParser.TraversalMethod_until_TraversalContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_value(final GremlinParser.TraversalMethod_valueContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_valueMap_String(final GremlinParser.TraversalMethod_valueMap_StringContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_valueMap_boolean_String(final GremlinParser.TraversalMethod_valueMap_boolean_StringContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_values(final GremlinParser.TraversalMethod_valuesContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_where_P(final GremlinParser.TraversalMethod_where_PContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_where_String_P(final GremlinParser.TraversalMethod_where_String_PContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalMethod_where_Traversal(final GremlinParser.TraversalMethod_where_TraversalContext ctx) { notImplemented(ctx); return null; }

	@Override
	public T visitTraversalMethod_with_String(final GremlinParser.TraversalMethod_with_StringContext ctx) {
		notImplemented(ctx); return null;
	}

	@Override
	public T visitTraversalMethod_with_String_Object(final GremlinParser.TraversalMethod_with_String_ObjectContext ctx) {
		notImplemented(ctx); return null;
	}

	@Override
	public T visitTraversalMethod_write(final GremlinParser.TraversalMethod_writeContext ctx) {
		return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalScope(final GremlinParser.TraversalScopeContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalToken(final GremlinParser.TraversalTokenContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalOrder(final GremlinParser.TraversalOrderContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalDirection(final GremlinParser.TraversalDirectionContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalCardinality(final GremlinParser.TraversalCardinalityContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalColumn(final GremlinParser.TraversalColumnContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalPop(final GremlinParser.TraversalPopContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalOperator(final GremlinParser.TraversalOperatorContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalOptionParent(final GremlinParser.TraversalOptionParentContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalPredicate(final GremlinParser.TraversalPredicateContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalTerminalMethod(final GremlinParser.TraversalTerminalMethodContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalSackMethod(final GremlinParser.TraversalSackMethodContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override
	public T visitTraversalSelfMethod(final GremlinParser.TraversalSelfMethodContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalComparator(final GremlinParser.TraversalComparatorContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalFunction(final GremlinParser.TraversalFunctionContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalBiFunction(final GremlinParser.TraversalBiFunctionContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalPredicate_eq(final GremlinParser.TraversalPredicate_eqContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalPredicate_neq(final GremlinParser.TraversalPredicate_neqContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalPredicate_lt(final GremlinParser.TraversalPredicate_ltContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalPredicate_lte(final GremlinParser.TraversalPredicate_lteContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalPredicate_gt(final GremlinParser.TraversalPredicate_gtContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalPredicate_gte(final GremlinParser.TraversalPredicate_gteContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalPredicate_inside(final GremlinParser.TraversalPredicate_insideContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalPredicate_outside(final GremlinParser.TraversalPredicate_outsideContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalPredicate_between(final GremlinParser.TraversalPredicate_betweenContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalPredicate_within(final GremlinParser.TraversalPredicate_withinContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalPredicate_without(final GremlinParser.TraversalPredicate_withoutContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalPredicate_not(final GremlinParser.TraversalPredicate_notContext ctx) { notImplemented(ctx); return null; }

	@Override
	public T visitTraversalPredicate_containing(final GremlinParser.TraversalPredicate_containingContext ctx) {
		notImplemented(ctx); return null;
	}

	@Override
	public T visitTraversalPredicate_notContaining(final GremlinParser.TraversalPredicate_notContainingContext ctx) {
		notImplemented(ctx); return null;
	}

	@Override
	public T visitTraversalPredicate_startingWith(final GremlinParser.TraversalPredicate_startingWithContext ctx) {
		notImplemented(ctx); return null;
	}

	@Override
	public T visitTraversalPredicate_notStartingWith(final GremlinParser.TraversalPredicate_notStartingWithContext ctx) {
		notImplemented(ctx); return null;
	}

	@Override
	public T visitTraversalPredicate_endingWith(final GremlinParser.TraversalPredicate_endingWithContext ctx) {
		notImplemented(ctx); return null;
	}

	@Override
	public T visitTraversalPredicate_notEndingWith(final GremlinParser.TraversalPredicate_notEndingWithContext ctx) {
		notImplemented(ctx); return null;
	}
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalTerminalMethod_iterate(final GremlinParser.TraversalTerminalMethod_iterateContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalTerminalMethod_explain(final GremlinParser.TraversalTerminalMethod_explainContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalTerminalMethod_hasNext(final GremlinParser.TraversalTerminalMethod_hasNextContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalTerminalMethod_tryNext(final GremlinParser.TraversalTerminalMethod_tryNextContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalTerminalMethod_next(final GremlinParser.TraversalTerminalMethod_nextContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalTerminalMethod_toList(final GremlinParser.TraversalTerminalMethod_toListContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalTerminalMethod_toSet(final GremlinParser.TraversalTerminalMethod_toSetContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalTerminalMethod_toBulkSet(final GremlinParser.TraversalTerminalMethod_toBulkSetContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalSelfMethod_none(final GremlinParser.TraversalSelfMethod_noneContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalStrategy(final GremlinParser.TraversalStrategyContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalStrategyList(final GremlinParser.TraversalStrategyListContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitTraversalStrategyExpr(final GremlinParser.TraversalStrategyExprContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override
	public T visitTraversalStrategyArgs_PartitionStrategy(final GremlinParser.TraversalStrategyArgs_PartitionStrategyContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override
	public T visitTraversalStrategyArgs_EdgeLabelVerificationStrategy(final GremlinParser.TraversalStrategyArgs_EdgeLabelVerificationStrategyContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override
	public T visitTraversalStrategyArgs_ReservedKeysVerificationStrategy(final GremlinParser.TraversalStrategyArgs_ReservedKeysVerificationStrategyContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override
	public T visitTraversalStrategyArgs_SubgraphStrategy(final GremlinParser.TraversalStrategyArgs_SubgraphStrategyContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override
	public T visitTraversalStrategyArgs_ProductiveByStrategy(GremlinParser.TraversalStrategyArgs_ProductiveByStrategyContext ctx) { return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitNestedTraversalList(final GremlinParser.NestedTraversalListContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitNestedTraversalExpr(final GremlinParser.NestedTraversalExprContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitGenericLiteralList(final GremlinParser.GenericLiteralListContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitGenericLiteralExpr(final GremlinParser.GenericLiteralExprContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitGenericLiteralRange(final GremlinParser.GenericLiteralRangeContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitGenericLiteralCollection(final GremlinParser.GenericLiteralCollectionContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitStringLiteralList(final GremlinParser.StringLiteralListContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitStringLiteralExpr(final GremlinParser.StringLiteralExprContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitGenericLiteral(final GremlinParser.GenericLiteralContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitGenericLiteralMap(final GremlinParser.GenericLiteralMapContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitIntegerLiteral(final GremlinParser.IntegerLiteralContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitFloatLiteral(final GremlinParser.FloatLiteralContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitBooleanLiteral(final GremlinParser.BooleanLiteralContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitStringLiteral(final GremlinParser.StringLiteralContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitDateLiteral(final GremlinParser.DateLiteralContext ctx) { notImplemented(ctx); return null; }
	/**
	 * {@inheritDoc}
	 */
	@Override public T visitNullLiteral(final GremlinParser.NullLiteralContext ctx) { notImplemented(ctx); return null; }

	/**
	 * {@inheritDoc}
	 */
	@Override
	public T visitGremlinStringConstants(final GremlinParser.GremlinStringConstantsContext ctx) {
		notImplemented(ctx); return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public T visitPageRankStringConstants(final GremlinParser.PageRankStringConstantsContext ctx) {
		notImplemented(ctx); return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public T visitPeerPressureStringConstants(final GremlinParser.PeerPressureStringConstantsContext ctx) {
		notImplemented(ctx); return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public T visitShortestPathStringConstants(final GremlinParser.ShortestPathStringConstantsContext ctx) {
		notImplemented(ctx); return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public T visitWithOptionsStringConstants(final GremlinParser.WithOptionsStringConstantsContext ctx) {
		notImplemented(ctx); return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public T visitGremlinStringConstants_pageRankStringConstants_edges(final GremlinParser.GremlinStringConstants_pageRankStringConstants_edgesContext ctx) {
		notImplemented(ctx); return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public T visitGremlinStringConstants_pageRankStringConstants_times(final GremlinParser.GremlinStringConstants_pageRankStringConstants_timesContext ctx) {
		notImplemented(ctx); return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public T visitGremlinStringConstants_pageRankStringConstants_propertyName(final GremlinParser.GremlinStringConstants_pageRankStringConstants_propertyNameContext ctx) {
		notImplemented(ctx); return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public T visitGremlinStringConstants_peerPressureStringConstants_edges(final GremlinParser.GremlinStringConstants_peerPressureStringConstants_edgesContext ctx) {
		notImplemented(ctx); return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public T visitGremlinStringConstants_peerPressureStringConstants_times(final GremlinParser.GremlinStringConstants_peerPressureStringConstants_timesContext ctx) {
		notImplemented(ctx);return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public T visitGremlinStringConstants_peerPressureStringConstants_propertyName(final GremlinParser.GremlinStringConstants_peerPressureStringConstants_propertyNameContext ctx) {
		notImplemented(ctx); return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public T visitGremlinStringConstants_shortestPathStringConstants_target(final GremlinParser.GremlinStringConstants_shortestPathStringConstants_targetContext ctx) {
		notImplemented(ctx); return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public T visitGremlinStringConstants_shortestPathStringConstants_edges(final GremlinParser.GremlinStringConstants_shortestPathStringConstants_edgesContext ctx) {
		notImplemented(ctx); return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public T visitGremlinStringConstants_shortestPathStringConstants_distance(final GremlinParser.GremlinStringConstants_shortestPathStringConstants_distanceContext ctx) {
		notImplemented(ctx); return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public T visitGremlinStringConstants_shortestPathStringConstants_maxDistance(final GremlinParser.GremlinStringConstants_shortestPathStringConstants_maxDistanceContext ctx) {
		notImplemented(ctx); return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public T visitGremlinStringConstants_shortestPathStringConstants_includeEdges(final GremlinParser.GremlinStringConstants_shortestPathStringConstants_includeEdgesContext ctx) {
		notImplemented(ctx); return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public T visitGremlinStringConstants_withOptionsStringConstants_tokens(final GremlinParser.GremlinStringConstants_withOptionsStringConstants_tokensContext ctx) {
		notImplemented(ctx); return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public T visitGremlinStringConstants_withOptionsStringConstants_none(final GremlinParser.GremlinStringConstants_withOptionsStringConstants_noneContext ctx) {
		notImplemented(ctx); return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public T visitGremlinStringConstants_withOptionsStringConstants_ids(final GremlinParser.GremlinStringConstants_withOptionsStringConstants_idsContext ctx) {
		notImplemented(ctx); return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public T visitGremlinStringConstants_withOptionsStringConstants_labels(final GremlinParser.GremlinStringConstants_withOptionsStringConstants_labelsContext ctx) {
		notImplemented(ctx); return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public T visitGremlinStringConstants_withOptionsStringConstants_keys(final GremlinParser.GremlinStringConstants_withOptionsStringConstants_keysContext ctx) {
		notImplemented(ctx); return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public T visitGremlinStringConstants_withOptionsStringConstants_values(final GremlinParser.GremlinStringConstants_withOptionsStringConstants_valuesContext ctx) {
		notImplemented(ctx); return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public T visitGremlinStringConstants_withOptionsStringConstants_all(final GremlinParser.GremlinStringConstants_withOptionsStringConstants_allContext ctx) {
		notImplemented(ctx); return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public T visitGremlinStringConstants_withOptionsStringConstants_indexer(final GremlinParser.GremlinStringConstants_withOptionsStringConstants_indexerContext ctx) {
		notImplemented(ctx); return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public T visitGremlinStringConstants_withOptionsStringConstants_list(final GremlinParser.GremlinStringConstants_withOptionsStringConstants_listContext ctx) {
		notImplemented(ctx); return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public T visitGremlinStringConstants_withOptionsStringConstants_map(final GremlinParser.GremlinStringConstants_withOptionsStringConstants_mapContext ctx) {
		notImplemented(ctx); return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public T visitPageRankStringConstant(final GremlinParser.PageRankStringConstantContext ctx) {
		notImplemented(ctx); return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public T visitPeerPressureStringConstant(final GremlinParser.PeerPressureStringConstantContext ctx) {
		notImplemented(ctx); return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public T visitShortestPathStringConstant(final GremlinParser.ShortestPathStringConstantContext ctx) {
		notImplemented(ctx); return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public T visitWithOptionsStringConstant(final GremlinParser.WithOptionsStringConstantContext ctx) {
		notImplemented(ctx); return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public T visitTraversalMethod_option_Predicate_Traversal(final GremlinParser.TraversalMethod_option_Predicate_TraversalContext ctx) {
		notImplemented(ctx); return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public T visitIoOptionsStringConstants(final GremlinParser.IoOptionsStringConstantsContext ctx) {
		notImplemented(ctx); return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public T visitGremlinStringConstants_ioOptionsStringConstants_reader(final GremlinParser.GremlinStringConstants_ioOptionsStringConstants_readerContext ctx) {
		notImplemented(ctx); return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public T visitGremlinStringConstants_ioOptionsStringConstants_writer(final GremlinParser.GremlinStringConstants_ioOptionsStringConstants_writerContext ctx) {
		notImplemented(ctx); return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public T visitGremlinStringConstants_ioOptionsStringConstants_gryo(final GremlinParser.GremlinStringConstants_ioOptionsStringConstants_gryoContext ctx) {
		notImplemented(ctx); return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public T visitGremlinStringConstants_ioOptionsStringConstants_graphson(final GremlinParser.GremlinStringConstants_ioOptionsStringConstants_graphsonContext ctx) {
		notImplemented(ctx); return null;
	}

	@Override
	public T visitGremlinStringConstants_ioOptionsStringConstants_graphml(final GremlinParser.GremlinStringConstants_ioOptionsStringConstants_graphmlContext ctx) {
		notImplemented(ctx); return null;
	}

	@Override
	public T visitConnectedComponentConstants(final GremlinParser.ConnectedComponentConstantsContext ctx) {
		notImplemented(ctx); return null;
	}

	@Override
	public T visitGremlinStringConstants_connectedComponentStringConstants_component(final GremlinParser.GremlinStringConstants_connectedComponentStringConstants_componentContext ctx) {
		notImplemented(ctx); return null;
	}

	@Override
	public T visitGremlinStringConstants_connectedComponentStringConstants_edges(final GremlinParser.GremlinStringConstants_connectedComponentStringConstants_edgesContext ctx) {
		notImplemented(ctx); return null;
	}

	@Override
	public T visitGremlinStringConstants_connectedComponentStringConstants_propertyName(GremlinParser.GremlinStringConstants_connectedComponentStringConstants_propertyNameContext ctx) {
		return null;
	}

	@Override
	public T visitConnectedComponentStringConstant(final GremlinParser.ConnectedComponentStringConstantContext ctx) {
		notImplemented(ctx); return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public T visitIoOptionsStringConstant(final GremlinParser.IoOptionsStringConstantContext ctx) {
		notImplemented(ctx); return null;
	}
}
