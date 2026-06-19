// Generated from gremlin-language/src/main/antlr4/Gremlin.g4 by ANTLR 4.13.2
// ignore_for_file: unused_import, unused_local_variable, prefer_single_quotes
import 'package:antlr4/antlr4.dart';

import 'GremlinParser.dart';

/// This abstract class defines a complete generic visitor for a parse tree
/// produced by [GremlinParser].
///
/// [T] is the eturn type of the visit operation. Use `void` for
/// operations with no return type.
abstract class GremlinVisitor<T> extends ParseTreeVisitor<T> {
  /// Visit a parse tree produced by [GremlinParser.queryList].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitQueryList(QueryListContext ctx);

  /// Visit a parse tree produced by [GremlinParser.query].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitQuery(QueryContext ctx);

  /// Visit a parse tree produced by [GremlinParser.emptyQuery].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitEmptyQuery(EmptyQueryContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalSource].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalSource(TraversalSourceContext ctx);

  /// Visit a parse tree produced by [GremlinParser.transactionPart].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTransactionPart(TransactionPartContext ctx);

  /// Visit a parse tree produced by [GremlinParser.rootTraversal].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitRootTraversal(RootTraversalContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalSourceSelfMethod].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalSourceSelfMethod(TraversalSourceSelfMethodContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalSourceSelfMethod_withBulk].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalSourceSelfMethod_withBulk(TraversalSourceSelfMethod_withBulkContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalSourceSelfMethod_withPath].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalSourceSelfMethod_withPath(TraversalSourceSelfMethod_withPathContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalSourceSelfMethod_withSack].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalSourceSelfMethod_withSack(TraversalSourceSelfMethod_withSackContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalSourceSelfMethod_withSideEffect].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalSourceSelfMethod_withSideEffect(TraversalSourceSelfMethod_withSideEffectContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalSourceSelfMethod_withStrategies].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalSourceSelfMethod_withStrategies(TraversalSourceSelfMethod_withStrategiesContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalSourceSelfMethod_withoutStrategies].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalSourceSelfMethod_withoutStrategies(TraversalSourceSelfMethod_withoutStrategiesContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalSourceSelfMethod_with].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalSourceSelfMethod_with(TraversalSourceSelfMethod_withContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalSourceSpawnMethod].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalSourceSpawnMethod(TraversalSourceSpawnMethodContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalSourceSpawnMethod_addE].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalSourceSpawnMethod_addE(TraversalSourceSpawnMethod_addEContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalSourceSpawnMethod_addV].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalSourceSpawnMethod_addV(TraversalSourceSpawnMethod_addVContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalSourceSpawnMethod_E].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalSourceSpawnMethod_E(TraversalSourceSpawnMethod_EContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalSourceSpawnMethod_V].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalSourceSpawnMethod_V(TraversalSourceSpawnMethod_VContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalSourceSpawnMethod_inject].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalSourceSpawnMethod_inject(TraversalSourceSpawnMethod_injectContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalSourceSpawnMethod_io].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalSourceSpawnMethod_io(TraversalSourceSpawnMethod_ioContext ctx);

  /// Visit a parse tree produced by the {@code traversalSourceSpawnMethod_mergeV_Map}
  /// labeled alternative in {@link GremlinParser#traversalSourceSpawnMethod_mergeV}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalSourceSpawnMethod_mergeV_Map(TraversalSourceSpawnMethod_mergeV_MapContext ctx);

  /// Visit a parse tree produced by the {@code traversalSourceSpawnMethod_mergeV_Traversal}
  /// labeled alternative in {@link GremlinParser#traversalSourceSpawnMethod_mergeV}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalSourceSpawnMethod_mergeV_Traversal(TraversalSourceSpawnMethod_mergeV_TraversalContext ctx);

  /// Visit a parse tree produced by the {@code traversalSourceSpawnMethod_mergeE_Map}
  /// labeled alternative in {@link GremlinParser#traversalSourceSpawnMethod_mergeE}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalSourceSpawnMethod_mergeE_Map(TraversalSourceSpawnMethod_mergeE_MapContext ctx);

  /// Visit a parse tree produced by the {@code traversalSourceSpawnMethod_mergeE_Traversal}
  /// labeled alternative in {@link GremlinParser#traversalSourceSpawnMethod_mergeE}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalSourceSpawnMethod_mergeE_Traversal(TraversalSourceSpawnMethod_mergeE_TraversalContext ctx);

  /// Visit a parse tree produced by the {@code traversalSourceSpawnMethod_call_empty}
  /// labeled alternative in {@link GremlinParser#traversalSourceSpawnMethod_call}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalSourceSpawnMethod_call_empty(TraversalSourceSpawnMethod_call_emptyContext ctx);

  /// Visit a parse tree produced by the {@code traversalSourceSpawnMethod_call_string}
  /// labeled alternative in {@link GremlinParser#traversalSourceSpawnMethod_call}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalSourceSpawnMethod_call_string(TraversalSourceSpawnMethod_call_stringContext ctx);

  /// Visit a parse tree produced by the {@code traversalSourceSpawnMethod_call_string_map}
  /// labeled alternative in {@link GremlinParser#traversalSourceSpawnMethod_call}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalSourceSpawnMethod_call_string_map(TraversalSourceSpawnMethod_call_string_mapContext ctx);

  /// Visit a parse tree produced by the {@code traversalSourceSpawnMethod_call_string_traversal}
  /// labeled alternative in {@link GremlinParser#traversalSourceSpawnMethod_call}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalSourceSpawnMethod_call_string_traversal(TraversalSourceSpawnMethod_call_string_traversalContext ctx);

  /// Visit a parse tree produced by the {@code traversalSourceSpawnMethod_call_string_map_traversal}
  /// labeled alternative in {@link GremlinParser#traversalSourceSpawnMethod_call}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalSourceSpawnMethod_call_string_map_traversal(TraversalSourceSpawnMethod_call_string_map_traversalContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalSourceSpawnMethod_union].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalSourceSpawnMethod_union(TraversalSourceSpawnMethod_unionContext ctx);

  /// Visit a parse tree produced by [GremlinParser.chainedTraversal].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitChainedTraversal(ChainedTraversalContext ctx);

  /// Visit a parse tree produced by [GremlinParser.nestedTraversal].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitNestedTraversal(NestedTraversalContext ctx);

  /// Visit a parse tree produced by [GremlinParser.terminatedTraversal].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTerminatedTraversal(TerminatedTraversalContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalMethod].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod(TraversalMethodContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalMethod_V].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_V(TraversalMethod_VContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalMethod_E].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_E(TraversalMethod_EContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_addE_String}
  /// labeled alternative in {@link GremlinParser#traversalMethod_addE}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_addE_String(TraversalMethod_addE_StringContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_addE_Traversal}
  /// labeled alternative in {@link GremlinParser#traversalMethod_addE}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_addE_Traversal(TraversalMethod_addE_TraversalContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_addV_Empty}
  /// labeled alternative in {@link GremlinParser#traversalMethod_addV}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_addV_Empty(TraversalMethod_addV_EmptyContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_addV_String}
  /// labeled alternative in {@link GremlinParser#traversalMethod_addV}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_addV_String(TraversalMethod_addV_StringContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_addV_Traversal}
  /// labeled alternative in {@link GremlinParser#traversalMethod_addV}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_addV_Traversal(TraversalMethod_addV_TraversalContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_aggregate_String}
  /// labeled alternative in {@link GremlinParser#traversalMethod_aggregate}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_aggregate_String(TraversalMethod_aggregate_StringContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_all_P}
  /// labeled alternative in {@link GremlinParser#traversalMethod_all}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_all_P(TraversalMethod_all_PContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalMethod_and].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_and(TraversalMethod_andContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_any_P}
  /// labeled alternative in {@link GremlinParser#traversalMethod_any}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_any_P(TraversalMethod_any_PContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalMethod_as].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_as(TraversalMethod_asContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalMethod_asBool].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_asBool(TraversalMethod_asBoolContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalMethod_asDate].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_asDate(TraversalMethod_asDateContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_asNumber_Empty}
  /// labeled alternative in {@link GremlinParser#traversalMethod_asNumber}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_asNumber_Empty(TraversalMethod_asNumber_EmptyContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_asNumber_traversalGType}
  /// labeled alternative in {@link GremlinParser#traversalMethod_asNumber}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_asNumber_traversalGType(TraversalMethod_asNumber_traversalGTypeContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_asString_Empty}
  /// labeled alternative in {@link GremlinParser#traversalMethod_asString}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_asString_Empty(TraversalMethod_asString_EmptyContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_asString_Scope}
  /// labeled alternative in {@link GremlinParser#traversalMethod_asString}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_asString_Scope(TraversalMethod_asString_ScopeContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_barrier_Consumer}
  /// labeled alternative in {@link GremlinParser#traversalMethod_barrier}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_barrier_Consumer(TraversalMethod_barrier_ConsumerContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_barrier_Empty}
  /// labeled alternative in {@link GremlinParser#traversalMethod_barrier}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_barrier_Empty(TraversalMethod_barrier_EmptyContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_barrier_int}
  /// labeled alternative in {@link GremlinParser#traversalMethod_barrier}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_barrier_int(TraversalMethod_barrier_intContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalMethod_both].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_both(TraversalMethod_bothContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalMethod_bothE].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_bothE(TraversalMethod_bothEContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalMethod_bothV].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_bothV(TraversalMethod_bothVContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalMethod_branch].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_branch(TraversalMethod_branchContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_by_Comparator}
  /// labeled alternative in {@link GremlinParser#traversalMethod_by}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_by_Comparator(TraversalMethod_by_ComparatorContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_by_Empty}
  /// labeled alternative in {@link GremlinParser#traversalMethod_by}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_by_Empty(TraversalMethod_by_EmptyContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_by_Function}
  /// labeled alternative in {@link GremlinParser#traversalMethod_by}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_by_Function(TraversalMethod_by_FunctionContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_by_Function_Comparator}
  /// labeled alternative in {@link GremlinParser#traversalMethod_by}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_by_Function_Comparator(TraversalMethod_by_Function_ComparatorContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_by_Order}
  /// labeled alternative in {@link GremlinParser#traversalMethod_by}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_by_Order(TraversalMethod_by_OrderContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_by_String}
  /// labeled alternative in {@link GremlinParser#traversalMethod_by}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_by_String(TraversalMethod_by_StringContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_by_String_Comparator}
  /// labeled alternative in {@link GremlinParser#traversalMethod_by}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_by_String_Comparator(TraversalMethod_by_String_ComparatorContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_by_T}
  /// labeled alternative in {@link GremlinParser#traversalMethod_by}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_by_T(TraversalMethod_by_TContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_by_Traversal}
  /// labeled alternative in {@link GremlinParser#traversalMethod_by}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_by_Traversal(TraversalMethod_by_TraversalContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_by_Traversal_Comparator}
  /// labeled alternative in {@link GremlinParser#traversalMethod_by}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_by_Traversal_Comparator(TraversalMethod_by_Traversal_ComparatorContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_call_string}
  /// labeled alternative in {@link GremlinParser#traversalMethod_call}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_call_string(TraversalMethod_call_stringContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_call_string_map}
  /// labeled alternative in {@link GremlinParser#traversalMethod_call}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_call_string_map(TraversalMethod_call_string_mapContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_call_string_traversal}
  /// labeled alternative in {@link GremlinParser#traversalMethod_call}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_call_string_traversal(TraversalMethod_call_string_traversalContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_call_string_map_traversal}
  /// labeled alternative in {@link GremlinParser#traversalMethod_call}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_call_string_map_traversal(TraversalMethod_call_string_map_traversalContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalMethod_cap].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_cap(TraversalMethod_capContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_choose_Function}
  /// labeled alternative in {@link GremlinParser#traversalMethod_choose}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_choose_Function(TraversalMethod_choose_FunctionContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_choose_Predicate_Traversal}
  /// labeled alternative in {@link GremlinParser#traversalMethod_choose}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_choose_Predicate_Traversal(TraversalMethod_choose_Predicate_TraversalContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_choose_Predicate_Traversal_Traversal}
  /// labeled alternative in {@link GremlinParser#traversalMethod_choose}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_choose_Predicate_Traversal_Traversal(TraversalMethod_choose_Predicate_Traversal_TraversalContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_choose_Traversal}
  /// labeled alternative in {@link GremlinParser#traversalMethod_choose}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_choose_Traversal(TraversalMethod_choose_TraversalContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_choose_Traversal_Traversal}
  /// labeled alternative in {@link GremlinParser#traversalMethod_choose}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_choose_Traversal_Traversal(TraversalMethod_choose_Traversal_TraversalContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_choose_Traversal_Traversal_Traversal}
  /// labeled alternative in {@link GremlinParser#traversalMethod_choose}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_choose_Traversal_Traversal_Traversal(TraversalMethod_choose_Traversal_Traversal_TraversalContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalMethod_coalesce].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_coalesce(TraversalMethod_coalesceContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalMethod_coin].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_coin(TraversalMethod_coinContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_combine_Object}
  /// labeled alternative in {@link GremlinParser#traversalMethod_combine}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_combine_Object(TraversalMethod_combine_ObjectContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_concat_Traversal_Traversal}
  /// labeled alternative in {@link GremlinParser#traversalMethod_concat}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_concat_Traversal_Traversal(TraversalMethod_concat_Traversal_TraversalContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_concat_String}
  /// labeled alternative in {@link GremlinParser#traversalMethod_concat}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_concat_String(TraversalMethod_concat_StringContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_conjoin_String}
  /// labeled alternative in {@link GremlinParser#traversalMethod_conjoin}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_conjoin_String(TraversalMethod_conjoin_StringContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalMethod_connectedComponent].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_connectedComponent(TraversalMethod_connectedComponentContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalMethod_constant].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_constant(TraversalMethod_constantContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_count_Empty}
  /// labeled alternative in {@link GremlinParser#traversalMethod_count}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_count_Empty(TraversalMethod_count_EmptyContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_count_Scope}
  /// labeled alternative in {@link GremlinParser#traversalMethod_count}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_count_Scope(TraversalMethod_count_ScopeContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalMethod_cyclicPath].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_cyclicPath(TraversalMethod_cyclicPathContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalMethod_dateAdd].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_dateAdd(TraversalMethod_dateAddContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_dateDiff_Traversal}
  /// labeled alternative in {@link GremlinParser#traversalMethod_dateDiff}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_dateDiff_Traversal(TraversalMethod_dateDiff_TraversalContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_dateDiff_Date}
  /// labeled alternative in {@link GremlinParser#traversalMethod_dateDiff}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_dateDiff_Date(TraversalMethod_dateDiff_DateContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_dedup_Scope_String}
  /// labeled alternative in {@link GremlinParser#traversalMethod_dedup}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_dedup_Scope_String(TraversalMethod_dedup_Scope_StringContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_dedup_String}
  /// labeled alternative in {@link GremlinParser#traversalMethod_dedup}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_dedup_String(TraversalMethod_dedup_StringContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_difference_Object}
  /// labeled alternative in {@link GremlinParser#traversalMethod_difference}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_difference_Object(TraversalMethod_difference_ObjectContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalMethod_discard].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_discard(TraversalMethod_discardContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_disjunct_Object}
  /// labeled alternative in {@link GremlinParser#traversalMethod_disjunct}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_disjunct_Object(TraversalMethod_disjunct_ObjectContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalMethod_drop].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_drop(TraversalMethod_dropContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalMethod_element].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_element(TraversalMethod_elementContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalMethod_elementMap].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_elementMap(TraversalMethod_elementMapContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_emit_Empty}
  /// labeled alternative in {@link GremlinParser#traversalMethod_emit}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_emit_Empty(TraversalMethod_emit_EmptyContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_emit_Predicate}
  /// labeled alternative in {@link GremlinParser#traversalMethod_emit}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_emit_Predicate(TraversalMethod_emit_PredicateContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_emit_Traversal}
  /// labeled alternative in {@link GremlinParser#traversalMethod_emit}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_emit_Traversal(TraversalMethod_emit_TraversalContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_fail_Empty}
  /// labeled alternative in {@link GremlinParser#traversalMethod_fail}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_fail_Empty(TraversalMethod_fail_EmptyContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_fail_String}
  /// labeled alternative in {@link GremlinParser#traversalMethod_fail}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_fail_String(TraversalMethod_fail_StringContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_filter_Predicate}
  /// labeled alternative in {@link GremlinParser#traversalMethod_filter}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_filter_Predicate(TraversalMethod_filter_PredicateContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_filter_Traversal}
  /// labeled alternative in {@link GremlinParser#traversalMethod_filter}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_filter_Traversal(TraversalMethod_filter_TraversalContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalMethod_flatMap].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_flatMap(TraversalMethod_flatMapContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_fold_Empty}
  /// labeled alternative in {@link GremlinParser#traversalMethod_fold}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_fold_Empty(TraversalMethod_fold_EmptyContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_fold_Object_BiFunction}
  /// labeled alternative in {@link GremlinParser#traversalMethod_fold}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_fold_Object_BiFunction(TraversalMethod_fold_Object_BiFunctionContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_format_String}
  /// labeled alternative in {@link GremlinParser#traversalMethod_format}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_format_String(TraversalMethod_format_StringContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_from_String}
  /// labeled alternative in {@link GremlinParser#traversalMethod_from}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_from_String(TraversalMethod_from_StringContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_from_Traversal}
  /// labeled alternative in {@link GremlinParser#traversalMethod_from}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_from_Traversal(TraversalMethod_from_TraversalContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_group_Empty}
  /// labeled alternative in {@link GremlinParser#traversalMethod_group}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_group_Empty(TraversalMethod_group_EmptyContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_group_String}
  /// labeled alternative in {@link GremlinParser#traversalMethod_group}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_group_String(TraversalMethod_group_StringContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_groupCount_Empty}
  /// labeled alternative in {@link GremlinParser#traversalMethod_groupCount}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_groupCount_Empty(TraversalMethod_groupCount_EmptyContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_groupCount_String}
  /// labeled alternative in {@link GremlinParser#traversalMethod_groupCount}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_groupCount_String(TraversalMethod_groupCount_StringContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_has_String}
  /// labeled alternative in {@link GremlinParser#traversalMethod_has}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_has_String(TraversalMethod_has_StringContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_has_String_Object}
  /// labeled alternative in {@link GremlinParser#traversalMethod_has}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_has_String_Object(TraversalMethod_has_String_ObjectContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_has_String_P}
  /// labeled alternative in {@link GremlinParser#traversalMethod_has}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_has_String_P(TraversalMethod_has_String_PContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_has_String_String_Object}
  /// labeled alternative in {@link GremlinParser#traversalMethod_has}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_has_String_String_Object(TraversalMethod_has_String_String_ObjectContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_has_String_String_P}
  /// labeled alternative in {@link GremlinParser#traversalMethod_has}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_has_String_String_P(TraversalMethod_has_String_String_PContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_has_T_Object}
  /// labeled alternative in {@link GremlinParser#traversalMethod_has}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_has_T_Object(TraversalMethod_has_T_ObjectContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_has_T_P}
  /// labeled alternative in {@link GremlinParser#traversalMethod_has}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_has_T_P(TraversalMethod_has_T_PContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_hasId_Object_Object}
  /// labeled alternative in {@link GremlinParser#traversalMethod_hasId}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_hasId_Object_Object(TraversalMethod_hasId_Object_ObjectContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_hasId_P}
  /// labeled alternative in {@link GremlinParser#traversalMethod_hasId}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_hasId_P(TraversalMethod_hasId_PContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_hasKey_P}
  /// labeled alternative in {@link GremlinParser#traversalMethod_hasKey}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_hasKey_P(TraversalMethod_hasKey_PContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_hasKey_String_String}
  /// labeled alternative in {@link GremlinParser#traversalMethod_hasKey}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_hasKey_String_String(TraversalMethod_hasKey_String_StringContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_hasLabel_P}
  /// labeled alternative in {@link GremlinParser#traversalMethod_hasLabel}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_hasLabel_P(TraversalMethod_hasLabel_PContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_hasLabel_String_String}
  /// labeled alternative in {@link GremlinParser#traversalMethod_hasLabel}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_hasLabel_String_String(TraversalMethod_hasLabel_String_StringContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalMethod_hasNot].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_hasNot(TraversalMethod_hasNotContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_hasValue_Object_Object}
  /// labeled alternative in {@link GremlinParser#traversalMethod_hasValue}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_hasValue_Object_Object(TraversalMethod_hasValue_Object_ObjectContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_hasValue_P}
  /// labeled alternative in {@link GremlinParser#traversalMethod_hasValue}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_hasValue_P(TraversalMethod_hasValue_PContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalMethod_id].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_id(TraversalMethod_idContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalMethod_identity].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_identity(TraversalMethod_identityContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalMethod_in].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_in(TraversalMethod_inContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalMethod_inE].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_inE(TraversalMethod_inEContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_intersect_Object}
  /// labeled alternative in {@link GremlinParser#traversalMethod_intersect}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_intersect_Object(TraversalMethod_intersect_ObjectContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalMethod_inV].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_inV(TraversalMethod_inVContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalMethod_index].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_index(TraversalMethod_indexContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalMethod_inject].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_inject(TraversalMethod_injectContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_is_Object}
  /// labeled alternative in {@link GremlinParser#traversalMethod_is}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_is_Object(TraversalMethod_is_ObjectContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_is_P}
  /// labeled alternative in {@link GremlinParser#traversalMethod_is}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_is_P(TraversalMethod_is_PContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalMethod_key].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_key(TraversalMethod_keyContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalMethod_label].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_label(TraversalMethod_labelContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_length_Empty}
  /// labeled alternative in {@link GremlinParser#traversalMethod_length}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_length_Empty(TraversalMethod_length_EmptyContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_length_Scope}
  /// labeled alternative in {@link GremlinParser#traversalMethod_length}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_length_Scope(TraversalMethod_length_ScopeContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_limit_Scope_long}
  /// labeled alternative in {@link GremlinParser#traversalMethod_limit}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_limit_Scope_long(TraversalMethod_limit_Scope_longContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_limit_long}
  /// labeled alternative in {@link GremlinParser#traversalMethod_limit}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_limit_long(TraversalMethod_limit_longContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalMethod_local].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_local(TraversalMethod_localContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_loops_Empty}
  /// labeled alternative in {@link GremlinParser#traversalMethod_loops}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_loops_Empty(TraversalMethod_loops_EmptyContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_loops_String}
  /// labeled alternative in {@link GremlinParser#traversalMethod_loops}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_loops_String(TraversalMethod_loops_StringContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_lTrim_Empty}
  /// labeled alternative in {@link GremlinParser#traversalMethod_lTrim}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_lTrim_Empty(TraversalMethod_lTrim_EmptyContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_lTrim_Scope}
  /// labeled alternative in {@link GremlinParser#traversalMethod_lTrim}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_lTrim_Scope(TraversalMethod_lTrim_ScopeContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalMethod_map].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_map(TraversalMethod_mapContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalMethod_match].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_match(TraversalMethod_matchContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalMethod_math].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_math(TraversalMethod_mathContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_max_Empty}
  /// labeled alternative in {@link GremlinParser#traversalMethod_max}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_max_Empty(TraversalMethod_max_EmptyContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_max_Scope}
  /// labeled alternative in {@link GremlinParser#traversalMethod_max}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_max_Scope(TraversalMethod_max_ScopeContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_mean_Empty}
  /// labeled alternative in {@link GremlinParser#traversalMethod_mean}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_mean_Empty(TraversalMethod_mean_EmptyContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_mean_Scope}
  /// labeled alternative in {@link GremlinParser#traversalMethod_mean}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_mean_Scope(TraversalMethod_mean_ScopeContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_merge_Object}
  /// labeled alternative in {@link GremlinParser#traversalMethod_merge}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_merge_Object(TraversalMethod_merge_ObjectContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_mergeV_empty}
  /// labeled alternative in {@link GremlinParser#traversalMethod_mergeV}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_mergeV_empty(TraversalMethod_mergeV_emptyContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_mergeV_Map}
  /// labeled alternative in {@link GremlinParser#traversalMethod_mergeV}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_mergeV_Map(TraversalMethod_mergeV_MapContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_mergeV_Traversal}
  /// labeled alternative in {@link GremlinParser#traversalMethod_mergeV}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_mergeV_Traversal(TraversalMethod_mergeV_TraversalContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_mergeE_empty}
  /// labeled alternative in {@link GremlinParser#traversalMethod_mergeE}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_mergeE_empty(TraversalMethod_mergeE_emptyContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_mergeE_Map}
  /// labeled alternative in {@link GremlinParser#traversalMethod_mergeE}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_mergeE_Map(TraversalMethod_mergeE_MapContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_mergeE_Traversal}
  /// labeled alternative in {@link GremlinParser#traversalMethod_mergeE}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_mergeE_Traversal(TraversalMethod_mergeE_TraversalContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_min_Empty}
  /// labeled alternative in {@link GremlinParser#traversalMethod_min}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_min_Empty(TraversalMethod_min_EmptyContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_min_Scope}
  /// labeled alternative in {@link GremlinParser#traversalMethod_min}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_min_Scope(TraversalMethod_min_ScopeContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_none_P}
  /// labeled alternative in {@link GremlinParser#traversalMethod_none}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_none_P(TraversalMethod_none_PContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalMethod_not].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_not(TraversalMethod_notContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_option_Predicate_Traversal}
  /// labeled alternative in {@link GremlinParser#traversalMethod_option}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_option_Predicate_Traversal(TraversalMethod_option_Predicate_TraversalContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_option_Merge_Map}
  /// labeled alternative in {@link GremlinParser#traversalMethod_option}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_option_Merge_Map(TraversalMethod_option_Merge_MapContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_option_Merge_Map_Cardinality}
  /// labeled alternative in {@link GremlinParser#traversalMethod_option}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_option_Merge_Map_Cardinality(TraversalMethod_option_Merge_Map_CardinalityContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_option_Merge_Traversal}
  /// labeled alternative in {@link GremlinParser#traversalMethod_option}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_option_Merge_Traversal(TraversalMethod_option_Merge_TraversalContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_option_Object_Traversal}
  /// labeled alternative in {@link GremlinParser#traversalMethod_option}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_option_Object_Traversal(TraversalMethod_option_Object_TraversalContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_option_Traversal}
  /// labeled alternative in {@link GremlinParser#traversalMethod_option}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_option_Traversal(TraversalMethod_option_TraversalContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalMethod_optional].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_optional(TraversalMethod_optionalContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalMethod_or].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_or(TraversalMethod_orContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_order_Empty}
  /// labeled alternative in {@link GremlinParser#traversalMethod_order}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_order_Empty(TraversalMethod_order_EmptyContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_order_Scope}
  /// labeled alternative in {@link GremlinParser#traversalMethod_order}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_order_Scope(TraversalMethod_order_ScopeContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalMethod_otherV].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_otherV(TraversalMethod_otherVContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalMethod_out].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_out(TraversalMethod_outContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalMethod_outE].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_outE(TraversalMethod_outEContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalMethod_outV].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_outV(TraversalMethod_outVContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_pageRank_Empty}
  /// labeled alternative in {@link GremlinParser#traversalMethod_pageRank}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_pageRank_Empty(TraversalMethod_pageRank_EmptyContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_pageRank_double}
  /// labeled alternative in {@link GremlinParser#traversalMethod_pageRank}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_pageRank_double(TraversalMethod_pageRank_doubleContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalMethod_path].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_path(TraversalMethod_pathContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalMethod_peerPressure].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_peerPressure(TraversalMethod_peerPressureContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_product_Object}
  /// labeled alternative in {@link GremlinParser#traversalMethod_product}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_product_Object(TraversalMethod_product_ObjectContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_profile_Empty}
  /// labeled alternative in {@link GremlinParser#traversalMethod_profile}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_profile_Empty(TraversalMethod_profile_EmptyContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_profile_String}
  /// labeled alternative in {@link GremlinParser#traversalMethod_profile}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_profile_String(TraversalMethod_profile_StringContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalMethod_project].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_project(TraversalMethod_projectContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalMethod_properties].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_properties(TraversalMethod_propertiesContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_property_Cardinality_Object_Object_Object}
  /// labeled alternative in {@link GremlinParser#traversalMethod_property}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_property_Cardinality_Object_Object_Object(TraversalMethod_property_Cardinality_Object_Object_ObjectContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_property_Cardinality_Object}
  /// labeled alternative in {@link GremlinParser#traversalMethod_property}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_property_Cardinality_Object(TraversalMethod_property_Cardinality_ObjectContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_property_Object_Object_Object}
  /// labeled alternative in {@link GremlinParser#traversalMethod_property}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_property_Object_Object_Object(TraversalMethod_property_Object_Object_ObjectContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_property_Object}
  /// labeled alternative in {@link GremlinParser#traversalMethod_property}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_property_Object(TraversalMethod_property_ObjectContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalMethod_propertyMap].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_propertyMap(TraversalMethod_propertyMapContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_range_Scope_long_long}
  /// labeled alternative in {@link GremlinParser#traversalMethod_range}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_range_Scope_long_long(TraversalMethod_range_Scope_long_longContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_range_long_long}
  /// labeled alternative in {@link GremlinParser#traversalMethod_range}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_range_long_long(TraversalMethod_range_long_longContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalMethod_read].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_read(TraversalMethod_readContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_repeat_String_Traversal}
  /// labeled alternative in {@link GremlinParser#traversalMethod_repeat}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_repeat_String_Traversal(TraversalMethod_repeat_String_TraversalContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_repeat_Traversal}
  /// labeled alternative in {@link GremlinParser#traversalMethod_repeat}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_repeat_Traversal(TraversalMethod_repeat_TraversalContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_replace_String_String}
  /// labeled alternative in {@link GremlinParser#traversalMethod_replace}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_replace_String_String(TraversalMethod_replace_String_StringContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_replace_Scope_String_String}
  /// labeled alternative in {@link GremlinParser#traversalMethod_replace}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_replace_Scope_String_String(TraversalMethod_replace_Scope_String_StringContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_reverse_Empty}
  /// labeled alternative in {@link GremlinParser#traversalMethod_reverse}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_reverse_Empty(TraversalMethod_reverse_EmptyContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_rTrim_Empty}
  /// labeled alternative in {@link GremlinParser#traversalMethod_rTrim}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_rTrim_Empty(TraversalMethod_rTrim_EmptyContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_rTrim_Scope}
  /// labeled alternative in {@link GremlinParser#traversalMethod_rTrim}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_rTrim_Scope(TraversalMethod_rTrim_ScopeContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_sack_BiFunction}
  /// labeled alternative in {@link GremlinParser#traversalMethod_sack}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_sack_BiFunction(TraversalMethod_sack_BiFunctionContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_sack_Empty}
  /// labeled alternative in {@link GremlinParser#traversalMethod_sack}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_sack_Empty(TraversalMethod_sack_EmptyContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_sample_Scope_int}
  /// labeled alternative in {@link GremlinParser#traversalMethod_sample}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_sample_Scope_int(TraversalMethod_sample_Scope_intContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_sample_int}
  /// labeled alternative in {@link GremlinParser#traversalMethod_sample}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_sample_int(TraversalMethod_sample_intContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_select_Column}
  /// labeled alternative in {@link GremlinParser#traversalMethod_select}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_select_Column(TraversalMethod_select_ColumnContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_select_Pop_String}
  /// labeled alternative in {@link GremlinParser#traversalMethod_select}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_select_Pop_String(TraversalMethod_select_Pop_StringContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_select_Pop_String_String_String}
  /// labeled alternative in {@link GremlinParser#traversalMethod_select}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_select_Pop_String_String_String(TraversalMethod_select_Pop_String_String_StringContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_select_Pop_Traversal}
  /// labeled alternative in {@link GremlinParser#traversalMethod_select}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_select_Pop_Traversal(TraversalMethod_select_Pop_TraversalContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_select_String}
  /// labeled alternative in {@link GremlinParser#traversalMethod_select}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_select_String(TraversalMethod_select_StringContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_select_String_String_String}
  /// labeled alternative in {@link GremlinParser#traversalMethod_select}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_select_String_String_String(TraversalMethod_select_String_String_StringContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_select_Traversal}
  /// labeled alternative in {@link GremlinParser#traversalMethod_select}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_select_Traversal(TraversalMethod_select_TraversalContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalMethod_shortestPath].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_shortestPath(TraversalMethod_shortestPathContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalMethod_sideEffect].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_sideEffect(TraversalMethod_sideEffectContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalMethod_simplePath].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_simplePath(TraversalMethod_simplePathContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_skip_Scope_long}
  /// labeled alternative in {@link GremlinParser#traversalMethod_skip}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_skip_Scope_long(TraversalMethod_skip_Scope_longContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_skip_long}
  /// labeled alternative in {@link GremlinParser#traversalMethod_skip}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_skip_long(TraversalMethod_skip_longContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_split_String}
  /// labeled alternative in {@link GremlinParser#traversalMethod_split}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_split_String(TraversalMethod_split_StringContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_split_Scope_String}
  /// labeled alternative in {@link GremlinParser#traversalMethod_split}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_split_Scope_String(TraversalMethod_split_Scope_StringContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalMethod_subgraph].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_subgraph(TraversalMethod_subgraphContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_substring_int}
  /// labeled alternative in {@link GremlinParser#traversalMethod_substring}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_substring_int(TraversalMethod_substring_intContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_substring_Scope_int}
  /// labeled alternative in {@link GremlinParser#traversalMethod_substring}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_substring_Scope_int(TraversalMethod_substring_Scope_intContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_substring_int_int}
  /// labeled alternative in {@link GremlinParser#traversalMethod_substring}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_substring_int_int(TraversalMethod_substring_int_intContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_substring_Scope_int_int}
  /// labeled alternative in {@link GremlinParser#traversalMethod_substring}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_substring_Scope_int_int(TraversalMethod_substring_Scope_int_intContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_sum_Empty}
  /// labeled alternative in {@link GremlinParser#traversalMethod_sum}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_sum_Empty(TraversalMethod_sum_EmptyContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_sum_Scope}
  /// labeled alternative in {@link GremlinParser#traversalMethod_sum}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_sum_Scope(TraversalMethod_sum_ScopeContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_tail_Empty}
  /// labeled alternative in {@link GremlinParser#traversalMethod_tail}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_tail_Empty(TraversalMethod_tail_EmptyContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_tail_Scope}
  /// labeled alternative in {@link GremlinParser#traversalMethod_tail}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_tail_Scope(TraversalMethod_tail_ScopeContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_tail_Scope_long}
  /// labeled alternative in {@link GremlinParser#traversalMethod_tail}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_tail_Scope_long(TraversalMethod_tail_Scope_longContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_tail_long}
  /// labeled alternative in {@link GremlinParser#traversalMethod_tail}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_tail_long(TraversalMethod_tail_longContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalMethod_timeLimit].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_timeLimit(TraversalMethod_timeLimitContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalMethod_times].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_times(TraversalMethod_timesContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_to_Direction_String}
  /// labeled alternative in {@link GremlinParser#traversalMethod_to}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_to_Direction_String(TraversalMethod_to_Direction_StringContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_to_String}
  /// labeled alternative in {@link GremlinParser#traversalMethod_to}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_to_String(TraversalMethod_to_StringContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_to_Traversal}
  /// labeled alternative in {@link GremlinParser#traversalMethod_to}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_to_Traversal(TraversalMethod_to_TraversalContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalMethod_toE].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_toE(TraversalMethod_toEContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_toLower_Empty}
  /// labeled alternative in {@link GremlinParser#traversalMethod_toLower}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_toLower_Empty(TraversalMethod_toLower_EmptyContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_toLower_Scope}
  /// labeled alternative in {@link GremlinParser#traversalMethod_toLower}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_toLower_Scope(TraversalMethod_toLower_ScopeContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_toUpper_Empty}
  /// labeled alternative in {@link GremlinParser#traversalMethod_toUpper}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_toUpper_Empty(TraversalMethod_toUpper_EmptyContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_toUpper_Scope}
  /// labeled alternative in {@link GremlinParser#traversalMethod_toUpper}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_toUpper_Scope(TraversalMethod_toUpper_ScopeContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalMethod_toV].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_toV(TraversalMethod_toVContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_tree_Empty}
  /// labeled alternative in {@link GremlinParser#traversalMethod_tree}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_tree_Empty(TraversalMethod_tree_EmptyContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_tree_String}
  /// labeled alternative in {@link GremlinParser#traversalMethod_tree}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_tree_String(TraversalMethod_tree_StringContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_trim_Empty}
  /// labeled alternative in {@link GremlinParser#traversalMethod_trim}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_trim_Empty(TraversalMethod_trim_EmptyContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_trim_Scope}
  /// labeled alternative in {@link GremlinParser#traversalMethod_trim}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_trim_Scope(TraversalMethod_trim_ScopeContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalMethod_unfold].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_unfold(TraversalMethod_unfoldContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalMethod_union].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_union(TraversalMethod_unionContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_until_Predicate}
  /// labeled alternative in {@link GremlinParser#traversalMethod_until}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_until_Predicate(TraversalMethod_until_PredicateContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_until_Traversal}
  /// labeled alternative in {@link GremlinParser#traversalMethod_until}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_until_Traversal(TraversalMethod_until_TraversalContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalMethod_value].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_value(TraversalMethod_valueContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_valueMap_String}
  /// labeled alternative in {@link GremlinParser#traversalMethod_valueMap}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_valueMap_String(TraversalMethod_valueMap_StringContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_valueMap_boolean_String}
  /// labeled alternative in {@link GremlinParser#traversalMethod_valueMap}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_valueMap_boolean_String(TraversalMethod_valueMap_boolean_StringContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalMethod_values].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_values(TraversalMethod_valuesContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_where_P}
  /// labeled alternative in {@link GremlinParser#traversalMethod_where}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_where_P(TraversalMethod_where_PContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_where_String_P}
  /// labeled alternative in {@link GremlinParser#traversalMethod_where}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_where_String_P(TraversalMethod_where_String_PContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_where_Traversal}
  /// labeled alternative in {@link GremlinParser#traversalMethod_where}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_where_Traversal(TraversalMethod_where_TraversalContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_with_String}
  /// labeled alternative in {@link GremlinParser#traversalMethod_with}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_with_String(TraversalMethod_with_StringContext ctx);

  /// Visit a parse tree produced by the {@code traversalMethod_with_String_Object}
  /// labeled alternative in {@link GremlinParser#traversalMethod_with}.
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_with_String_Object(TraversalMethod_with_String_ObjectContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalMethod_write].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMethod_write(TraversalMethod_writeContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalStrategy].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalStrategy(TraversalStrategyContext ctx);

  /// Visit a parse tree produced by [GremlinParser.configuration].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitConfiguration(ConfigurationContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalScope].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalScope(TraversalScopeContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalBarrier].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalBarrier(TraversalBarrierContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalT].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalT(TraversalTContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalTShort].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalTShort(TraversalTShortContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalTLong].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalTLong(TraversalTLongContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalMerge].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalMerge(TraversalMergeContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalOrder].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalOrder(TraversalOrderContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalDirection].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalDirection(TraversalDirectionContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalDirectionShort].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalDirectionShort(TraversalDirectionShortContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalDirectionLong].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalDirectionLong(TraversalDirectionLongContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalCardinality].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalCardinality(TraversalCardinalityContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalColumn].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalColumn(TraversalColumnContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalPop].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalPop(TraversalPopContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalOperator].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalOperator(TraversalOperatorContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalPick].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalPick(TraversalPickContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalDT].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalDT(TraversalDTContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalGType].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalGType(TraversalGTypeContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalPredicate].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalPredicate(TraversalPredicateContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalTerminalMethod].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalTerminalMethod(TraversalTerminalMethodContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalSackMethod].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalSackMethod(TraversalSackMethodContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalComparator].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalComparator(TraversalComparatorContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalFunction].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalFunction(TraversalFunctionContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalBiFunction].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalBiFunction(TraversalBiFunctionContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalPredicate_eq].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalPredicate_eq(TraversalPredicate_eqContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalPredicate_neq].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalPredicate_neq(TraversalPredicate_neqContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalPredicate_typeOf].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalPredicate_typeOf(TraversalPredicate_typeOfContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalPredicate_lt].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalPredicate_lt(TraversalPredicate_ltContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalPredicate_lte].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalPredicate_lte(TraversalPredicate_lteContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalPredicate_gt].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalPredicate_gt(TraversalPredicate_gtContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalPredicate_gte].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalPredicate_gte(TraversalPredicate_gteContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalPredicate_inside].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalPredicate_inside(TraversalPredicate_insideContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalPredicate_outside].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalPredicate_outside(TraversalPredicate_outsideContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalPredicate_between].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalPredicate_between(TraversalPredicate_betweenContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalPredicate_within].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalPredicate_within(TraversalPredicate_withinContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalPredicate_without].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalPredicate_without(TraversalPredicate_withoutContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalPredicate_not].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalPredicate_not(TraversalPredicate_notContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalPredicate_containing].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalPredicate_containing(TraversalPredicate_containingContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalPredicate_notContaining].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalPredicate_notContaining(TraversalPredicate_notContainingContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalPredicate_startingWith].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalPredicate_startingWith(TraversalPredicate_startingWithContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalPredicate_notStartingWith].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalPredicate_notStartingWith(TraversalPredicate_notStartingWithContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalPredicate_endingWith].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalPredicate_endingWith(TraversalPredicate_endingWithContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalPredicate_notEndingWith].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalPredicate_notEndingWith(TraversalPredicate_notEndingWithContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalPredicate_regex].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalPredicate_regex(TraversalPredicate_regexContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalPredicate_notRegex].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalPredicate_notRegex(TraversalPredicate_notRegexContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalTerminalMethod_explain].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalTerminalMethod_explain(TraversalTerminalMethod_explainContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalTerminalMethod_hasNext].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalTerminalMethod_hasNext(TraversalTerminalMethod_hasNextContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalTerminalMethod_iterate].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalTerminalMethod_iterate(TraversalTerminalMethod_iterateContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalTerminalMethod_tryNext].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalTerminalMethod_tryNext(TraversalTerminalMethod_tryNextContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalTerminalMethod_next].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalTerminalMethod_next(TraversalTerminalMethod_nextContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalTerminalMethod_toList].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalTerminalMethod_toList(TraversalTerminalMethod_toListContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalTerminalMethod_toSet].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalTerminalMethod_toSet(TraversalTerminalMethod_toSetContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalTerminalMethod_toBulkSet].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalTerminalMethod_toBulkSet(TraversalTerminalMethod_toBulkSetContext ctx);

  /// Visit a parse tree produced by [GremlinParser.withOptionKeys].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitWithOptionKeys(WithOptionKeysContext ctx);

  /// Visit a parse tree produced by [GremlinParser.connectedComponentConstants].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitConnectedComponentConstants(ConnectedComponentConstantsContext ctx);

  /// Visit a parse tree produced by [GremlinParser.pageRankConstants].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitPageRankConstants(PageRankConstantsContext ctx);

  /// Visit a parse tree produced by [GremlinParser.peerPressureConstants].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitPeerPressureConstants(PeerPressureConstantsContext ctx);

  /// Visit a parse tree produced by [GremlinParser.shortestPathConstants].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitShortestPathConstants(ShortestPathConstantsContext ctx);

  /// Visit a parse tree produced by [GremlinParser.withOptionsValues].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitWithOptionsValues(WithOptionsValuesContext ctx);

  /// Visit a parse tree produced by [GremlinParser.ioOptionsKeys].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitIoOptionsKeys(IoOptionsKeysContext ctx);

  /// Visit a parse tree produced by [GremlinParser.ioOptionsValues].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitIoOptionsValues(IoOptionsValuesContext ctx);

  /// Visit a parse tree produced by [GremlinParser.connectedComponentConstants_component].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitConnectedComponentConstants_component(ConnectedComponentConstants_componentContext ctx);

  /// Visit a parse tree produced by [GremlinParser.connectedComponentConstants_edges].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitConnectedComponentConstants_edges(ConnectedComponentConstants_edgesContext ctx);

  /// Visit a parse tree produced by [GremlinParser.connectedComponentConstants_propertyName].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitConnectedComponentConstants_propertyName(ConnectedComponentConstants_propertyNameContext ctx);

  /// Visit a parse tree produced by [GremlinParser.pageRankConstants_edges].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitPageRankConstants_edges(PageRankConstants_edgesContext ctx);

  /// Visit a parse tree produced by [GremlinParser.pageRankConstants_times].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitPageRankConstants_times(PageRankConstants_timesContext ctx);

  /// Visit a parse tree produced by [GremlinParser.pageRankConstants_propertyName].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitPageRankConstants_propertyName(PageRankConstants_propertyNameContext ctx);

  /// Visit a parse tree produced by [GremlinParser.peerPressureConstants_edges].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitPeerPressureConstants_edges(PeerPressureConstants_edgesContext ctx);

  /// Visit a parse tree produced by [GremlinParser.peerPressureConstants_times].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitPeerPressureConstants_times(PeerPressureConstants_timesContext ctx);

  /// Visit a parse tree produced by [GremlinParser.peerPressureConstants_propertyName].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitPeerPressureConstants_propertyName(PeerPressureConstants_propertyNameContext ctx);

  /// Visit a parse tree produced by [GremlinParser.shortestPathConstants_target].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitShortestPathConstants_target(ShortestPathConstants_targetContext ctx);

  /// Visit a parse tree produced by [GremlinParser.shortestPathConstants_edges].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitShortestPathConstants_edges(ShortestPathConstants_edgesContext ctx);

  /// Visit a parse tree produced by [GremlinParser.shortestPathConstants_distance].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitShortestPathConstants_distance(ShortestPathConstants_distanceContext ctx);

  /// Visit a parse tree produced by [GremlinParser.shortestPathConstants_maxDistance].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitShortestPathConstants_maxDistance(ShortestPathConstants_maxDistanceContext ctx);

  /// Visit a parse tree produced by [GremlinParser.shortestPathConstants_includeEdges].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitShortestPathConstants_includeEdges(ShortestPathConstants_includeEdgesContext ctx);

  /// Visit a parse tree produced by [GremlinParser.withOptionsConstants_tokens].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitWithOptionsConstants_tokens(WithOptionsConstants_tokensContext ctx);

  /// Visit a parse tree produced by [GremlinParser.withOptionsConstants_none].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitWithOptionsConstants_none(WithOptionsConstants_noneContext ctx);

  /// Visit a parse tree produced by [GremlinParser.withOptionsConstants_ids].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitWithOptionsConstants_ids(WithOptionsConstants_idsContext ctx);

  /// Visit a parse tree produced by [GremlinParser.withOptionsConstants_labels].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitWithOptionsConstants_labels(WithOptionsConstants_labelsContext ctx);

  /// Visit a parse tree produced by [GremlinParser.withOptionsConstants_keys].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitWithOptionsConstants_keys(WithOptionsConstants_keysContext ctx);

  /// Visit a parse tree produced by [GremlinParser.withOptionsConstants_values].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitWithOptionsConstants_values(WithOptionsConstants_valuesContext ctx);

  /// Visit a parse tree produced by [GremlinParser.withOptionsConstants_all].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitWithOptionsConstants_all(WithOptionsConstants_allContext ctx);

  /// Visit a parse tree produced by [GremlinParser.withOptionsConstants_indexer].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitWithOptionsConstants_indexer(WithOptionsConstants_indexerContext ctx);

  /// Visit a parse tree produced by [GremlinParser.withOptionsConstants_list].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitWithOptionsConstants_list(WithOptionsConstants_listContext ctx);

  /// Visit a parse tree produced by [GremlinParser.withOptionsConstants_map].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitWithOptionsConstants_map(WithOptionsConstants_mapContext ctx);

  /// Visit a parse tree produced by [GremlinParser.ioOptionsConstants_reader].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitIoOptionsConstants_reader(IoOptionsConstants_readerContext ctx);

  /// Visit a parse tree produced by [GremlinParser.ioOptionsConstants_writer].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitIoOptionsConstants_writer(IoOptionsConstants_writerContext ctx);

  /// Visit a parse tree produced by [GremlinParser.ioOptionsConstants_gryo].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitIoOptionsConstants_gryo(IoOptionsConstants_gryoContext ctx);

  /// Visit a parse tree produced by [GremlinParser.ioOptionsConstants_graphson].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitIoOptionsConstants_graphson(IoOptionsConstants_graphsonContext ctx);

  /// Visit a parse tree produced by [GremlinParser.ioOptionsConstants_graphml].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitIoOptionsConstants_graphml(IoOptionsConstants_graphmlContext ctx);

  /// Visit a parse tree produced by [GremlinParser.connectedComponentStringConstant].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitConnectedComponentStringConstant(ConnectedComponentStringConstantContext ctx);

  /// Visit a parse tree produced by [GremlinParser.pageRankStringConstant].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitPageRankStringConstant(PageRankStringConstantContext ctx);

  /// Visit a parse tree produced by [GremlinParser.peerPressureStringConstant].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitPeerPressureStringConstant(PeerPressureStringConstantContext ctx);

  /// Visit a parse tree produced by [GremlinParser.shortestPathStringConstant].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitShortestPathStringConstant(ShortestPathStringConstantContext ctx);

  /// Visit a parse tree produced by [GremlinParser.withOptionsStringConstant].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitWithOptionsStringConstant(WithOptionsStringConstantContext ctx);

  /// Visit a parse tree produced by [GremlinParser.ioOptionsStringConstant].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitIoOptionsStringConstant(IoOptionsStringConstantContext ctx);

  /// Visit a parse tree produced by [GremlinParser.booleanArgument].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitBooleanArgument(BooleanArgumentContext ctx);

  /// Visit a parse tree produced by [GremlinParser.integerArgument].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitIntegerArgument(IntegerArgumentContext ctx);

  /// Visit a parse tree produced by [GremlinParser.stringArgument].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitStringArgument(StringArgumentContext ctx);

  /// Visit a parse tree produced by [GremlinParser.stringNullableArgument].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitStringNullableArgument(StringNullableArgumentContext ctx);

  /// Visit a parse tree produced by [GremlinParser.stringNullableArgumentVarargs].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitStringNullableArgumentVarargs(StringNullableArgumentVarargsContext ctx);

  /// Visit a parse tree produced by [GremlinParser.dateArgument].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitDateArgument(DateArgumentContext ctx);

  /// Visit a parse tree produced by [GremlinParser.genericArgument].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitGenericArgument(GenericArgumentContext ctx);

  /// Visit a parse tree produced by [GremlinParser.genericArgumentVarargs].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitGenericArgumentVarargs(GenericArgumentVarargsContext ctx);

  /// Visit a parse tree produced by [GremlinParser.genericMapArgument].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitGenericMapArgument(GenericMapArgumentContext ctx);

  /// Visit a parse tree produced by [GremlinParser.genericMapNullableArgument].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitGenericMapNullableArgument(GenericMapNullableArgumentContext ctx);

  /// Visit a parse tree produced by [GremlinParser.nullableGenericLiteralMap].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitNullableGenericLiteralMap(NullableGenericLiteralMapContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalStrategyVarargs].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalStrategyVarargs(TraversalStrategyVarargsContext ctx);

  /// Visit a parse tree produced by [GremlinParser.traversalStrategyExpr].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitTraversalStrategyExpr(TraversalStrategyExprContext ctx);

  /// Visit a parse tree produced by [GremlinParser.classTypeList].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitClassTypeList(ClassTypeListContext ctx);

  /// Visit a parse tree produced by [GremlinParser.classTypeExpr].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitClassTypeExpr(ClassTypeExprContext ctx);

  /// Visit a parse tree produced by [GremlinParser.nestedTraversalList].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitNestedTraversalList(NestedTraversalListContext ctx);

  /// Visit a parse tree produced by [GremlinParser.nestedTraversalExpr].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitNestedTraversalExpr(NestedTraversalExprContext ctx);

  /// Visit a parse tree produced by [GremlinParser.genericCollectionLiteral].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitGenericCollectionLiteral(GenericCollectionLiteralContext ctx);

  /// Visit a parse tree produced by [GremlinParser.genericLiteralVarargs].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitGenericLiteralVarargs(GenericLiteralVarargsContext ctx);

  /// Visit a parse tree produced by [GremlinParser.genericLiteralExpr].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitGenericLiteralExpr(GenericLiteralExprContext ctx);

  /// Visit a parse tree produced by [GremlinParser.genericMapNullableLiteral].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitGenericMapNullableLiteral(GenericMapNullableLiteralContext ctx);

  /// Visit a parse tree produced by [GremlinParser.genericRangeLiteral].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitGenericRangeLiteral(GenericRangeLiteralContext ctx);

  /// Visit a parse tree produced by [GremlinParser.genericSetLiteral].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitGenericSetLiteral(GenericSetLiteralContext ctx);

  /// Visit a parse tree produced by [GremlinParser.stringNullableLiteralVarargs].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitStringNullableLiteralVarargs(StringNullableLiteralVarargsContext ctx);

  /// Visit a parse tree produced by [GremlinParser.genericLiteral].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitGenericLiteral(GenericLiteralContext ctx);

  /// Visit a parse tree produced by [GremlinParser.genericMapLiteral].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitGenericMapLiteral(GenericMapLiteralContext ctx);

  /// Visit a parse tree produced by [GremlinParser.mapKey].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitMapKey(MapKeyContext ctx);

  /// Visit a parse tree produced by [GremlinParser.mapEntry].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitMapEntry(MapEntryContext ctx);

  /// Visit a parse tree produced by [GremlinParser.stringLiteral].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitStringLiteral(StringLiteralContext ctx);

  /// Visit a parse tree produced by [GremlinParser.stringNullableLiteral].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitStringNullableLiteral(StringNullableLiteralContext ctx);

  /// Visit a parse tree produced by [GremlinParser.integerLiteral].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitIntegerLiteral(IntegerLiteralContext ctx);

  /// Visit a parse tree produced by [GremlinParser.floatLiteral].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitFloatLiteral(FloatLiteralContext ctx);

  /// Visit a parse tree produced by [GremlinParser.numericLiteral].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitNumericLiteral(NumericLiteralContext ctx);

  /// Visit a parse tree produced by [GremlinParser.booleanLiteral].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitBooleanLiteral(BooleanLiteralContext ctx);

  /// Visit a parse tree produced by [GremlinParser.dateLiteral].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitDateLiteral(DateLiteralContext ctx);

  /// Visit a parse tree produced by [GremlinParser.nullLiteral].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitNullLiteral(NullLiteralContext ctx);

  /// Visit a parse tree produced by [GremlinParser.nanLiteral].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitNanLiteral(NanLiteralContext ctx);

  /// Visit a parse tree produced by [GremlinParser.infLiteral].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitInfLiteral(InfLiteralContext ctx);

  /// Visit a parse tree produced by [GremlinParser.uuidLiteral].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitUuidLiteral(UuidLiteralContext ctx);

  /// Visit a parse tree produced by [GremlinParser.characterLiteral].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitCharacterLiteral(CharacterLiteralContext ctx);

  /// Visit a parse tree produced by [GremlinParser.durationLiteral].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitDurationLiteral(DurationLiteralContext ctx);

  /// Visit a parse tree produced by [GremlinParser.binaryLiteral].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitBinaryLiteral(BinaryLiteralContext ctx);

  /// Visit a parse tree produced by [GremlinParser.nakedKey].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitNakedKey(NakedKeyContext ctx);

  /// Visit a parse tree produced by [GremlinParser.classType].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitClassType(ClassTypeContext ctx);

  /// Visit a parse tree produced by [GremlinParser.variable].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitVariable(VariableContext ctx);

  /// Visit a parse tree produced by [GremlinParser.keyword].
  /// [ctx] the parse tree.
  /// Return the visitor result.
  T? visitKeyword(KeywordContext ctx);
}