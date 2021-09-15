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
grammar Gremlin;

/*********************************************
    PARSER RULES
**********************************************/

queryList
    : query (SEMI? query)* SEMI? EOF
    ;

query
    : traversalSource
    | traversalSource DOT transactionPart
    | rootTraversal
    | rootTraversal DOT traversalTerminalMethod
    | query DOT 'toString' LPAREN RPAREN
    | emptyQuery
    ;

emptyQuery
    : EmptyStringLiteral
    ;

traversalSource
    : TRAVERSAL_ROOT
    | TRAVERSAL_ROOT DOT traversalSourceSelfMethod
    | traversalSource DOT traversalSourceSelfMethod
    ;

transactionPart
    : 'tx' LPAREN RPAREN DOT 'commit' LPAREN RPAREN
    | 'tx' LPAREN RPAREN DOT 'rollback' LPAREN RPAREN
    | 'tx' LPAREN RPAREN
    ;

rootTraversal
    : traversalSource DOT traversalSourceSpawnMethod
    | traversalSource DOT traversalSourceSpawnMethod DOT chainedTraversal
    | traversalSource DOT traversalSourceSpawnMethod DOT chainedParentOfGraphTraversal
    ;

traversalSourceSelfMethod
    : traversalSourceSelfMethod_withBulk
    | traversalSourceSelfMethod_withPath
    | traversalSourceSelfMethod_withSack
    | traversalSourceSelfMethod_withSideEffect
    | traversalSourceSelfMethod_withStrategies
    | traversalSourceSelfMethod_with
    ;

traversalSourceSelfMethod_withBulk
    : 'withBulk' LPAREN booleanLiteral RPAREN
    ;

traversalSourceSelfMethod_withPath
    : 'withPath' LPAREN RPAREN
    ;

traversalSourceSelfMethod_withSack
    : 'withSack' LPAREN genericLiteral RPAREN
    | 'withSack' LPAREN genericLiteral COMMA traversalOperator RPAREN
    ;

traversalSourceSelfMethod_withSideEffect
    : 'withSideEffect' LPAREN stringLiteral COMMA genericLiteral RPAREN
    ;

traversalSourceSelfMethod_withStrategies
    : 'withStrategies' LPAREN traversalStrategy (COMMA traversalStrategyList)? RPAREN
    ;

traversalSourceSelfMethod_with
    : 'with' LPAREN stringLiteral RPAREN
    | 'with' LPAREN stringLiteral COMMA genericLiteral RPAREN
    ;

traversalSourceSpawnMethod
	: traversalSourceSpawnMethod_addE
	| traversalSourceSpawnMethod_addV
	| traversalSourceSpawnMethod_E
	| traversalSourceSpawnMethod_V
	| traversalSourceSpawnMethod_inject
    | traversalSourceSpawnMethod_io
    ;

traversalSourceSpawnMethod_addE
	: 'addE' LPAREN stringLiteral RPAREN
	| 'addE' LPAREN nestedTraversal RPAREN
	;

traversalSourceSpawnMethod_addV
	: 'addV' LPAREN RPAREN
	| 'addV' LPAREN stringLiteral RPAREN
	| 'addV' LPAREN nestedTraversal RPAREN
	;

traversalSourceSpawnMethod_E
	: 'E' LPAREN genericLiteralList RPAREN
	;

traversalSourceSpawnMethod_V
	: 'V' LPAREN genericLiteralList RPAREN
	;

traversalSourceSpawnMethod_inject
    : 'inject' LPAREN genericLiteralList RPAREN
    ;

traversalSourceSpawnMethod_io
    : 'io' LPAREN stringLiteral RPAREN
    ;

chainedTraversal
    : traversalMethod
    | chainedTraversal DOT traversalMethod
    | chainedTraversal DOT chainedParentOfGraphTraversal
    ;

chainedParentOfGraphTraversal
    : traversalSelfMethod
    | chainedParentOfGraphTraversal DOT traversalSelfMethod
    ;

nestedTraversal
    : rootTraversal
    | chainedTraversal
    | ANON_TRAVERSAL_ROOT DOT chainedTraversal
    ;

terminatedTraversal
    : rootTraversal DOT traversalTerminalMethod
    ;

/*********************************************
    GENERATED GRAMMAR - DO NOT CHANGE
**********************************************/

traversalMethod
	: traversalMethod_V
	| traversalMethod_addE
	| traversalMethod_addV
	| traversalMethod_aggregate
	| traversalMethod_and
	| traversalMethod_as
	| traversalMethod_barrier
	| traversalMethod_both
	| traversalMethod_bothE
	| traversalMethod_bothV
	| traversalMethod_branch
	| traversalMethod_by
	| traversalMethod_cap
	| traversalMethod_choose
	| traversalMethod_coalesce
	| traversalMethod_coin
	| traversalMethod_connectedComponent
	| traversalMethod_constant
	| traversalMethod_count
	| traversalMethod_cyclicPath
	| traversalMethod_dedup
	| traversalMethod_drop
	| traversalMethod_elementMap
	| traversalMethod_emit
	| traversalMethod_filter
	| traversalMethod_flatMap
	| traversalMethod_fold
	| traversalMethod_from
	| traversalMethod_group
	| traversalMethod_groupCount
	| traversalMethod_has
	| traversalMethod_hasId
	| traversalMethod_hasKey
	| traversalMethod_hasLabel
	| traversalMethod_hasNot
	| traversalMethod_hasValue
	| traversalMethod_id
	| traversalMethod_identity
	| traversalMethod_in
	| traversalMethod_inE
	| traversalMethod_inV
	| traversalMethod_index
	| traversalMethod_inject
	| traversalMethod_is
	| traversalMethod_key
	| traversalMethod_label
	| traversalMethod_limit
	| traversalMethod_local
	| traversalMethod_loops
	| traversalMethod_map
	| traversalMethod_match
	| traversalMethod_math
	| traversalMethod_max
	| traversalMethod_mean
	| traversalMethod_min
	| traversalMethod_not
	| traversalMethod_option
	| traversalMethod_optional
	| traversalMethod_or
	| traversalMethod_order
	| traversalMethod_otherV
	| traversalMethod_out
	| traversalMethod_outE
	| traversalMethod_outV
	| traversalMethod_pageRank
	| traversalMethod_path
	| traversalMethod_peerPressure
	| traversalMethod_profile
	| traversalMethod_project
	| traversalMethod_properties
	| traversalMethod_property
	| traversalMethod_propertyMap
	| traversalMethod_range
	| traversalMethod_read
	| traversalMethod_repeat
	| traversalMethod_sack
	| traversalMethod_sample
	| traversalMethod_select
	| traversalMethod_shortestPath
	| traversalMethod_sideEffect
	| traversalMethod_simplePath
	| traversalMethod_skip
	| traversalMethod_store
	| traversalMethod_subgraph
	| traversalMethod_sum
	| traversalMethod_tail
	| traversalMethod_timeLimit
	| traversalMethod_times
	| traversalMethod_to
	| traversalMethod_toE
	| traversalMethod_toV
	| traversalMethod_tree
	| traversalMethod_unfold
	| traversalMethod_union
	| traversalMethod_until
	| traversalMethod_value
	| traversalMethod_valueMap
	| traversalMethod_values
	| traversalMethod_where
	| traversalMethod_with
	| traversalMethod_write
	;
traversalMethod_V
	: 'V' LPAREN genericLiteralList RPAREN
	;

traversalMethod_addE
	: 'addE' LPAREN stringLiteral RPAREN #traversalMethod_addE_String
	| 'addE' LPAREN nestedTraversal RPAREN #traversalMethod_addE_Traversal
	;

traversalMethod_addV
	: 'addV' LPAREN RPAREN #traversalMethod_addV_Empty
	| 'addV' LPAREN stringLiteral RPAREN #traversalMethod_addV_String
	| 'addV' LPAREN nestedTraversal RPAREN #traversalMethod_addV_Traversal
	;

traversalMethod_aggregate
	: 'aggregate' LPAREN traversalScope COMMA stringLiteral RPAREN #traversalMethod_aggregate_Scope_String
	| 'aggregate' LPAREN stringLiteral RPAREN #traversalMethod_aggregate_String
	;

traversalMethod_and
	: 'and' LPAREN nestedTraversalList RPAREN
	;

traversalMethod_as
	: 'as' LPAREN stringLiteral (COMMA stringLiteralList)? RPAREN
	;

traversalMethod_barrier
	: 'barrier' LPAREN traversalSackMethod RPAREN #traversalMethod_barrier_Consumer
	| 'barrier' LPAREN RPAREN #traversalMethod_barrier_Empty
	| 'barrier' LPAREN integerLiteral RPAREN #traversalMethod_barrier_int
	;

traversalMethod_both
	: 'both' LPAREN stringLiteralList RPAREN
	;

traversalMethod_bothE
	: 'bothE' LPAREN stringLiteralList RPAREN
	;

traversalMethod_bothV
	: 'bothV' LPAREN RPAREN
	;

traversalMethod_branch
	: 'branch' LPAREN nestedTraversal RPAREN
	;

traversalMethod_by
	: 'by' LPAREN traversalComparator RPAREN #traversalMethod_by_Comparator
	| 'by' LPAREN RPAREN #traversalMethod_by_Empty
	| 'by' LPAREN traversalFunction RPAREN #traversalMethod_by_Function
	| 'by' LPAREN traversalFunction COMMA traversalComparator RPAREN #traversalMethod_by_Function_Comparator
	| 'by' LPAREN traversalOrder RPAREN #traversalMethod_by_Order
	| 'by' LPAREN stringLiteral RPAREN #traversalMethod_by_String
	| 'by' LPAREN stringLiteral COMMA traversalComparator RPAREN #traversalMethod_by_String_Comparator
	| 'by' LPAREN traversalToken RPAREN #traversalMethod_by_T
	| 'by' LPAREN nestedTraversal RPAREN #traversalMethod_by_Traversal
	| 'by' LPAREN nestedTraversal COMMA traversalComparator RPAREN #traversalMethod_by_Traversal_Comparator
	;

traversalMethod_cap
	: 'cap' LPAREN stringLiteral (COMMA stringLiteralList)? RPAREN
	;

traversalMethod_choose
	: 'choose' LPAREN traversalFunction RPAREN #traversalMethod_choose_Function
	| 'choose' LPAREN traversalPredicate COMMA nestedTraversal RPAREN #traversalMethod_choose_Predicate_Traversal
	| 'choose' LPAREN traversalPredicate COMMA nestedTraversal COMMA nestedTraversal RPAREN #traversalMethod_choose_Predicate_Traversal_Traversal
	| 'choose' LPAREN nestedTraversal RPAREN #traversalMethod_choose_Traversal
	| 'choose' LPAREN nestedTraversal COMMA nestedTraversal RPAREN #traversalMethod_choose_Traversal_Traversal
	| 'choose' LPAREN nestedTraversal COMMA nestedTraversal COMMA nestedTraversal RPAREN #traversalMethod_choose_Traversal_Traversal_Traversal
	;

traversalMethod_coalesce
	: 'coalesce' LPAREN nestedTraversalList RPAREN
	;

traversalMethod_coin
	: 'coin' LPAREN floatLiteral RPAREN
	;

traversalMethod_connectedComponent
	: 'connectedComponent' LPAREN RPAREN
	;

traversalMethod_constant
	: 'constant' LPAREN genericLiteral RPAREN
	;

traversalMethod_count
	: 'count' LPAREN RPAREN #traversalMethod_count_Empty
	| 'count' LPAREN traversalScope RPAREN #traversalMethod_count_Scope
	;

traversalMethod_cyclicPath
	: 'cyclicPath' LPAREN RPAREN
	;

traversalMethod_dedup
	: 'dedup' LPAREN traversalScope (COMMA stringLiteralList)? RPAREN #traversalMethod_dedup_Scope_String
	| 'dedup' LPAREN stringLiteralList RPAREN #traversalMethod_dedup_String
	;

traversalMethod_drop
	: 'drop' LPAREN RPAREN
	;

traversalMethod_elementMap
	: 'elementMap' LPAREN stringLiteralList RPAREN
	;

traversalMethod_emit
	: 'emit' LPAREN RPAREN #traversalMethod_emit_Empty
	| 'emit' LPAREN traversalPredicate RPAREN #traversalMethod_emit_Predicate
	| 'emit' LPAREN nestedTraversal RPAREN #traversalMethod_emit_Traversal
	;

traversalMethod_filter
	: 'filter' LPAREN traversalPredicate RPAREN #traversalMethod_filter_Predicate
	| 'filter' LPAREN nestedTraversal RPAREN #traversalMethod_filter_Traversal
	;

traversalMethod_flatMap
	: 'flatMap' LPAREN nestedTraversal RPAREN
	;

traversalMethod_fold
	: 'fold' LPAREN RPAREN #traversalMethod_fold_Empty
	| 'fold' LPAREN genericLiteral COMMA traversalBiFunction RPAREN #traversalMethod_fold_Object_BiFunction
	;

traversalMethod_from
	: 'from' LPAREN stringLiteral RPAREN #traversalMethod_from_String
	| 'from' LPAREN nestedTraversal RPAREN #traversalMethod_from_Traversal
	;

traversalMethod_group
	: 'group' LPAREN RPAREN #traversalMethod_group_Empty
	| 'group' LPAREN stringLiteral RPAREN #traversalMethod_group_String
	;

traversalMethod_groupCount
	: 'groupCount' LPAREN RPAREN #traversalMethod_groupCount_Empty
	| 'groupCount' LPAREN stringLiteral RPAREN #traversalMethod_groupCount_String
	;

traversalMethod_has
	: 'has' LPAREN stringLiteral RPAREN #traversalMethod_has_String
	| 'has' LPAREN stringLiteral COMMA genericLiteral RPAREN #traversalMethod_has_String_Object
	| 'has' LPAREN stringLiteral COMMA traversalPredicate RPAREN #traversalMethod_has_String_P
	| 'has' LPAREN stringLiteral COMMA stringLiteral COMMA genericLiteral RPAREN #traversalMethod_has_String_String_Object
	| 'has' LPAREN stringLiteral COMMA stringLiteral COMMA traversalPredicate RPAREN #traversalMethod_has_String_String_P
	| 'has' LPAREN stringLiteral COMMA nestedTraversal RPAREN #traversalMethod_has_String_Traversal
	| 'has' LPAREN traversalToken COMMA genericLiteral RPAREN #traversalMethod_has_T_Object
	| 'has' LPAREN traversalToken COMMA traversalPredicate RPAREN #traversalMethod_has_T_P
	| 'has' LPAREN traversalToken COMMA nestedTraversal RPAREN #traversalMethod_has_T_Traversal
	;

traversalMethod_hasId
	: 'hasId' LPAREN genericLiteral (COMMA genericLiteralList)? RPAREN #traversalMethod_hasId_Object_Object
	| 'hasId' LPAREN traversalPredicate RPAREN #traversalMethod_hasId_P
	;

traversalMethod_hasKey
	: 'hasKey' LPAREN traversalPredicate RPAREN #traversalMethod_hasKey_P
	| 'hasKey' LPAREN stringLiteral (COMMA stringLiteralList)? RPAREN #traversalMethod_hasKey_String_String
	;

traversalMethod_hasLabel
	: 'hasLabel' LPAREN traversalPredicate RPAREN #traversalMethod_hasLabel_P
	| 'hasLabel' LPAREN stringLiteral (COMMA stringLiteralList)? RPAREN #traversalMethod_hasLabel_String_String
	;

traversalMethod_hasNot
	: 'hasNot' LPAREN stringLiteral RPAREN
	;

traversalMethod_hasValue
	: 'hasValue' LPAREN genericLiteral (COMMA genericLiteralList)? RPAREN #traversalMethod_hasValue_Object_Object
	| 'hasValue' LPAREN traversalPredicate RPAREN #traversalMethod_hasValue_P
	;

traversalMethod_id
	: 'id' LPAREN RPAREN
	;

traversalMethod_identity
	: 'identity' LPAREN RPAREN
	;

traversalMethod_in
	: 'in' LPAREN stringLiteralList RPAREN
	;

traversalMethod_inE
	: 'inE' LPAREN stringLiteralList RPAREN
	;

traversalMethod_inV
	: 'inV' LPAREN RPAREN
	;

traversalMethod_index
	: 'index' LPAREN RPAREN
	;

traversalMethod_inject
	: 'inject' LPAREN genericLiteralList RPAREN
	;

traversalMethod_is
	: 'is' LPAREN genericLiteral RPAREN #traversalMethod_is_Object
	| 'is' LPAREN traversalPredicate RPAREN #traversalMethod_is_P
	;

traversalMethod_key
	: 'key' LPAREN RPAREN
	;

traversalMethod_label
	: 'label' LPAREN RPAREN
	;

traversalMethod_limit
	: 'limit' LPAREN traversalScope COMMA integerLiteral RPAREN #traversalMethod_limit_Scope_long
	| 'limit' LPAREN integerLiteral RPAREN #traversalMethod_limit_long
	;

traversalMethod_local
	: 'local' LPAREN nestedTraversal RPAREN
	;

traversalMethod_loops
	: 'loops' LPAREN RPAREN #traversalMethod_loops_Empty
	| 'loops' LPAREN stringLiteral RPAREN #traversalMethod_loops_String
	;

traversalMethod_map
	: 'map' LPAREN nestedTraversal RPAREN
	;

traversalMethod_match
	: 'match' LPAREN nestedTraversalList RPAREN
	;

traversalMethod_math
	: 'math' LPAREN stringLiteral RPAREN
	;

traversalMethod_max
	: 'max' LPAREN RPAREN #traversalMethod_max_Empty
	| 'max' LPAREN traversalScope RPAREN #traversalMethod_max_Scope
	;

traversalMethod_mean
	: 'mean' LPAREN RPAREN #traversalMethod_mean_Empty
	| 'mean' LPAREN traversalScope RPAREN #traversalMethod_mean_Scope
	;

traversalMethod_min
	: 'min' LPAREN RPAREN #traversalMethod_min_Empty
	| 'min' LPAREN traversalScope RPAREN #traversalMethod_min_Scope
	;

traversalMethod_not
	: 'not' LPAREN nestedTraversal RPAREN
	;

traversalMethod_option
	: 'option' LPAREN traversalPredicate COMMA nestedTraversal RPAREN #traversalMethod_option_Predicate_Traversal
	| 'option' LPAREN genericLiteral COMMA nestedTraversal RPAREN #traversalMethod_option_Object_Traversal
	| 'option' LPAREN nestedTraversal RPAREN #traversalMethod_option_Traversal
	;

traversalMethod_optional
	: 'optional' LPAREN nestedTraversal RPAREN
	;

traversalMethod_or
	: 'or' LPAREN nestedTraversalList RPAREN
	;

traversalMethod_order
	: 'order' LPAREN RPAREN #traversalMethod_order_Empty
	| 'order' LPAREN traversalScope RPAREN #traversalMethod_order_Scope
	;

traversalMethod_otherV
	: 'otherV' LPAREN RPAREN
	;

traversalMethod_out
	: 'out' LPAREN stringLiteralList RPAREN
	;

traversalMethod_outE
	: 'outE' LPAREN stringLiteralList RPAREN
	;

traversalMethod_outV
	: 'outV' LPAREN RPAREN
	;

traversalMethod_pageRank
	: 'pageRank' LPAREN RPAREN #traversalMethod_pageRank_Empty
	| 'pageRank' LPAREN floatLiteral RPAREN #traversalMethod_pageRank_double
	;

traversalMethod_path
	: 'path' LPAREN RPAREN
	;

traversalMethod_peerPressure
	: 'peerPressure' LPAREN RPAREN
	;

traversalMethod_profile
	: 'profile' LPAREN RPAREN #traversalMethod_profile_Empty
	| 'profile' LPAREN stringLiteral RPAREN #traversalMethod_profile_String
	;

traversalMethod_project
	: 'project' LPAREN stringLiteral (COMMA stringLiteralList)? RPAREN
	;

traversalMethod_properties
	: 'properties' LPAREN stringLiteralList RPAREN
	;

traversalMethod_property
	: 'property' LPAREN traversalCardinality COMMA genericLiteral COMMA genericLiteral (COMMA genericLiteralList)? RPAREN #traversalMethod_property_Cardinality_Object_Object_Object
	| 'property' LPAREN genericLiteral COMMA genericLiteral (COMMA genericLiteralList)? RPAREN #traversalMethod_property_Object_Object_Object
	;

traversalMethod_propertyMap
	: 'propertyMap' LPAREN stringLiteralList RPAREN
	;

traversalMethod_range
	: 'range' LPAREN traversalScope COMMA integerLiteral COMMA integerLiteral RPAREN #traversalMethod_range_Scope_long_long
	| 'range' LPAREN integerLiteral COMMA integerLiteral RPAREN #traversalMethod_range_long_long
	;

traversalMethod_read
	: 'read' LPAREN RPAREN
	;

traversalMethod_repeat
	: 'repeat' LPAREN stringLiteral COMMA nestedTraversal RPAREN #traversalMethod_repeat_String_Traversal
	| 'repeat' LPAREN nestedTraversal RPAREN #traversalMethod_repeat_Traversal
	;

traversalMethod_sack
	: 'sack' LPAREN traversalBiFunction RPAREN #traversalMethod_sack_BiFunction
	| 'sack' LPAREN RPAREN #traversalMethod_sack_Empty
	;

traversalMethod_sample
	: 'sample' LPAREN traversalScope COMMA integerLiteral RPAREN #traversalMethod_sample_Scope_int
	| 'sample' LPAREN integerLiteral RPAREN #traversalMethod_sample_int
	;

traversalMethod_select
	: 'select' LPAREN traversalColumn RPAREN #traversalMethod_select_Column
	| 'select' LPAREN traversalPop COMMA stringLiteral RPAREN #traversalMethod_select_Pop_String
	| 'select' LPAREN traversalPop COMMA stringLiteral COMMA stringLiteral (COMMA stringLiteralList)? RPAREN #traversalMethod_select_Pop_String_String_String
	| 'select' LPAREN traversalPop COMMA nestedTraversal RPAREN #traversalMethod_select_Pop_Traversal
	| 'select' LPAREN stringLiteral RPAREN #traversalMethod_select_String
	| 'select' LPAREN stringLiteral COMMA stringLiteral (COMMA stringLiteralList)? RPAREN #traversalMethod_select_String_String_String
	| 'select' LPAREN nestedTraversal RPAREN #traversalMethod_select_Traversal
	;

traversalMethod_shortestPath
	: 'shortestPath' LPAREN RPAREN
	;

traversalMethod_sideEffect
	: 'sideEffect' LPAREN nestedTraversal RPAREN
	;

traversalMethod_simplePath
	: 'simplePath' LPAREN RPAREN
	;

traversalMethod_skip
	: 'skip' LPAREN traversalScope COMMA integerLiteral RPAREN #traversalMethod_skip_Scope_long
	| 'skip' LPAREN integerLiteral RPAREN #traversalMethod_skip_long
	;

traversalMethod_store
	: 'store' LPAREN stringLiteral RPAREN
	;

traversalMethod_subgraph
	: 'subgraph' LPAREN stringLiteral RPAREN
	;

traversalMethod_sum
	: 'sum' LPAREN RPAREN #traversalMethod_sum_Empty
	| 'sum' LPAREN traversalScope RPAREN #traversalMethod_sum_Scope
	;

traversalMethod_tail
	: 'tail' LPAREN RPAREN #traversalMethod_tail_Empty
	| 'tail' LPAREN traversalScope RPAREN #traversalMethod_tail_Scope
	| 'tail' LPAREN traversalScope COMMA integerLiteral RPAREN #traversalMethod_tail_Scope_long
	| 'tail' LPAREN integerLiteral RPAREN #traversalMethod_tail_long
	;

traversalMethod_timeLimit
	: 'timeLimit' LPAREN integerLiteral RPAREN
	;

traversalMethod_times
	: 'times' LPAREN integerLiteral RPAREN
	;

traversalMethod_to
	: 'to' LPAREN traversalDirection (COMMA stringLiteralList)? RPAREN #traversalMethod_to_Direction_String
	| 'to' LPAREN stringLiteral RPAREN #traversalMethod_to_String
	| 'to' LPAREN nestedTraversal RPAREN #traversalMethod_to_Traversal
	;

traversalMethod_toE
	: 'toE' LPAREN traversalDirection (COMMA stringLiteralList)? RPAREN
	;

traversalMethod_toV
	: 'toV' LPAREN traversalDirection RPAREN
	;

traversalMethod_tree
	: 'tree' LPAREN RPAREN #traversalMethod_tree_Empty
	| 'tree' LPAREN stringLiteral RPAREN #traversalMethod_tree_String
	;

traversalMethod_unfold
	: 'unfold' LPAREN RPAREN
	;

traversalMethod_union
	: 'union' LPAREN nestedTraversalList RPAREN
	;

traversalMethod_until
	: 'until' LPAREN traversalPredicate RPAREN #traversalMethod_until_Predicate
	| 'until' LPAREN nestedTraversal RPAREN #traversalMethod_until_Traversal
	;

traversalMethod_value
	: 'value' LPAREN RPAREN
	;

traversalMethod_valueMap
	: 'valueMap' LPAREN stringLiteralList RPAREN #traversalMethod_valueMap_String
	| 'valueMap' LPAREN booleanLiteral (COMMA stringLiteralList)? RPAREN #traversalMethod_valueMap_boolean_String
	;

traversalMethod_values
	: 'values' LPAREN stringLiteralList RPAREN
	;

traversalMethod_where
	: 'where' LPAREN traversalPredicate RPAREN #traversalMethod_where_P
	| 'where' LPAREN stringLiteral COMMA traversalPredicate RPAREN #traversalMethod_where_String_P
	| 'where' LPAREN nestedTraversal RPAREN #traversalMethod_where_Traversal
	;

traversalMethod_with
	: 'with' LPAREN stringLiteral RPAREN #traversalMethod_with_String
	| 'with' LPAREN stringLiteral COMMA genericLiteral RPAREN #traversalMethod_with_String_Object
	;

traversalMethod_write
	: 'write' LPAREN RPAREN
	;

/*********************************************
    ARGUMENT AND TERMINAL RULES
**********************************************/

traversalStrategy
//  : 'ConnectiveStrategy' - not supported as it is a default strategy and we don't allow removal at this time
//  | 'ElementIdStrategy' - not supported as the configuration takes a lambda
//  | 'EventStrategy' - not supported as there is no way to send events back to the client
//  | 'HaltedTraverserStrategy' - not supported as it is not typically relevant to OLTP
//  | 'OptionsStrategy' - not supported as it's internal to with()
    : 'new' 'PartitionStrategy' LPAREN traversalStrategyArgs_PartitionStrategy? (COMMA traversalStrategyArgs_PartitionStrategy)* RPAREN
//  | 'RequirementStrategy' - not supported as it's internally relevant only
//  | 'SackStrategy' - not supported directly as it's internal to withSack()
    | 'new' 'SeedStrategy' LPAREN integerLiteral RPAREN
//  | 'SideEffectStrategy' - not supported directly as it's internal to withSideEffect()
    | 'new' 'SubgraphStrategy' LPAREN traversalStrategyArgs_SubgraphStrategy? (COMMA traversalStrategyArgs_SubgraphStrategy)* RPAREN
//  | 'MatchAlgorithmStrategy' - not supported directly as it's internal to match()
//  | 'ProfileStrategy' - not supported directly as it's internal to profile()
//  | 'ReferenceElementStrategy' - not supported directly as users really can't/shouldn't change this in our context of a remote Gremlin provider
//  | 'AdjacentToIncidentStrategy' - not supported as it is a default strategy and we don't allow removal at this time
//  | 'ByModulatorOptimizationStrategy' - not supported as it is a default strategy and we don't allow removal at this time
//  | 'CountStrategy' - not supported as it is a default strategy and we don't allow removal at this time
//  | 'EarlyLimitStrategy' - not supported as it is a default strategy and we don't allow removal at this time
//  | 'FilterRankingStrategy' - not supported as it is a default strategy and we don't allow removal at this time
//  | 'IdentityRemovalStrategy' - not supported as it is a default strategy and we don't allow removal at this time
//  | 'IncidentToAdjacentStrategy' - not supported as it is a default strategy and we don't allow removal at this time
//  | 'InlineFilterStrategy' - not supported as it is a default strategy and we don't allow removal at this time
//  | 'LazyBarrierStrategy' - not supported as it is a default strategy and we don't allow removal at this time
//  | 'MatchPredicateStrategy' - not supported as it is a default strategy and we don't allow removal at this time
//  | 'OrderLimitStrategy' - not supported as it is a default strategy and we don't allow removal at this time
//  | 'PathProcessorStrategy' - not supported as it is a default strategy and we don't allow removal at this time
//  | 'PathRetractionStrategy' - not supported as it is a default strategy and we don't allow removal at this time
//  | 'RepeatUnrollStrategy' - not supported as it is a default strategy and we don't allow removal at this time
//  | 'ComputerVerificationStrategy' - not supported since it's GraphComputer related
    | 'new' 'EdgeLabelVerificationStrategy' LPAREN traversalStrategyArgs_EdgeLabelVerificationStrategy? (COMMA traversalStrategyArgs_EdgeLabelVerificationStrategy)* RPAREN
//  | 'LambdaRestrictionStrategy' - not supported as we don't support lambdas in any situation
    | 'ReadOnlyStrategy'
    | 'new' 'ReservedKeysVerificationStrategy' LPAREN traversalStrategyArgs_ReservedKeysVerificationStrategy? (COMMA traversalStrategyArgs_ReservedKeysVerificationStrategy)* RPAREN
//  | 'StandardVerificationStrategy' - not supported since this is an interal strategy
    ;

traversalStrategyArgs_PartitionStrategy
    : 'includeMetaProperties' COLON booleanLiteral
    | 'writePartition' COLON stringLiteral
    | 'partitionKey' COLON stringLiteral
    | 'readPartitions' COLON stringLiteralList
    ;

traversalStrategyArgs_SubgraphStrategy
    : 'vertices' COLON nestedTraversal
    | 'edges' COLON nestedTraversal
    | 'vertexProperties' COLON nestedTraversal
    | 'checkAdjacentVertices' COLON booleanLiteral
    ;

traversalStrategyArgs_EdgeLabelVerificationStrategy
    : 'throwException' COLON booleanLiteral
    | 'logWarning' COLON booleanLiteral
    ;

traversalStrategyArgs_ReservedKeysVerificationStrategy
    : 'keys' COLON stringLiteralList
    | 'throwException' COLON booleanLiteral
    | 'logWarning' COLON booleanLiteral
    ;

traversalScope
    : 'local' | 'Scope.local'
    | 'global' | 'Scope.global'
    ;

traversalToken
    : 'id' | 'T.id'
    | 'label' | 'T.label'
    | 'key' | 'T.key'
    | 'value' | 'T.value'
    ;

traversalOrder
    : 'incr' | 'Order.incr'
    | 'decr' | 'Order.decr'
    | 'asc'  | 'Order.asc'
    | 'desc' | 'Order.desc'
    | 'shuffle' | 'Order.shuffle'
    ;

traversalDirection
    : 'IN' | 'Direction.IN'
    | 'OUT' | 'Direction.OUT'
    | 'BOTH' | 'Direction.BOTH'
    ;

traversalCardinality
    : 'single' | 'Cardinality.single'
    | 'set' | 'Cardinality.set'
    | 'list' | 'Cardinality.list'
    ;

traversalColumn
    : 'keys' | 'Column.keys'
    | 'values' | 'Column.values'
    ;

traversalPop
    : 'first' | 'Pop.first'
    | 'last' | 'Pop.last'
    | 'all' | 'Pop.all'
    | 'mixed' | 'Pop.mixed'
    ;

traversalOperator
    : 'addAll' | 'Operator.addAll'
    | 'and' | 'Operator.and'
    | 'assign' | 'Operator.assign'
    | 'div' | 'Operator.div'
    | 'max' | 'Operator.max'
    | 'min' | 'Operator.min'
    | 'minus' | 'Operator.minus'
    | 'mult' | 'Operator.mult'
    | 'or' | 'Operator.or'
    | 'sum' | 'Operator.sum'
    | 'sumLong' | 'Operator.sumLong'
    ;

traversalOptionParent
    : 'any' | 'Pick.any'
    | 'none' | 'Pick.none'
    ;

traversalPredicate
    : traversalPredicate_eq
    | traversalPredicate_neq
    | traversalPredicate_lt
    | traversalPredicate_lte
    | traversalPredicate_gt
    | traversalPredicate_gte
    | traversalPredicate_inside
    | traversalPredicate_outside
    | traversalPredicate_between
    | traversalPredicate_within
    | traversalPredicate_without
    | traversalPredicate_not
    | traversalPredicate_startingWith
    | traversalPredicate_notStartingWith
    | traversalPredicate_endingWith
    | traversalPredicate_notEndingWith
    | traversalPredicate_containing
    | traversalPredicate_notContaining
    | traversalPredicate DOT 'and' LPAREN traversalPredicate RPAREN
    | traversalPredicate DOT 'or' LPAREN traversalPredicate RPAREN
    | traversalPredicate DOT 'negate' LPAREN RPAREN
    ;

traversalTerminalMethod
    : traversalTerminalMethod_explain
    | traversalTerminalMethod_iterate
    | traversalTerminalMethod_hasNext
    | traversalTerminalMethod_tryNext
    | traversalTerminalMethod_next
    | traversalTerminalMethod_toList
    | traversalTerminalMethod_toSet
    | traversalTerminalMethod_toBulkSet
    ;

traversalSackMethod
    : 'normSack' | 'Barrier.normSack'
    ;

traversalSelfMethod
    : traversalSelfMethod_none
    ;

// Additional special rules that are derived from above
// These are used to restrict broad method signatures that accept lambdas
// to a smaller set.
traversalComparator
    : traversalOrder
    ;

traversalFunction
    : traversalToken
    | traversalColumn
    ;

traversalBiFunction
    : traversalOperator
    ;

traversalPredicate_eq
    : ('P.eq' | 'eq') LPAREN genericLiteral RPAREN
    ;

traversalPredicate_neq
    : ('P.neq' | 'neq') LPAREN genericLiteral RPAREN
    ;

traversalPredicate_lt
    : ('P.lt' | 'lt') LPAREN genericLiteral RPAREN
    ;

traversalPredicate_lte
    : ('P.lte' | 'lte') LPAREN genericLiteral RPAREN
    ;

traversalPredicate_gt
    : ('P.gt' | 'gt') LPAREN genericLiteral RPAREN
    ;

traversalPredicate_gte
    : ('P.gte' | 'gte') LPAREN genericLiteral RPAREN
    ;

traversalPredicate_inside
    : ('P.inside' | 'inside') LPAREN genericLiteral COMMA genericLiteral RPAREN
    ;

traversalPredicate_outside
    : ('P.outside' | 'outside') LPAREN genericLiteral COMMA genericLiteral RPAREN
    ;

traversalPredicate_between
    : ('P.between' | 'between') LPAREN genericLiteral COMMA genericLiteral RPAREN
    ;

traversalPredicate_within
    : ('P.within' | 'within') LPAREN genericLiteralList RPAREN
    ;

traversalPredicate_without
    : ('P.without' | 'without') LPAREN genericLiteralList RPAREN
    ;

traversalPredicate_not
    : ('P.not' | 'not') LPAREN traversalPredicate RPAREN
    ;

traversalPredicate_containing
    : ('TextP.containing' | 'containing') LPAREN stringLiteral RPAREN
    ;

traversalPredicate_notContaining
    : ('TextP.notContaining' | 'notContaining') LPAREN stringLiteral RPAREN
    ;

traversalPredicate_startingWith
    : ('TextP.startingWith' | 'startingWith') LPAREN stringLiteral RPAREN
    ;

traversalPredicate_notStartingWith
    : ('TextP.notStartingWith' | 'notStartingWith') LPAREN stringLiteral RPAREN
    ;

traversalPredicate_endingWith
    : ('TextP.endingWith' | 'endingWith') LPAREN stringLiteral RPAREN
    ;

traversalPredicate_notEndingWith
    : ('TextP.notEndingWith' | 'notEndingWith') LPAREN stringLiteral RPAREN
    ;

traversalTerminalMethod_explain
    : 'explain' LPAREN RPAREN
    ;

traversalTerminalMethod_hasNext
    : 'hasNext' LPAREN RPAREN
    ;

traversalTerminalMethod_iterate
    : 'iterate' LPAREN RPAREN
    ;

traversalTerminalMethod_tryNext
    : 'tryNext' LPAREN RPAREN
    ;

traversalTerminalMethod_next
    : 'next' LPAREN RPAREN
    | 'next' LPAREN integerLiteral RPAREN
    ;

traversalTerminalMethod_toList
    : 'toList' LPAREN RPAREN
    ;

traversalTerminalMethod_toSet
    : 'toSet' LPAREN RPAREN
    ;

traversalTerminalMethod_toBulkSet
    : 'toBulkSet' LPAREN RPAREN
    ;

traversalSelfMethod_none
    : 'none' LPAREN RPAREN
    ;

// Gremlin specific lexer rules

gremlinStringConstants
    : withOptionsStringConstants
    | shortestPathStringConstants
    | connectedComponentConstants
    | pageRankStringConstants
    | peerPressureStringConstants
    | ioOptionsStringConstants
    ;

connectedComponentConstants
    : gremlinStringConstants_connectedComponentStringConstants_component
    | gremlinStringConstants_connectedComponentStringConstants_edges
    | gremlinStringConstants_connectedComponentStringConstants_propertyName
    ;

pageRankStringConstants
    : gremlinStringConstants_pageRankStringConstants_edges
    | gremlinStringConstants_pageRankStringConstants_times
    | gremlinStringConstants_pageRankStringConstants_propertyName
    ;

peerPressureStringConstants
    : gremlinStringConstants_peerPressureStringConstants_edges
    | gremlinStringConstants_peerPressureStringConstants_times
    | gremlinStringConstants_peerPressureStringConstants_propertyName
    ;

shortestPathStringConstants
    : gremlinStringConstants_shortestPathStringConstants_target
    | gremlinStringConstants_shortestPathStringConstants_edges
    | gremlinStringConstants_shortestPathStringConstants_distance
    | gremlinStringConstants_shortestPathStringConstants_maxDistance
    | gremlinStringConstants_shortestPathStringConstants_includeEdges
    ;

withOptionsStringConstants
    : gremlinStringConstants_withOptionsStringConstants_tokens
    | gremlinStringConstants_withOptionsStringConstants_none
    | gremlinStringConstants_withOptionsStringConstants_ids
    | gremlinStringConstants_withOptionsStringConstants_labels
    | gremlinStringConstants_withOptionsStringConstants_keys
    | gremlinStringConstants_withOptionsStringConstants_values
    | gremlinStringConstants_withOptionsStringConstants_all
    | gremlinStringConstants_withOptionsStringConstants_indexer
    | gremlinStringConstants_withOptionsStringConstants_list
    | gremlinStringConstants_withOptionsStringConstants_map
    ;

ioOptionsStringConstants
    : gremlinStringConstants_ioOptionsStringConstants_reader
    | gremlinStringConstants_ioOptionsStringConstants_writer
    | gremlinStringConstants_ioOptionsStringConstants_gryo
    | gremlinStringConstants_ioOptionsStringConstants_graphson
    | gremlinStringConstants_ioOptionsStringConstants_graphml
    ;

gremlinStringConstants_connectedComponentStringConstants_component
    : connectedComponentStringConstant DOT 'component'
    ;

gremlinStringConstants_connectedComponentStringConstants_edges
    : connectedComponentStringConstant DOT 'edges'
    ;

gremlinStringConstants_connectedComponentStringConstants_propertyName
    : connectedComponentStringConstant DOT 'propertyName'
    ;

gremlinStringConstants_pageRankStringConstants_edges
    : pageRankStringConstant DOT 'edges'
    ;

gremlinStringConstants_pageRankStringConstants_times
    : pageRankStringConstant DOT 'times'
    ;

gremlinStringConstants_pageRankStringConstants_propertyName
    : pageRankStringConstant DOT 'propertyName'
    ;

gremlinStringConstants_peerPressureStringConstants_edges
    : peerPressureStringConstant DOT 'edges'
    ;

gremlinStringConstants_peerPressureStringConstants_times
    : peerPressureStringConstant DOT 'times'
    ;

gremlinStringConstants_peerPressureStringConstants_propertyName
    : peerPressureStringConstant DOT 'propertyName'
    ;

gremlinStringConstants_shortestPathStringConstants_target
    : shortestPathStringConstant DOT 'target'
    ;

gremlinStringConstants_shortestPathStringConstants_edges
    : shortestPathStringConstant DOT 'edges'
    ;

gremlinStringConstants_shortestPathStringConstants_distance
    : shortestPathStringConstant DOT 'distance'
    ;

gremlinStringConstants_shortestPathStringConstants_maxDistance
    : shortestPathStringConstant DOT 'maxDistance'
    ;

gremlinStringConstants_shortestPathStringConstants_includeEdges
    : shortestPathStringConstant DOT 'includeEdges'
    ;

gremlinStringConstants_withOptionsStringConstants_tokens
    : withOptionsStringConstant DOT 'tokens'
    ;

gremlinStringConstants_withOptionsStringConstants_none
    : withOptionsStringConstant DOT 'none'
    ;

gremlinStringConstants_withOptionsStringConstants_ids
    : withOptionsStringConstant DOT 'ids'
    ;

gremlinStringConstants_withOptionsStringConstants_labels
    : withOptionsStringConstant DOT 'labels'
    ;

gremlinStringConstants_withOptionsStringConstants_keys
    : withOptionsStringConstant DOT 'keys'
    ;

gremlinStringConstants_withOptionsStringConstants_values
    : withOptionsStringConstant DOT 'values'
    ;

gremlinStringConstants_withOptionsStringConstants_all
    : withOptionsStringConstant DOT 'all'
    ;

gremlinStringConstants_withOptionsStringConstants_indexer
    : withOptionsStringConstant DOT 'indexer'
    ;

gremlinStringConstants_withOptionsStringConstants_list
    : withOptionsStringConstant DOT 'list'
    ;

gremlinStringConstants_withOptionsStringConstants_map
    : withOptionsStringConstant DOT 'map'
    ;

gremlinStringConstants_ioOptionsStringConstants_reader
    : ioOptionsStringConstant DOT 'reader'
    ;

gremlinStringConstants_ioOptionsStringConstants_writer
    : ioOptionsStringConstant DOT 'writer'
    ;

gremlinStringConstants_ioOptionsStringConstants_gryo
    : ioOptionsStringConstant DOT 'gryo'
    ;

gremlinStringConstants_ioOptionsStringConstants_graphson
    : ioOptionsStringConstant DOT 'graphson'
    ;

gremlinStringConstants_ioOptionsStringConstants_graphml
    : ioOptionsStringConstant DOT 'graphml'
    ;

connectedComponentStringConstant
    : 'ConnectedComponent'
    ;

pageRankStringConstant
    : 'PageRank'
    ;

peerPressureStringConstant
    : 'PeerPressure'
    ;

shortestPathStringConstant
    : 'ShortestPath'
    ;

withOptionsStringConstant
    : 'WithOptions'
    ;

ioOptionsStringConstant
    : 'IO'
    ;

traversalStrategyList
    : traversalStrategyExpr?
    ;

traversalStrategyExpr
    : traversalStrategy (COMMA traversalStrategy)*
    ;

nestedTraversalList
    : nestedTraversalExpr?
    ;

nestedTraversalExpr
    : nestedTraversal (COMMA nestedTraversal)*
    ;

genericLiteralList
    : genericLiteralExpr?
    ;

genericLiteralExpr
    : genericLiteral (COMMA genericLiteral)*
    ;

genericLiteralRange
    : integerLiteral DOT DOT integerLiteral
    | stringLiteral DOT DOT stringLiteral
    ;

genericLiteralCollection
    : LBRACK (genericLiteral (COMMA genericLiteral)*)? RBRACK
    ;

stringLiteralList
    : stringLiteralExpr?
    | LBRACK stringLiteralExpr? RBRACK
    ;

stringLiteralExpr
    : stringLiteral (COMMA stringLiteral)*
    ;

genericLiteral
	: integerLiteral
	| floatLiteral
	| booleanLiteral
	| stringLiteral
	| dateLiteral
	| nullLiteral
	// Allow the generic literal to match specific gremlin tokens also
	| traversalToken
	| traversalCardinality
	| traversalDirection
	| traversalOptionParent
	| genericLiteralCollection
	| genericLiteralRange
	| nestedTraversal
	| terminatedTraversal
	| genericLiteralMap
	;

genericLiteralMap
  : LBRACK (genericLiteral)? COLON (genericLiteral)? (COMMA (genericLiteral)? COLON (genericLiteral)?)* RBRACK
  ;

integerLiteral
    : IntegerLiteral
    ;

floatLiteral
    : FloatingPointLiteral
    ;

booleanLiteral
    : BooleanLiteral
    ;

stringLiteral
    : NonEmptyStringLiteral
    | EmptyStringLiteral
    | NullLiteral
    | gremlinStringConstants
    ;

dateLiteral
    : 'datetime' LPAREN stringLiteral RPAREN
    ;

nullLiteral
    : NullLiteral
    ;

/*********************************************
    LEXER RULES
**********************************************/

// Lexer rules
// These rules are extracted from Java ANTLRv4 Grammar.
// Source: https://github.com/antlr/grammars-v4/blob/master/java8/Java8.g4

// §3.9 Keywords

NEW : 'new';

// Integer Literals

IntegerLiteral
	:	Sign? DecimalIntegerLiteral
	|	Sign? HexIntegerLiteral
	|	Sign? OctalIntegerLiteral
	;

fragment
DecimalIntegerLiteral
	:	DecimalNumeral IntegerTypeSuffix?
	;

fragment
HexIntegerLiteral
	:	HexNumeral IntegerTypeSuffix?
	;

fragment
OctalIntegerLiteral
	:	OctalNumeral IntegerTypeSuffix?
	;

fragment
IntegerTypeSuffix
	:	[lL]
	;

fragment
DecimalNumeral
	:	'0'
	|	NonZeroDigit (Digits? | Underscores Digits)
	;

fragment
Digits
	:	Digit (DigitsAndUnderscores? Digit)?
	;

fragment
Digit
	:	'0'
	|	NonZeroDigit
	;

fragment
NonZeroDigit
	:	[1-9]
	;

fragment
DigitsAndUnderscores
	:	DigitOrUnderscore+
	;

fragment
DigitOrUnderscore
	:	Digit
	|	'_'
	;

fragment
Underscores
	:	'_'+
	;

fragment
HexNumeral
	:	'0' [xX] HexDigits
	;

fragment
HexDigits
	:	HexDigit (HexDigitsAndUnderscores? HexDigit)?
	;

fragment
HexDigit
	:	[0-9a-fA-F]
	;

fragment
HexDigitsAndUnderscores
	:	HexDigitOrUnderscore+
	;

fragment
HexDigitOrUnderscore
	:	HexDigit
	|	'_'
	;

fragment
OctalNumeral
	:	'0' Underscores? OctalDigits
	;

fragment
OctalDigits
	:	OctalDigit (OctalDigitsAndUnderscores? OctalDigit)?
	;

fragment
OctalDigit
	:	[0-7]
	;

fragment
OctalDigitsAndUnderscores
	:	OctalDigitOrUnderscore+
	;

fragment
OctalDigitOrUnderscore
	:	OctalDigit
	|	'_'
	;

// Floating-Point Literals

FloatingPointLiteral
	:	Sign? DecimalFloatingPointLiteral
	;

fragment
DecimalFloatingPointLiteral
    :   Digits ('.' Digits ExponentPart? | ExponentPart) FloatTypeSuffix?
	|	Digits FloatTypeSuffix
	;

fragment
ExponentPart
	:	ExponentIndicator SignedInteger
	;

fragment
ExponentIndicator
	:	[eE]
	;

fragment
SignedInteger
	:	Sign? Digits
	;

fragment
Sign
	:	[+-]
	;

fragment
FloatTypeSuffix
	:	[fFdD]
	;

// Boolean Literals

BooleanLiteral
	:	'true'
	|	'false'
	;

// Null Literal

NullLiteral
	:	'null'
	;

// String Literals

// String literal is customized since Java only allows double quoted strings where Groovy supports single quoted
// literals also. A side effect of this is ANTLR will not be able to parse single character string literals with
// single quoted so we instead remove char literal altogether and only have string literal in lexer tokens.
NonEmptyStringLiteral
	:   '"' DoubleQuotedStringCharacters '"'
	|   '\'' SingleQuotedStringCharacters '\''
	;

// We define NonEmptyStringLiteral and EmptyStringLiteral separately so that we can unambiguously handle empty queries
EmptyStringLiteral
	:   '""'
	|   '\'\''
	;

fragment
DoubleQuotedStringCharacters
	:	DoubleQuotedStringCharacter+
	;

fragment
DoubleQuotedStringCharacter
	:	~('"' | '\\')
	|   JoinLineEscape
	|	EscapeSequence
	;

fragment
SingleQuotedStringCharacters
	:	SingleQuotedStringCharacter+
	;

fragment
SingleQuotedStringCharacter
	:	~('\'' | '\\')
	|   JoinLineEscape
	|	EscapeSequence
	;

// Escape Sequences for Character and String Literals
fragment JoinLineEscape
    : '\\' '\r'? '\n'
    ;

fragment
EscapeSequence
	:	'\\' [btnfr"'\\]
	|	OctalEscape
    |   UnicodeEscape // This is not in the spec but prevents having to preprocess the input
	;

fragment
OctalEscape
	:	'\\' OctalDigit
	|	'\\' OctalDigit OctalDigit
	|	'\\' ZeroToThree OctalDigit OctalDigit
	;

fragment
ZeroToThree
	:	[0-3]
	;

// This is not in the spec but prevents having to preprocess the input
fragment
UnicodeEscape
    :   '\\' 'u'+  HexDigit HexDigit HexDigit HexDigit
    ;

// Separators

LPAREN : '(';
RPAREN : ')';
LBRACE : '{';
RBRACE : '}';
LBRACK : '[';
RBRACK : ']';
SEMI : ';';
COMMA : ',';
DOT : '.';
COLON : ':';

TRAVERSAL_ROOT:     'g';
ANON_TRAVERSAL_ROOT:     '__';

// Trim whitespace and comments if present

WS  :  [ \t\r\n\u000C]+ -> skip
    ;

LINE_COMMENT
    :   '//' ~[\r\n]* -> skip
    ;
