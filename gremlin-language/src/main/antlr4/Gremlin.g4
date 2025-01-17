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
    : 'tx' LPAREN RPAREN DOT 'begin' LPAREN RPAREN
    | 'tx' LPAREN RPAREN DOT 'commit' LPAREN RPAREN
    | 'tx' LPAREN RPAREN DOT 'rollback' LPAREN RPAREN
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
    | traversalSourceSelfMethod_withoutStrategies
    | traversalSourceSelfMethod_with
    ;

traversalSourceSelfMethod_withBulk
    : 'withBulk' LPAREN booleanArgument RPAREN
    ;

traversalSourceSelfMethod_withPath
    : 'withPath' LPAREN RPAREN
    ;

traversalSourceSelfMethod_withSack
    : 'withSack' LPAREN genericLiteralArgument RPAREN
    | 'withSack' LPAREN genericLiteralArgument COMMA traversalBiFunction RPAREN
    ;

traversalSourceSelfMethod_withSideEffect
    : 'withSideEffect' LPAREN stringLiteral COMMA genericLiteralArgument RPAREN
    | 'withSideEffect' LPAREN stringLiteral COMMA genericLiteralArgument COMMA traversalBiFunction RPAREN
    ;

traversalSourceSelfMethod_withStrategies
    : 'withStrategies' LPAREN traversalStrategy (COMMA traversalStrategyList)? RPAREN
    ;

traversalSourceSelfMethod_withoutStrategies
    : 'withoutStrategies' LPAREN classType (COMMA classTypeList)? RPAREN
    ;

traversalSourceSelfMethod_with
    : 'with' LPAREN stringLiteral RPAREN
    | 'with' LPAREN stringLiteral COMMA genericLiteralArgument RPAREN
    ;

traversalSourceSpawnMethod
    : traversalSourceSpawnMethod_addE
    | traversalSourceSpawnMethod_addV
    | traversalSourceSpawnMethod_E
    | traversalSourceSpawnMethod_V
    | traversalSourceSpawnMethod_mergeE
    | traversalSourceSpawnMethod_mergeV
    | traversalSourceSpawnMethod_inject
    | traversalSourceSpawnMethod_io
    | traversalSourceSpawnMethod_call
    | traversalSourceSpawnMethod_union
    ;

traversalSourceSpawnMethod_addE
    : 'addE' LPAREN stringArgument RPAREN
    | 'addE' LPAREN nestedTraversal RPAREN
    ;

traversalSourceSpawnMethod_addV
    : 'addV' LPAREN RPAREN
    | 'addV' LPAREN stringArgument RPAREN
    | 'addV' LPAREN nestedTraversal RPAREN
    ;

traversalSourceSpawnMethod_E
    : 'E' LPAREN genericLiteralVarargs RPAREN
    ;

traversalSourceSpawnMethod_V
    : 'V' LPAREN genericLiteralVarargs RPAREN
    ;

traversalSourceSpawnMethod_inject
    : 'inject' LPAREN genericLiteralVarargs RPAREN
    ;

traversalSourceSpawnMethod_io
    : 'io' LPAREN stringLiteral RPAREN
    ;

traversalSourceSpawnMethod_mergeV
    : 'mergeV' LPAREN genericLiteralMapNullableArgument RPAREN #traversalSourceSpawnMethod_mergeV_Map
    | 'mergeV' LPAREN nestedTraversal RPAREN #traversalSourceSpawnMethod_mergeV_Traversal
    ;

traversalSourceSpawnMethod_mergeE
    : 'mergeE' LPAREN genericLiteralMapNullableArgument RPAREN #traversalSourceSpawnMethod_mergeE_Map
    | 'mergeE' LPAREN nestedTraversal RPAREN #traversalSourceSpawnMethod_mergeE_Traversal
    ;

traversalSourceSpawnMethod_call
    : 'call' LPAREN RPAREN #traversalSourceSpawnMethod_call_empty
    | 'call' LPAREN stringLiteral RPAREN #traversalSourceSpawnMethod_call_string
    | 'call' LPAREN stringLiteral COMMA genericLiteralMapArgument RPAREN #traversalSourceSpawnMethod_call_string_map
    | 'call' LPAREN stringLiteral COMMA nestedTraversal RPAREN #traversalSourceSpawnMethod_call_string_traversal
    | 'call' LPAREN stringLiteral COMMA genericLiteralMapArgument COMMA nestedTraversal RPAREN #traversalSourceSpawnMethod_call_string_map_traversal
    ;

traversalSourceSpawnMethod_union
    : 'union' LPAREN nestedTraversalList RPAREN
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
    : chainedTraversal
    | ANON_TRAVERSAL_ROOT DOT chainedTraversal
    ;

terminatedTraversal
    : rootTraversal DOT traversalTerminalMethod
    ;

traversalMethod
    : traversalMethod_V
    | traversalMethod_E
    | traversalMethod_addE
    | traversalMethod_addV
    | traversalMethod_mergeE
    | traversalMethod_mergeV
    | traversalMethod_aggregate
    | traversalMethod_all
    | traversalMethod_and
    | traversalMethod_any
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
    | traversalMethod_conjoin
    | traversalMethod_connectedComponent
    | traversalMethod_constant
    | traversalMethod_count
    | traversalMethod_cyclicPath
    | traversalMethod_dedup
    | traversalMethod_difference
    | traversalMethod_disjunct
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
    | traversalMethod_intersect
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
    | traversalMethod_none
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
    | traversalMethod_combine
    | traversalMethod_product
    | traversalMethod_merge
    | traversalMethod_shortestPath
    | traversalMethod_sideEffect
    | traversalMethod_simplePath
    | traversalMethod_skip
    | traversalMethod_store
    | traversalMethod_subgraph
    | traversalMethod_sum
    | traversalMethod_tail
    | traversalMethod_fail
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
    | traversalMethod_element
    | traversalMethod_call
    | traversalMethod_concat
    | traversalMethod_asString
    | traversalMethod_format
    | traversalMethod_toUpper
    | traversalMethod_toLower
    | traversalMethod_length
    | traversalMethod_trim
    | traversalMethod_lTrim
    | traversalMethod_rTrim
    | traversalMethod_reverse
    | traversalMethod_replace
    | traversalMethod_split
    | traversalMethod_substring
    | traversalMethod_asDate
    | traversalMethod_dateAdd
    | traversalMethod_dateDiff
    ;

traversalMethod_V
    : 'V' LPAREN genericLiteralVarargs RPAREN
    ;

traversalMethod_E
    : 'E' LPAREN genericLiteralVarargs RPAREN
    ;

traversalMethod_addE
    : 'addE' LPAREN stringArgument RPAREN #traversalMethod_addE_String
    | 'addE' LPAREN nestedTraversal RPAREN #traversalMethod_addE_Traversal
    ;

traversalMethod_addV
    : 'addV' LPAREN RPAREN #traversalMethod_addV_Empty
    | 'addV' LPAREN stringArgument RPAREN #traversalMethod_addV_String
    | 'addV' LPAREN nestedTraversal RPAREN #traversalMethod_addV_Traversal
    ;

traversalMethod_mergeV
    : 'mergeV' LPAREN RPAREN #traversalMethod_mergeV_empty
    | 'mergeV' LPAREN genericLiteralMapNullableArgument RPAREN #traversalMethod_mergeV_Map
    | 'mergeV' LPAREN nestedTraversal RPAREN #traversalMethod_mergeV_Traversal
    ;

traversalMethod_mergeE
    : 'mergeE' LPAREN RPAREN #traversalMethod_mergeE_empty
    | 'mergeE' LPAREN genericLiteralMapNullableArgument RPAREN #traversalMethod_mergeE_Map
    | 'mergeE' LPAREN nestedTraversal RPAREN #traversalMethod_mergeE_Traversal
    ;

traversalMethod_aggregate
    : 'aggregate' LPAREN traversalScope COMMA stringLiteral RPAREN #traversalMethod_aggregate_Scope_String
    | 'aggregate' LPAREN stringLiteral RPAREN #traversalMethod_aggregate_String
    ;

traversalMethod_all
    : 'all' LPAREN traversalPredicate RPAREN #traversalMethod_all_P
    ;

traversalMethod_and
    : 'and' LPAREN nestedTraversalList RPAREN
    ;

traversalMethod_any
    : 'any' LPAREN traversalPredicate RPAREN #traversalMethod_any_P
    ;

traversalMethod_as
    : 'as' LPAREN stringLiteral (COMMA stringLiteralVarargsLiterals)? RPAREN
    ;

traversalMethod_barrier
    : 'barrier' LPAREN traversalSackMethod RPAREN #traversalMethod_barrier_Consumer
    | 'barrier' LPAREN RPAREN #traversalMethod_barrier_Empty
    | 'barrier' LPAREN integerLiteral RPAREN #traversalMethod_barrier_int
    ;

traversalMethod_both
    : 'both' LPAREN stringLiteralVarargs RPAREN
    ;

traversalMethod_bothE
    : 'bothE' LPAREN stringLiteralVarargs RPAREN
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
    : 'cap' LPAREN stringLiteral (COMMA stringLiteralVarargsLiterals)? RPAREN
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
    : 'coin' LPAREN floatArgument RPAREN
    ;

traversalMethod_combine
    : 'combine' LPAREN genericLiteralArgument RPAREN #traversalMethod_combine_Object
    ;

traversalMethod_connectedComponent
    : 'connectedComponent' LPAREN RPAREN
    ;

traversalMethod_constant
    : 'constant' LPAREN genericLiteralArgument RPAREN
    ;

traversalMethod_count
    : 'count' LPAREN RPAREN #traversalMethod_count_Empty
    | 'count' LPAREN traversalScope RPAREN #traversalMethod_count_Scope
    ;

traversalMethod_cyclicPath
    : 'cyclicPath' LPAREN RPAREN
    ;

traversalMethod_dedup
    : 'dedup' LPAREN traversalScope (COMMA stringLiteralVarargsLiterals)? RPAREN #traversalMethod_dedup_Scope_String
    | 'dedup' LPAREN stringLiteralVarargsLiterals RPAREN #traversalMethod_dedup_String
    ;

traversalMethod_difference
    : 'difference' LPAREN genericLiteralArgument RPAREN #traversalMethod_difference_Object
    ;

traversalMethod_disjunct
    : 'disjunct' LPAREN genericLiteralArgument RPAREN #traversalMethod_disjunct_Object
    ;

traversalMethod_drop
    : 'drop' LPAREN RPAREN
    ;

traversalMethod_elementMap
    : 'elementMap' LPAREN stringLiteralVarargsLiterals RPAREN
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
    | 'fold' LPAREN genericLiteralArgument COMMA traversalBiFunction RPAREN #traversalMethod_fold_Object_BiFunction
    ;

traversalMethod_from
    : 'from' LPAREN stringLiteral RPAREN #traversalMethod_from_String
    | 'from' LPAREN structureVertexArgument RPAREN #traversalMethod_from_Vertex
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
    : 'has' LPAREN stringNullableLiteral RPAREN #traversalMethod_has_String
    | 'has' LPAREN stringNullableLiteral COMMA genericLiteralArgument RPAREN #traversalMethod_has_String_Object
    | 'has' LPAREN stringNullableLiteral COMMA traversalPredicate RPAREN #traversalMethod_has_String_P
    | 'has' LPAREN stringNullableArgument COMMA stringNullableLiteral COMMA genericLiteralArgument RPAREN #traversalMethod_has_String_String_Object
    | 'has' LPAREN stringNullableArgument COMMA stringNullableLiteral COMMA traversalPredicate RPAREN #traversalMethod_has_String_String_P
    | 'has' LPAREN stringNullableLiteral COMMA nestedTraversal RPAREN #traversalMethod_has_String_Traversal
    | 'has' LPAREN traversalToken COMMA genericLiteralArgument RPAREN #traversalMethod_has_T_Object
    | 'has' LPAREN traversalToken COMMA traversalPredicate RPAREN #traversalMethod_has_T_P
    | 'has' LPAREN traversalToken COMMA nestedTraversal RPAREN #traversalMethod_has_T_Traversal
    ;

traversalMethod_hasId
    : 'hasId' LPAREN genericLiteralArgument (COMMA genericLiteralVarargs)? RPAREN #traversalMethod_hasId_Object_Object
    | 'hasId' LPAREN traversalPredicate RPAREN #traversalMethod_hasId_P
    ;

traversalMethod_hasKey
    : 'hasKey' LPAREN traversalPredicate RPAREN #traversalMethod_hasKey_P
    | 'hasKey' LPAREN stringNullableLiteral (COMMA stringLiteralVarargsLiterals)? RPAREN #traversalMethod_hasKey_String_String
    ;

traversalMethod_hasLabel
    : 'hasLabel' LPAREN traversalPredicate RPAREN #traversalMethod_hasLabel_P
    | 'hasLabel' LPAREN stringNullableArgument (COMMA stringLiteralVarargs)? RPAREN #traversalMethod_hasLabel_String_String
    ;

traversalMethod_hasNot
    : 'hasNot' LPAREN stringNullableLiteral RPAREN
    ;

traversalMethod_hasValue
    : 'hasValue' LPAREN genericLiteralArgument (COMMA genericLiteralVarargs)? RPAREN #traversalMethod_hasValue_Object_Object
    | 'hasValue' LPAREN traversalPredicate RPAREN #traversalMethod_hasValue_P
    ;

traversalMethod_id
    : 'id' LPAREN RPAREN
    ;

traversalMethod_identity
    : 'identity' LPAREN RPAREN
    ;

traversalMethod_in
    : 'in' LPAREN stringLiteralVarargs RPAREN
    ;

traversalMethod_inE
    : 'inE' LPAREN stringLiteralVarargs RPAREN
    ;

traversalMethod_intersect
    : 'intersect' LPAREN genericLiteralArgument RPAREN #traversalMethod_intersect_Object
    ;

traversalMethod_inV
    : 'inV' LPAREN RPAREN
    ;

traversalMethod_index
    : 'index' LPAREN RPAREN
    ;

traversalMethod_inject
    : 'inject' LPAREN genericLiteralVarargs RPAREN
    ;

traversalMethod_is
    : 'is' LPAREN genericLiteralArgument RPAREN #traversalMethod_is_Object
    | 'is' LPAREN traversalPredicate RPAREN #traversalMethod_is_P
    ;

traversalMethod_conjoin
    : 'conjoin' LPAREN stringArgument RPAREN #traversalMethod_conjoin_String
    ;

traversalMethod_key
    : 'key' LPAREN RPAREN
    ;

traversalMethod_label
    : 'label' LPAREN RPAREN
    ;

traversalMethod_limit
    : 'limit' LPAREN traversalScope COMMA integerArgument RPAREN #traversalMethod_limit_Scope_long
    | 'limit' LPAREN integerArgument RPAREN #traversalMethod_limit_long
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

traversalMethod_merge
    : 'merge' LPAREN genericLiteralArgument RPAREN #traversalMethod_merge_Object
    ;

traversalMethod_min
    : 'min' LPAREN RPAREN #traversalMethod_min_Empty
    | 'min' LPAREN traversalScope RPAREN #traversalMethod_min_Scope
    ;

traversalMethod_none
    : 'none' LPAREN traversalPredicate RPAREN #traversalMethod_none_P
    ;

traversalMethod_not
    : 'not' LPAREN nestedTraversal RPAREN
    ;

traversalMethod_option
    : 'option' LPAREN traversalPredicate COMMA nestedTraversal RPAREN #traversalMethod_option_Predicate_Traversal
    | 'option' LPAREN traversalMerge COMMA genericLiteralMapNullableArgument RPAREN #traversalMethod_option_Merge_Map
    | 'option' LPAREN traversalMerge COMMA nullableGenericLiteralMap COMMA traversalCardinality RPAREN #traversalMethod_option_Merge_Map_Cardinality
    | 'option' LPAREN traversalMerge COMMA nestedTraversal RPAREN #traversalMethod_option_Merge_Traversal
    | 'option' LPAREN genericLiteralArgument COMMA nestedTraversal RPAREN #traversalMethod_option_Object_Traversal
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
    : 'out' LPAREN stringLiteralVarargs RPAREN
    ;

traversalMethod_outE
    : 'outE' LPAREN stringLiteralVarargs RPAREN
    ;

traversalMethod_outV
    : 'outV' LPAREN RPAREN
    ;

traversalMethod_pageRank
    : 'pageRank' LPAREN RPAREN #traversalMethod_pageRank_Empty
    | 'pageRank' LPAREN floatArgument RPAREN #traversalMethod_pageRank_double
    ;

traversalMethod_path
    : 'path' LPAREN RPAREN
    ;

traversalMethod_peerPressure
    : 'peerPressure' LPAREN RPAREN
    ;

traversalMethod_product
    : 'product' LPAREN genericLiteralArgument RPAREN #traversalMethod_product_Object
    ;

traversalMethod_profile
    : 'profile' LPAREN RPAREN #traversalMethod_profile_Empty
    | 'profile' LPAREN stringLiteral RPAREN #traversalMethod_profile_String
    ;

traversalMethod_project
    : 'project' LPAREN stringLiteral (COMMA stringLiteralVarargsLiterals)? RPAREN
    ;

traversalMethod_properties
    : 'properties' LPAREN stringLiteralVarargsLiterals RPAREN
    ;

traversalMethod_property
    : 'property' LPAREN traversalCardinality COMMA genericLiteralArgument COMMA genericLiteralArgument (COMMA genericLiteralVarargs)? RPAREN #traversalMethod_property_Cardinality_Object_Object_Object
    | 'property' LPAREN traversalCardinality COMMA genericLiteralMapNullableArgument RPAREN # traversalMethod_property_Cardinality_Object
    | 'property' LPAREN genericLiteralArgument COMMA genericLiteralArgument (COMMA genericLiteralVarargs)? RPAREN #traversalMethod_property_Object_Object_Object
    | 'property' LPAREN genericLiteralMapNullableArgument RPAREN # traversalMethod_property_Object
    ;

traversalMethod_propertyMap
    : 'propertyMap' LPAREN stringLiteralVarargsLiterals RPAREN
    ;

traversalMethod_range
    : 'range' LPAREN traversalScope COMMA integerArgument COMMA integerArgument RPAREN #traversalMethod_range_Scope_long_long
    | 'range' LPAREN integerArgument COMMA integerArgument RPAREN #traversalMethod_range_long_long
    ;

traversalMethod_read
    : 'read' LPAREN RPAREN
    ;

traversalMethod_repeat
    : 'repeat' LPAREN stringLiteral COMMA nestedTraversal RPAREN #traversalMethod_repeat_String_Traversal
    | 'repeat' LPAREN nestedTraversal RPAREN #traversalMethod_repeat_Traversal
    ;

traversalMethod_reverse
    : 'reverse' LPAREN RPAREN #traversalMethod_reverse_Empty
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
    | 'select' LPAREN traversalPop COMMA stringLiteral COMMA stringLiteral (COMMA stringLiteralVarargsLiterals)? RPAREN #traversalMethod_select_Pop_String_String_String
    | 'select' LPAREN traversalPop COMMA nestedTraversal RPAREN #traversalMethod_select_Pop_Traversal
    | 'select' LPAREN stringLiteral RPAREN #traversalMethod_select_String
    | 'select' LPAREN stringLiteral COMMA stringLiteral (COMMA stringLiteralVarargsLiterals)? RPAREN #traversalMethod_select_String_String_String
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
    : 'skip' LPAREN traversalScope COMMA integerArgument RPAREN #traversalMethod_skip_Scope_long
    | 'skip' LPAREN integerArgument RPAREN #traversalMethod_skip_long
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
    | 'tail' LPAREN traversalScope COMMA integerArgument RPAREN #traversalMethod_tail_Scope_long
    | 'tail' LPAREN integerArgument RPAREN #traversalMethod_tail_long
    ;

traversalMethod_fail
    : 'fail' LPAREN RPAREN #traversalMethod_fail_Empty
    | 'fail' LPAREN stringLiteral RPAREN #traversalMethod_fail_String
    ;

traversalMethod_timeLimit
    : 'timeLimit' LPAREN integerLiteral RPAREN
    ;

traversalMethod_times
    : 'times' LPAREN integerLiteral RPAREN
    ;

traversalMethod_to
    : 'to' LPAREN traversalDirection (COMMA stringLiteralVarargs)? RPAREN #traversalMethod_to_Direction_String
    | 'to' LPAREN stringLiteral RPAREN #traversalMethod_to_String
    | 'to' LPAREN structureVertexArgument RPAREN #traversalMethod_to_Vertex
    | 'to' LPAREN nestedTraversal RPAREN #traversalMethod_to_Traversal
    ;

traversalMethod_toE
    : 'toE' LPAREN traversalDirection (COMMA stringLiteralVarargs)? RPAREN
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
    : 'valueMap' LPAREN stringLiteralVarargsLiterals RPAREN #traversalMethod_valueMap_String
    | 'valueMap' LPAREN booleanLiteral (COMMA stringLiteralVarargsLiterals)? RPAREN #traversalMethod_valueMap_boolean_String
    ;

traversalMethod_values
    : 'values' LPAREN stringLiteralVarargsLiterals RPAREN
    ;

traversalMethod_where
    : 'where' LPAREN traversalPredicate RPAREN #traversalMethod_where_P
    | 'where' LPAREN stringLiteral COMMA traversalPredicate RPAREN #traversalMethod_where_String_P
    | 'where' LPAREN nestedTraversal RPAREN #traversalMethod_where_Traversal
    ;

traversalMethod_with
    : 'with' LPAREN (withOptionKeys | stringLiteral) RPAREN #traversalMethod_with_String
    | 'with' LPAREN (withOptionKeys | stringLiteral) COMMA (withOptionsValues | ioOptionsValues | genericLiteralArgument) RPAREN #traversalMethod_with_String_Object
    ;

traversalMethod_write
    : 'write' LPAREN RPAREN
    ;

traversalMethod_element
    : 'element' LPAREN RPAREN
    ;

traversalMethod_call
    : 'call' LPAREN stringLiteral RPAREN #traversalMethod_call_string
    | 'call' LPAREN stringLiteral COMMA genericLiteralMapArgument RPAREN #traversalMethod_call_string_map
    | 'call' LPAREN stringLiteral COMMA nestedTraversal RPAREN #traversalMethod_call_string_traversal
    | 'call' LPAREN stringLiteral COMMA genericLiteralMapArgument COMMA nestedTraversal RPAREN #traversalMethod_call_string_map_traversal
    ;

traversalMethod_concat
    : 'concat' LPAREN nestedTraversal (COMMA nestedTraversalList)? RPAREN #traversalMethod_concat_Traversal_Traversal
    | 'concat' LPAREN stringLiteralVarargsLiterals RPAREN #traversalMethod_concat_String
    ;

traversalMethod_asString
    : 'asString' LPAREN RPAREN #traversalMethod_asString_Empty
    | 'asString' LPAREN traversalScope RPAREN #traversalMethod_asString_Scope
    ;

traversalMethod_format
    : 'format' LPAREN stringLiteral RPAREN #traversalMethod_format_String
    ;

traversalMethod_toUpper
    : 'toUpper' LPAREN RPAREN #traversalMethod_toUpper_Empty
    | 'toUpper' LPAREN traversalScope RPAREN #traversalMethod_toUpper_Scope
    ;

traversalMethod_toLower
    : 'toLower' LPAREN RPAREN #traversalMethod_toLower_Empty
    | 'toLower' LPAREN traversalScope RPAREN #traversalMethod_toLower_Scope
    ;

traversalMethod_length
    : 'length' LPAREN RPAREN #traversalMethod_length_Empty
    | 'length' LPAREN traversalScope RPAREN #traversalMethod_length_Scope
    ;

traversalMethod_trim
    : 'trim' LPAREN RPAREN #traversalMethod_trim_Empty
    | 'trim' LPAREN traversalScope RPAREN #traversalMethod_trim_Scope
    ;

traversalMethod_lTrim
    : 'lTrim' LPAREN RPAREN #traversalMethod_lTrim_Empty
    | 'lTrim' LPAREN traversalScope RPAREN #traversalMethod_lTrim_Scope
    ;

traversalMethod_rTrim
    : 'rTrim' LPAREN RPAREN #traversalMethod_rTrim_Empty
    | 'rTrim' LPAREN traversalScope RPAREN #traversalMethod_rTrim_Scope
    ;

traversalMethod_replace
    : 'replace' LPAREN stringNullableLiteral COMMA stringNullableLiteral RPAREN #traversalMethod_replace_String_String
    | 'replace' LPAREN traversalScope COMMA stringNullableLiteral COMMA stringNullableLiteral RPAREN #traversalMethod_replace_Scope_String_String
    ;

traversalMethod_split
    : 'split' LPAREN stringNullableLiteral RPAREN #traversalMethod_split_String
    | 'split' LPAREN traversalScope COMMA stringNullableLiteral RPAREN #traversalMethod_split_Scope_String
    ;

traversalMethod_substring
    : 'substring' LPAREN integerLiteral RPAREN #traversalMethod_substring_int
    | 'substring' LPAREN traversalScope COMMA integerLiteral RPAREN #traversalMethod_substring_Scope_int
    | 'substring' LPAREN integerLiteral COMMA integerLiteral RPAREN #traversalMethod_substring_int_int
    | 'substring' LPAREN traversalScope COMMA integerLiteral COMMA integerLiteral RPAREN #traversalMethod_substring_Scope_int_int
    ;

traversalMethod_asDate
    : 'asDate' LPAREN RPAREN
    ;

traversalMethod_dateAdd
    : 'dateAdd' LPAREN traversalDT COMMA integerLiteral RPAREN
    ;

traversalMethod_dateDiff
    : 'dateDiff' LPAREN nestedTraversal RPAREN #traversalMethod_dateDiff_Traversal
    | 'dateDiff' LPAREN dateLiteral RPAREN #traversalMethod_dateDiff_Date
    ;

/*********************************************
    ARGUMENT AND TERMINAL RULES
**********************************************/

// There is syntax available in the construction of a ReferenceVertex, that allows the label to not be specified.
// That use case is related to OLAP when the StarGraph does not preserve the label of adjacent vertices or other
// fail fast scenarios in that processing model. It is not relevant to the grammar however when a user is creating
// the Vertex to be used in a Traversal and therefore both id and label are required.
structureVertex
    : NEW? ('Vertex'|'ReferenceVertex') LPAREN genericLiteralArgument COMMA stringArgument RPAREN
    ;

traversalStrategy
    : NEW? classType (LPAREN (configuration (COMMA configuration)*)? RPAREN)?
    ;

configuration
    : keyword COLON genericLiteralArgument
    | Identifier COLON genericLiteralArgument
    ;

traversalScope
    : 'local' | 'Scope.local'
    | 'global' | 'Scope.global'
    ;

traversalBarrier
    : 'normSack' | 'Barrier.normSack'
    ;

traversalToken
    : 'id' | 'T.id'
    | 'label' | 'T.label'
    | 'key' | 'T.key'
    | 'value' | 'T.value'
    ;

traversalMerge
    : 'onCreate' | 'Merge.onCreate'
    | 'onMatch' | 'Merge.onMatch'
    | 'outV' | 'Merge.outV'
    | 'inV' | 'Merge.inV'
    ;

traversalOrder
    : 'incr' | 'Order.incr'
    | 'decr' | 'Order.decr'
    | 'asc'  | 'Order.asc'
    | 'desc' | 'Order.desc'
    | 'shuffle' | 'Order.shuffle'
    ;

traversalDirection
    : 'IN' | 'Direction.IN' | 'Direction.from' | 'from'
    | 'OUT' | 'Direction.OUT' | 'Direction.to' | 'to'
    | 'BOTH' | 'Direction.BOTH'
    ;

traversalCardinality
    : 'Cardinality.single' LPAREN genericLiteral RPAREN
    | 'Cardinality.set' LPAREN genericLiteral RPAREN
    | 'Cardinality.list' LPAREN genericLiteral RPAREN
    | 'single' LPAREN genericLiteral RPAREN
    | 'set' LPAREN genericLiteral RPAREN
    | 'list' LPAREN genericLiteral RPAREN
    | 'single' | 'Cardinality.single'
    | 'set' | 'Cardinality.set'
    | 'list' | 'Cardinality.list'
    ;

traversalColumn
    : KEYS | 'Column.keys'
    | VALUES | 'Column.values'
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

traversalPick
    : 'any' | 'Pick.any'
    | 'none' | 'Pick.none'
    ;

traversalDT
    : 'second' | 'DT.second'
    | 'minute' | 'DT.minute'
    | 'hour' | 'DT.hour'
    | 'day' | 'DT.day'
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
    | traversalPredicate_regex
    | traversalPredicate_notRegex
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
    : traversalBarrier
    ;

traversalSelfMethod
    : traversalSelfMethod_discard
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
    : ('P.eq' | 'eq') LPAREN genericLiteralArgument RPAREN
    ;

traversalPredicate_neq
    : ('P.neq' | 'neq') LPAREN genericLiteralArgument RPAREN
    ;

traversalPredicate_lt
    : ('P.lt' | 'lt') LPAREN genericLiteralArgument RPAREN
    ;

traversalPredicate_lte
    : ('P.lte' | 'lte') LPAREN genericLiteralArgument RPAREN
    ;

traversalPredicate_gt
    : ('P.gt' | 'gt') LPAREN genericLiteralArgument RPAREN
    ;

traversalPredicate_gte
    : ('P.gte' | 'gte') LPAREN genericLiteralArgument RPAREN
    ;

traversalPredicate_inside
    : ('P.inside' | 'inside') LPAREN genericLiteralArgument COMMA genericLiteralArgument RPAREN
    ;

traversalPredicate_outside
    : ('P.outside' | 'outside') LPAREN genericLiteralArgument COMMA genericLiteralArgument RPAREN
    ;

traversalPredicate_between
    : ('P.between' | 'between') LPAREN genericLiteralArgument COMMA genericLiteralArgument RPAREN
    ;

traversalPredicate_within
    : ('P.within' | 'within') LPAREN RPAREN
    | ('P.within' | 'within') LPAREN genericLiteralListArgument RPAREN
    ;

traversalPredicate_without
    : ('P.without' | 'without') LPAREN RPAREN
    | ('P.without' | 'without') LPAREN genericLiteralListArgument RPAREN
    ;

traversalPredicate_not
    : ('P.not' | 'not') LPAREN traversalPredicate RPAREN
    ;

traversalPredicate_containing
    : ('TextP.containing' | 'containing') LPAREN stringArgument RPAREN
    ;

traversalPredicate_notContaining
    : ('TextP.notContaining' | 'notContaining') LPAREN stringArgument RPAREN
    ;

traversalPredicate_startingWith
    : ('TextP.startingWith' | 'startingWith') LPAREN stringArgument RPAREN
    ;

traversalPredicate_notStartingWith
    : ('TextP.notStartingWith' | 'notStartingWith') LPAREN stringArgument RPAREN
    ;

traversalPredicate_endingWith
    : ('TextP.endingWith' | 'endingWith') LPAREN stringArgument RPAREN
    ;

traversalPredicate_notEndingWith
    : ('TextP.notEndingWith' | 'notEndingWith') LPAREN stringArgument RPAREN
    ;

traversalPredicate_regex
    : ('TextP.regex' | 'regex') LPAREN stringArgument RPAREN
    ;

traversalPredicate_notRegex
    : ('TextP.notRegex' | 'notRegex') LPAREN stringArgument RPAREN
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

traversalSelfMethod_discard
    : 'discard' LPAREN RPAREN
    ;

// Gremlin specific lexer rules

withOptionKeys
    : shortestPathConstants
    | connectedComponentConstants
    | pageRankConstants
    | peerPressureConstants
    | ioOptionsKeys
    | withOptionsConstants_tokens
    | withOptionsConstants_indexer
    ;

connectedComponentConstants
    : connectedComponentConstants_component
    | connectedComponentConstants_edges
    | connectedComponentConstants_propertyName
    ;

pageRankConstants
    : pageRankConstants_edges
    | pageRankConstants_times
    | pageRankConstants_propertyName
    ;

peerPressureConstants
    : peerPressureConstants_edges
    | peerPressureConstants_times
    | peerPressureConstants_propertyName
    ;

shortestPathConstants
    : shortestPathConstants_target
    | shortestPathConstants_edges
    | shortestPathConstants_distance
    | shortestPathConstants_maxDistance
    | shortestPathConstants_includeEdges
    ;

withOptionsValues
    : withOptionsConstants_tokens
    | withOptionsConstants_none
    | withOptionsConstants_ids
    | withOptionsConstants_labels
    | withOptionsConstants_keys
    | withOptionsConstants_values
    | withOptionsConstants_all
    | withOptionsConstants_list
    | withOptionsConstants_map
    ;

ioOptionsKeys
    : ioOptionsConstants_reader
    | ioOptionsConstants_writer
    ;

ioOptionsValues
    : ioOptionsConstants_gryo
    | ioOptionsConstants_graphson
    | ioOptionsConstants_graphml
    ;

connectedComponentConstants_component
    : connectedComponentStringConstant DOT 'component'
    ;

connectedComponentConstants_edges
    : connectedComponentStringConstant DOT EDGES
    ;

connectedComponentConstants_propertyName
    : connectedComponentStringConstant DOT 'propertyName'
    ;

pageRankConstants_edges
    : pageRankStringConstant DOT EDGES
    ;

pageRankConstants_times
    : pageRankStringConstant DOT 'times'
    ;

pageRankConstants_propertyName
    : pageRankStringConstant DOT 'propertyName'
    ;

peerPressureConstants_edges
    : peerPressureStringConstant DOT EDGES
    ;

peerPressureConstants_times
    : peerPressureStringConstant DOT 'times'
    ;

peerPressureConstants_propertyName
    : peerPressureStringConstant DOT 'propertyName'
    ;

shortestPathConstants_target
    : shortestPathStringConstant DOT 'target'
    ;

shortestPathConstants_edges
    : shortestPathStringConstant DOT EDGES
    ;

shortestPathConstants_distance
    : shortestPathStringConstant DOT 'distance'
    ;

shortestPathConstants_maxDistance
    : shortestPathStringConstant DOT 'maxDistance'
    ;

shortestPathConstants_includeEdges
    : shortestPathStringConstant DOT 'includeEdges'
    ;

withOptionsConstants_tokens
    : withOptionsStringConstant DOT 'tokens'
    ;

withOptionsConstants_none
    : withOptionsStringConstant DOT 'none'
    ;

withOptionsConstants_ids
    : withOptionsStringConstant DOT 'ids'
    ;

withOptionsConstants_labels
    : withOptionsStringConstant DOT 'labels'
    ;

withOptionsConstants_keys
    : withOptionsStringConstant DOT 'keys'
    ;

withOptionsConstants_values
    : withOptionsStringConstant DOT 'values'
    ;

withOptionsConstants_all
    : withOptionsStringConstant DOT 'all'
    ;

withOptionsConstants_indexer
    : withOptionsStringConstant DOT 'indexer'
    ;

withOptionsConstants_list
    : withOptionsStringConstant DOT 'list'
    ;

withOptionsConstants_map
    : withOptionsStringConstant DOT 'map'
    ;

ioOptionsConstants_reader
    : ioOptionsStringConstant DOT 'reader'
    ;

ioOptionsConstants_writer
    : ioOptionsStringConstant DOT 'writer'
    ;

ioOptionsConstants_gryo
    : ioOptionsStringConstant DOT 'gryo'
    ;

ioOptionsConstants_graphson
    : ioOptionsStringConstant DOT 'graphson'
    ;

ioOptionsConstants_graphml
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

booleanArgument
    : booleanLiteral
    | variable
    ;

integerArgument
    : integerLiteral
    | variable
    ;

floatArgument
    : floatLiteral
    | variable
    ;

stringArgument
    : stringLiteral
    | variable
    ;

stringNullableArgument
    : stringNullableLiteral
    | variable
    ;

dateArgument
    : dateLiteral
    | variable
    ;

genericLiteralArgument
    : genericLiteral
    | variable
    ;

genericLiteralListArgument
    : genericLiteralList
    | variable
    ;

genericLiteralMapArgument
    : genericLiteralMap
    | variable
    ;

genericLiteralMapNullableArgument
    : genericLiteralMap
    | nullLiteral
    | variable
    ;

nullableGenericLiteralMap
    : genericLiteralMap
    | nullLiteral
    ;

structureVertexArgument
    : structureVertex
    | variable
    ;

traversalStrategyList
    : traversalStrategyExpr?
    ;

traversalStrategyExpr
    : traversalStrategy (COMMA traversalStrategy)*
    ;

classTypeList
    : classTypeExpr?
    ;

classTypeExpr
    : classType (COMMA classType)*
    ;

nestedTraversalList
    : nestedTraversalExpr?
    ;

nestedTraversalExpr
    : nestedTraversal (COMMA nestedTraversal)*
    ;

genericLiteralVarargs
    : (genericLiteralArgument (COMMA genericLiteralArgument)*)?
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

genericLiteralSet
    : LBRACE (genericLiteral (COMMA genericLiteral)*)? RBRACE
    ;

genericLiteralCollection
    : LBRACK (genericLiteral (COMMA genericLiteral)*)? RBRACK
    ;

stringLiteralVarargs
    : (stringNullableArgument (COMMA stringNullableArgument)*)?
    ;

stringLiteralVarargsLiterals
    : (stringNullableLiteral (COMMA stringNullableLiteral)*)?
    ;

stringLiteralList
    : stringLiteralExpr?
    | LBRACK stringLiteralExpr? RBRACK
    ;

stringLiteralExpr
    : stringNullableLiteral (COMMA stringNullableLiteral)*
    ;

genericLiteral
    : numericLiteral
    | booleanLiteral
    | stringLiteral
    | dateLiteral
    | nullLiteral
    | nanLiteral
    | infLiteral
    // Allow the generic literal to match specific gremlin tokens also
    | traversalToken
    | traversalCardinality
    | traversalDirection
    | traversalMerge
    | traversalPick
    | traversalDT
    | structureVertex
    | genericLiteralSet
    | genericLiteralCollection
    | genericLiteralRange
    | nestedTraversal
    | terminatedTraversal
    | genericLiteralMap
    ;

genericLiteralMap
    : LBRACK COLON RBRACK
    | LBRACK mapEntry (COMMA mapEntry)* RBRACK
    ;

// allow builds of Map that sorta make sense in the Gremlin context.
mapEntry
    : (LPAREN stringLiteral RPAREN | stringLiteral) COLON genericLiteral
    | (LPAREN numericLiteral RPAREN | numericLiteral) COLON genericLiteral
    | (LPAREN traversalToken RPAREN | traversalToken) COLON genericLiteral
    | (LPAREN traversalDirection RPAREN | traversalDirection) COLON genericLiteral
    | (LPAREN genericLiteralSet RPAREN | genericLiteralSet) COLON genericLiteral
    | (LPAREN genericLiteralCollection RPAREN | genericLiteralCollection) COLON genericLiteral
    | (LPAREN genericLiteralMap RPAREN | genericLiteralMap) COLON genericLiteral
    | keyword COLON genericLiteral
    | Identifier COLON genericLiteral
    ;

stringLiteral
    : EmptyStringLiteral
    | NonEmptyStringLiteral
    ;

stringNullableLiteral
    : EmptyStringLiteral
    | NonEmptyStringLiteral
    | NullLiteral
    ;

integerLiteral
    : IntegerLiteral
    ;

floatLiteral
    : FloatingPointLiteral
    ;

numericLiteral
    : integerLiteral
    | floatLiteral
    ;

booleanLiteral
    : BooleanLiteral
    ;

dateLiteral
    : 'datetime' LPAREN stringArgument RPAREN
    | 'datetime' LPAREN RPAREN
    ;

nullLiteral
    : NullLiteral
    ;

nanLiteral
    : NaNLiteral
    ;

infLiteral
    : SignedInfLiteral
    ;

classType
    : Identifier
    ;

variable
    : Identifier
    ;

// need to complete this list to fix https://issues.apache.org/jira/browse/TINKERPOP-3047 but this much proves the
// approach works and allows the TraversalStrategy work to be complete.
keyword
    : EDGES
    | KEYS
    | NEW
    | VALUES
    ;

/*********************************************
    LEXER RULES
**********************************************/

// Lexer rules
// These rules are extracted from Java ANTLRv4 Grammar.
// Source: https://github.com/antlr/grammars-v4/blob/master/java8/Java8.g4

// §3.9 Keywords

EDGES: 'edges';
KEYS: 'keys';
NEW : 'new';
VALUES: 'values';

// Integer Literals

IntegerLiteral
    : Sign? DecimalIntegerLiteral
    | Sign? HexIntegerLiteral
    | Sign? OctalIntegerLiteral
    ;

fragment
DecimalIntegerLiteral
    : DecimalNumeral IntegerTypeSuffix?
    ;

fragment
HexIntegerLiteral
    : HexNumeral IntegerTypeSuffix?
    ;

fragment
OctalIntegerLiteral
    : OctalNumeral IntegerTypeSuffix?
    ;

fragment
IntegerTypeSuffix
    : [bBsSnNiIlL]
    ;

fragment
DecimalNumeral
    : '0'
    | NonZeroDigit (Digits? | Underscores Digits)
    ;

fragment
Digits
    : Digit (DigitsAndUnderscores? Digit)?
    ;

fragment
Digit
    : '0'
    | NonZeroDigit
    ;

fragment
NonZeroDigit
    : [1-9]
    ;

fragment
DigitsAndUnderscores
    : DigitOrUnderscore+
    ;

fragment
DigitOrUnderscore
    : Digit
    | '_'
    ;

fragment
Underscores
    : '_'+
    ;

fragment
HexNumeral
    : '0' [xX] HexDigits
    ;

fragment
HexDigits
    : HexDigit (HexDigitsAndUnderscores? HexDigit)?
    ;

fragment
HexDigit
    : [0-9a-fA-F]
    ;

fragment
HexDigitsAndUnderscores
    : HexDigitOrUnderscore+
    ;

fragment
HexDigitOrUnderscore
    : HexDigit
    | '_'
    ;

fragment
OctalNumeral
    : '0' Underscores? OctalDigits
    ;

fragment
OctalDigits
    : OctalDigit (OctalDigitsAndUnderscores? OctalDigit)?
    ;

fragment
OctalDigit
    : [0-7]
    ;

fragment
OctalDigitsAndUnderscores
    : OctalDigitOrUnderscore+
    ;

fragment
OctalDigitOrUnderscore
    : OctalDigit
    | '_'
    ;

// Floating-Point Literals

FloatingPointLiteral
    : Sign? DecimalFloatingPointLiteral
    ;

fragment
DecimalFloatingPointLiteral
    : Digits ('.' Digits ExponentPart? | ExponentPart) FloatTypeSuffix?
    | Digits FloatTypeSuffix
    ;

fragment
ExponentPart
    : ExponentIndicator SignedInteger
    ;

fragment
ExponentIndicator
    : [eE]
    ;

fragment
SignedInteger
    : Sign? Digits
    ;

fragment
Sign
    : [+-]
    ;

fragment
FloatTypeSuffix
    : [fFdDmM]
    ;

// Boolean Literals

BooleanLiteral
    : 'true'
    | 'false'
    ;

// Null Literal

NullLiteral
    : 'null'
    ;

// NaN Literal

NaNLiteral
    : 'NaN'
    ;

// Inf Literal

SignedInfLiteral
    : Sign? InfLiteral
    ;

InfLiteral
    : 'Infinity'
    ;

// String Literals

// String literal is customized since Java only allows double quoted strings where Groovy supports single quoted
// literals also. A side effect of this is ANTLR will not be able to parse single character string literals with
// single quoted so we instead remove char literal altogether and only have string literal in lexer tokens.
NonEmptyStringLiteral
    : '"' DoubleQuotedStringCharacters '"'
    | '\'' SingleQuotedStringCharacters '\''
    ;

// We define NonEmptyStringLiteral and EmptyStringLiteral separately so that we can unambiguously handle empty queries
EmptyStringLiteral
    : '""'
    | '\'\''
    ;

fragment
DoubleQuotedStringCharacters
    : DoubleQuotedStringCharacter+
    ;

fragment
DoubleQuotedStringCharacter
    : ~('"' | '\\')
    | JoinLineEscape
    | EscapeSequence
    ;

fragment
SingleQuotedStringCharacters
    : SingleQuotedStringCharacter+
    ;

fragment
SingleQuotedStringCharacter
    : ~('\'' | '\\')
    | JoinLineEscape
    | EscapeSequence
    ;

// Escape Sequences for Character and String Literals
fragment JoinLineEscape
    : '\\' '\r'? '\n'
    ;

fragment
EscapeSequence
    : '\\' [btnfr"'\\]
    | OctalEscape
    | UnicodeEscape // This is not in the spec but prevents having to preprocess the input
    ;

fragment
OctalEscape
    : '\\' OctalDigit
    | '\\' OctalDigit OctalDigit
    | '\\' ZeroToThree OctalDigit OctalDigit
    ;

fragment
ZeroToThree
    : [0-3]
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

Identifier
    : IdentifierStart IdentifierPart*
    ;

// REFERENCE: https://github.com/antlr/grammars-v4/blob/master/java/java8/Java8Lexer.g4
fragment
IdentifierStart
    : [\u0024]
    | [\u0041-\u005A]
    | [\u005F]
    | [\u0061-\u007A]
    | [\u00A2-\u00A5]
    | [\u00AA]
    | [\u00B5]
    | [\u00BA]
    | [\u00C0-\u00D6]
    | [\u00D8-\u00F6]
    | [\u00F8-\u02C1]
    | [\u02C6-\u02D1]
    | [\u02E0-\u02E4]
    | [\u02EC]
    | [\u02EE]
    | [\u0370-\u0374]
    | [\u0376-\u0377]
    | [\u037A-\u037D]
    | [\u037F]
    | [\u0386]
    | [\u0388-\u038A]
    | [\u038C]
    | [\u038E-\u03A1]
    | [\u03A3-\u03F5]
    | [\u03F7-\u0481]
    | [\u048A-\u052F]
    | [\u0531-\u0556]
    | [\u0559]
    | [\u0561-\u0587]
    | [\u058F]
    | [\u05D0-\u05EA]
    | [\u05F0-\u05F2]
    | [\u060B]
    | [\u0620-\u064A]
    | [\u066E-\u066F]
    | [\u0671-\u06D3]
    | [\u06D5]
    | [\u06E5-\u06E6]
    | [\u06EE-\u06EF]
    | [\u06FA-\u06FC]
    | [\u06FF]
    | [\u0710]
    | [\u0712-\u072F]
    | [\u074D-\u07A5]
    | [\u07B1]
    | [\u07CA-\u07EA]
    | [\u07F4-\u07F5]
    | [\u07FA]
    | [\u0800-\u0815]
    | [\u081A]
    | [\u0824]
    | [\u0828]
    | [\u0840-\u0858]
    | [\u0860-\u086A]
    | [\u08A0-\u08B4]
    | [\u08B6-\u08BD]
    | [\u0904-\u0939]
    | [\u093D]
    | [\u0950]
    | [\u0958-\u0961]
    | [\u0971-\u0980]
    | [\u0985-\u098C]
    | [\u098F-\u0990]
    | [\u0993-\u09A8]
    | [\u09AA-\u09B0]
    | [\u09B2]
    | [\u09B6-\u09B9]
    | [\u09BD]
    | [\u09CE]
    | [\u09DC-\u09DD]
    | [\u09DF-\u09E1]
    | [\u09F0-\u09F3]
    | [\u09FB-\u09FC]
    | [\u0A05-\u0A0A]
    | [\u0A0F-\u0A10]
    | [\u0A13-\u0A28]
    | [\u0A2A-\u0A30]
    | [\u0A32-\u0A33]
    | [\u0A35-\u0A36]
    | [\u0A38-\u0A39]
    | [\u0A59-\u0A5C]
    | [\u0A5E]
    | [\u0A72-\u0A74]
    | [\u0A85-\u0A8D]
    | [\u0A8F-\u0A91]
    | [\u0A93-\u0AA8]
    | [\u0AAA-\u0AB0]
    | [\u0AB2-\u0AB3]
    | [\u0AB5-\u0AB9]
    | [\u0ABD]
    | [\u0AD0]
    | [\u0AE0-\u0AE1]
    | [\u0AF1]
    | [\u0AF9]
    | [\u0B05-\u0B0C]
    | [\u0B0F-\u0B10]
    | [\u0B13-\u0B28]
    | [\u0B2A-\u0B30]
    | [\u0B32-\u0B33]
    | [\u0B35-\u0B39]
    | [\u0B3D]
    | [\u0B5C-\u0B5D]
    | [\u0B5F-\u0B61]
    | [\u0B71]
    | [\u0B83]
    | [\u0B85-\u0B8A]
    | [\u0B8E-\u0B90]
    | [\u0B92-\u0B95]
    | [\u0B99-\u0B9A]
    | [\u0B9C]
    | [\u0B9E-\u0B9F]
    | [\u0BA3-\u0BA4]
    | [\u0BA8-\u0BAA]
    | [\u0BAE-\u0BB9]
    | [\u0BD0]
    | [\u0BF9]
    | [\u0C05-\u0C0C]
    | [\u0C0E-\u0C10]
    | [\u0C12-\u0C28]
    | [\u0C2A-\u0C39]
    | [\u0C3D]
    | [\u0C58-\u0C5A]
    | [\u0C60-\u0C61]
    | [\u0C80]
    | [\u0C85-\u0C8C]
    | [\u0C8E-\u0C90]
    | [\u0C92-\u0CA8]
    | [\u0CAA-\u0CB3]
    | [\u0CB5-\u0CB9]
    | [\u0CBD]
    | [\u0CDE]
    | [\u0CE0-\u0CE1]
    | [\u0CF1-\u0CF2]
    | [\u0D05-\u0D0C]
    | [\u0D0E-\u0D10]
    | [\u0D12-\u0D3A]
    | [\u0D3D]
    | [\u0D4E]
    | [\u0D54-\u0D56]
    | [\u0D5F-\u0D61]
    | [\u0D7A-\u0D7F]
    | [\u0D85-\u0D96]
    | [\u0D9A-\u0DB1]
    | [\u0DB3-\u0DBB]
    | [\u0DBD]
    | [\u0DC0-\u0DC6]
    | [\u0E01-\u0E30]
    | [\u0E32-\u0E33]
    | [\u0E3F-\u0E46]
    | [\u0E81-\u0E82]
    | [\u0E84]
    | [\u0E87-\u0E88]
    | [\u0E8A]
    | [\u0E8D]
    | [\u0E94-\u0E97]
    | [\u0E99-\u0E9F]
    | [\u0EA1-\u0EA3]
    | [\u0EA5]
    | [\u0EA7]
    | [\u0EAA-\u0EAB]
    | [\u0EAD-\u0EB0]
    | [\u0EB2-\u0EB3]
    | [\u0EBD]
    | [\u0EC0-\u0EC4]
    | [\u0EC6]
    | [\u0EDC-\u0EDF]
    | [\u0F00]
    | [\u0F40-\u0F47]
    | [\u0F49-\u0F6C]
    | [\u0F88-\u0F8C]
    | [\u1000-\u102A]
    | [\u103F]
    | [\u1050-\u1055]
    | [\u105A-\u105D]
    | [\u1061]
    | [\u1065-\u1066]
    | [\u106E-\u1070]
    | [\u1075-\u1081]
    | [\u108E]
    | [\u10A0-\u10C5]
    | [\u10C7]
    | [\u10CD]
    | [\u10D0-\u10FA]
    | [\u10FC-\u1248]
    | [\u124A-\u124D]
    | [\u1250-\u1256]
    | [\u1258]
    | [\u125A-\u125D]
    | [\u1260-\u1288]
    | [\u128A-\u128D]
    | [\u1290-\u12B0]
    | [\u12B2-\u12B5]
    | [\u12B8-\u12BE]
    | [\u12C0]
    | [\u12C2-\u12C5]
    | [\u12C8-\u12D6]
    | [\u12D8-\u1310]
    | [\u1312-\u1315]
    | [\u1318-\u135A]
    | [\u1380-\u138F]
    | [\u13A0-\u13F5]
    | [\u13F8-\u13FD]
    | [\u1401-\u166C]
    | [\u166F-\u167F]
    | [\u1681-\u169A]
    | [\u16A0-\u16EA]
    | [\u16EE-\u16F8]
    | [\u1700-\u170C]
    | [\u170E-\u1711]
    | [\u1720-\u1731]
    | [\u1740-\u1751]
    | [\u1760-\u176C]
    | [\u176E-\u1770]
    | [\u1780-\u17B3]
    | [\u17D7]
    | [\u17DB-\u17DC]
    | [\u1820-\u1877]
    | [\u1880-\u1884]
    | [\u1887-\u18A8]
    | [\u18AA]
    | [\u18B0-\u18F5]
    | [\u1900-\u191E]
    | [\u1950-\u196D]
    | [\u1970-\u1974]
    | [\u1980-\u19AB]
    | [\u19B0-\u19C9]
    | [\u1A00-\u1A16]
    | [\u1A20-\u1A54]
    | [\u1AA7]
    | [\u1B05-\u1B33]
    | [\u1B45-\u1B4B]
    | [\u1B83-\u1BA0]
    | [\u1BAE-\u1BAF]
    | [\u1BBA-\u1BE5]
    | [\u1C00-\u1C23]
    | [\u1C4D-\u1C4F]
    | [\u1C5A-\u1C7D]
    | [\u1C80-\u1C88]
    | [\u1CE9-\u1CEC]
    | [\u1CEE-\u1CF1]
    | [\u1CF5-\u1CF6]
    | [\u1D00-\u1DBF]
    | [\u1E00-\u1F15]
    | [\u1F18-\u1F1D]
    | [\u1F20-\u1F45]
    | [\u1F48-\u1F4D]
    | [\u1F50-\u1F57]
    | [\u1F59]
    | [\u1F5B]
    | [\u1F5D]
    | [\u1F5F-\u1F7D]
    | [\u1F80-\u1FB4]
    | [\u1FB6-\u1FBC]
    | [\u1FBE]
    | [\u1FC2-\u1FC4]
    | [\u1FC6-\u1FCC]
    | [\u1FD0-\u1FD3]
    | [\u1FD6-\u1FDB]
    | [\u1FE0-\u1FEC]
    | [\u1FF2-\u1FF4]
    | [\u1FF6-\u1FFC]
    | [\u203F-\u2040]
    | [\u2054]
    | [\u2071]
    | [\u207F]
    | [\u2090-\u209C]
    | [\u20A0-\u20BF]
    | [\u2102]
    | [\u2107]
    | [\u210A-\u2113]
    | [\u2115]
    | [\u2119-\u211D]
    | [\u2124]
    | [\u2126]
    | [\u2128]
    | [\u212A-\u212D]
    | [\u212F-\u2139]
    | [\u213C-\u213F]
    | [\u2145-\u2149]
    | [\u214E]
    | [\u2160-\u2188]
    | [\u2C00-\u2C2E]
    | [\u2C30-\u2C5E]
    | [\u2C60-\u2CE4]
    | [\u2CEB-\u2CEE]
    | [\u2CF2-\u2CF3]
    | [\u2D00-\u2D25]
    | [\u2D27]
    | [\u2D2D]
    | [\u2D30-\u2D67]
    | [\u2D6F]
    | [\u2D80-\u2D96]
    | [\u2DA0-\u2DA6]
    | [\u2DA8-\u2DAE]
    | [\u2DB0-\u2DB6]
    | [\u2DB8-\u2DBE]
    | [\u2DC0-\u2DC6]
    | [\u2DC8-\u2DCE]
    | [\u2DD0-\u2DD6]
    | [\u2DD8-\u2DDE]
    | [\u2E2F]
    | [\u3005-\u3007]
    | [\u3021-\u3029]
    | [\u3031-\u3035]
    | [\u3038-\u303C]
    | [\u3041-\u3096]
    | [\u309D-\u309F]
    | [\u30A1-\u30FA]
    | [\u30FC-\u30FF]
    | [\u3105-\u312E]
    | [\u3131-\u318E]
    | [\u31A0-\u31BA]
    | [\u31F0-\u31FF]
    | [\u3400-\u4DB5]
    | [\u4E00-\u9FEA]
    | [\uA000-\uA48C]
    | [\uA4D0-\uA4FD]
    | [\uA500-\uA60C]
    | [\uA610-\uA61F]
    | [\uA62A-\uA62B]
    | [\uA640-\uA66E]
    | [\uA67F-\uA69D]
    | [\uA6A0-\uA6EF]
    | [\uA717-\uA71F]
    | [\uA722-\uA788]
    | [\uA78B-\uA7AE]
    | [\uA7B0-\uA7B7]
    | [\uA7F7-\uA801]
    | [\uA803-\uA805]
    | [\uA807-\uA80A]
    | [\uA80C-\uA822]
    | [\uA838]
    | [\uA840-\uA873]
    | [\uA882-\uA8B3]
    | [\uA8F2-\uA8F7]
    | [\uA8FB]
    | [\uA8FD]
    | [\uA90A-\uA925]
    | [\uA930-\uA946]
    | [\uA960-\uA97C]
    | [\uA984-\uA9B2]
    | [\uA9CF]
    | [\uA9E0-\uA9E4]
    | [\uA9E6-\uA9EF]
    | [\uA9FA-\uA9FE]
    | [\uAA00-\uAA28]
    | [\uAA40-\uAA42]
    | [\uAA44-\uAA4B]
    | [\uAA60-\uAA76]
    | [\uAA7A]
    | [\uAA7E-\uAAAF]
    | [\uAAB1]
    | [\uAAB5-\uAAB6]
    | [\uAAB9-\uAABD]
    | [\uAAC0]
    | [\uAAC2]
    | [\uAADB-\uAADD]
    | [\uAAE0-\uAAEA]
    | [\uAAF2-\uAAF4]
    | [\uAB01-\uAB06]
    | [\uAB09-\uAB0E]
    | [\uAB11-\uAB16]
    | [\uAB20-\uAB26]
    | [\uAB28-\uAB2E]
    | [\uAB30-\uAB5A]
    | [\uAB5C-\uAB65]
    | [\uAB70-\uABE2]
    | [\uAC00-\uD7A3]
    | [\uD7B0-\uD7C6]
    | [\uD7CB-\uD7FB]
    | [\uF900-\uFA6D]
    | [\uFA70-\uFAD9]
    | [\uFB00-\uFB06]
    | [\uFB13-\uFB17]
    | [\uFB1D]
    | [\uFB1F-\uFB28]
    | [\uFB2A-\uFB36]
    | [\uFB38-\uFB3C]
    | [\uFB3E]
    | [\uFB40-\uFB41]
    | [\uFB43-\uFB44]
    | [\uFB46-\uFBB1]
    | [\uFBD3-\uFD3D]
    | [\uFD50-\uFD8F]
    | [\uFD92-\uFDC7]
    | [\uFDF0-\uFDFC]
    | [\uFE33-\uFE34]
    | [\uFE4D-\uFE4F]
    | [\uFE69]
    | [\uFE70-\uFE74]
    | [\uFE76-\uFEFC]
    | [\uFF04]
    | [\uFF21-\uFF3A]
    | [\uFF3F]
    | [\uFF41-\uFF5A]
    | [\uFF66-\uFFBE]
    | [\uFFC2-\uFFC7]
    | [\uFFCA-\uFFCF]
    | [\uFFD2-\uFFD7]
    | [\uFFDA-\uFFDC]
    | [\uFFE0-\uFFE1]
    | [\uFFE5-\uFFE6]
    ;

fragment
IdentifierPart
    : IdentifierStart
    | [\u0030-\u0039]
    | [\u007F-\u009F]
    | [\u00AD]
    | [\u0300-\u036F]
    | [\u0483-\u0487]
    | [\u0591-\u05BD]
    | [\u05BF]
    | [\u05C1-\u05C2]
    | [\u05C4-\u05C5]
    | [\u05C7]
    | [\u0600-\u0605]
    | [\u0610-\u061A]
    | [\u061C]
    | [\u064B-\u0669]
    | [\u0670]
    | [\u06D6-\u06DD]
    | [\u06DF-\u06E4]
    | [\u06E7-\u06E8]
    | [\u06EA-\u06ED]
    | [\u06F0-\u06F9]
    | [\u070F]
    | [\u0711]
    | [\u0730-\u074A]
    | [\u07A6-\u07B0]
    | [\u07C0-\u07C9]
    | [\u07EB-\u07F3]
    | [\u0816-\u0819]
    | [\u081B-\u0823]
    | [\u0825-\u0827]
    | [\u0829-\u082D]
    | [\u0859-\u085B]
    | [\u08D4-\u0903]
    | [\u093A-\u093C]
    | [\u093E-\u094F]
    | [\u0951-\u0957]
    | [\u0962-\u0963]
    | [\u0966-\u096F]
    | [\u0981-\u0983]
    | [\u09BC]
    | [\u09BE-\u09C4]
    | [\u09C7-\u09C8]
    | [\u09CB-\u09CD]
    | [\u09D7]
    | [\u09E2-\u09E3]
    | [\u09E6-\u09EF]
    | [\u0A01-\u0A03]
    | [\u0A3C]
    | [\u0A3E-\u0A42]
    | [\u0A47-\u0A48]
    | [\u0A4B-\u0A4D]
    | [\u0A51]
    | [\u0A66-\u0A71]
    | [\u0A75]
    | [\u0A81-\u0A83]
    | [\u0ABC]
    | [\u0ABE-\u0AC5]
    | [\u0AC7-\u0AC9]
    | [\u0ACB-\u0ACD]
    | [\u0AE2-\u0AE3]
    | [\u0AE6-\u0AEF]
    | [\u0AFA-\u0AFF]
    | [\u0B01-\u0B03]
    | [\u0B3C]
    | [\u0B3E-\u0B44]
    | [\u0B47-\u0B48]
    | [\u0B4B-\u0B4D]
    | [\u0B56-\u0B57]
    | [\u0B62-\u0B63]
    | [\u0B66-\u0B6F]
    | [\u0B82]
    | [\u0BBE-\u0BC2]
    | [\u0BC6-\u0BC8]
    | [\u0BCA-\u0BCD]
    | [\u0BD7]
    | [\u0BE6-\u0BEF]
    | [\u0C00-\u0C03]
    | [\u0C3E-\u0C44]
    | [\u0C46-\u0C48]
    | [\u0C4A-\u0C4D]
    | [\u0C55-\u0C56]
    | [\u0C62-\u0C63]
    | [\u0C66-\u0C6F]
    | [\u0C81-\u0C83]
    | [\u0CBC]
    | [\u0CBE-\u0CC4]
    | [\u0CC6-\u0CC8]
    | [\u0CCA-\u0CCD]
    | [\u0CD5-\u0CD6]
    | [\u0CE2-\u0CE3]
    | [\u0CE6-\u0CEF]
    | [\u0D00-\u0D03]
    | [\u0D3B-\u0D3C]
    | [\u0D3E-\u0D44]
    | [\u0D46-\u0D48]
    | [\u0D4A-\u0D4D]
    | [\u0D57]
    | [\u0D62-\u0D63]
    | [\u0D66-\u0D6F]
    | [\u0D82-\u0D83]
    | [\u0DCA]
    | [\u0DCF-\u0DD4]
    | [\u0DD6]
    | [\u0DD8-\u0DDF]
    | [\u0DE6-\u0DEF]
    | [\u0DF2-\u0DF3]
    | [\u0E31]
    | [\u0E34-\u0E3A]
    | [\u0E47-\u0E4E]
    | [\u0E50-\u0E59]
    | [\u0EB1]
    | [\u0EB4-\u0EB9]
    | [\u0EBB-\u0EBC]
    | [\u0EC8-\u0ECD]
    | [\u0ED0-\u0ED9]
    | [\u0F18-\u0F19]
    | [\u0F20-\u0F29]
    | [\u0F35]
    | [\u0F37]
    | [\u0F39]
    | [\u0F3E-\u0F3F]
    | [\u0F71-\u0F84]
    | [\u0F86-\u0F87]
    | [\u0F8D-\u0F97]
    | [\u0F99-\u0FBC]
    | [\u0FC6]
    | [\u102B-\u103E]
    | [\u1040-\u1049]
    | [\u1056-\u1059]
    | [\u105E-\u1060]
    | [\u1062-\u1064]
    | [\u1067-\u106D]
    | [\u1071-\u1074]
    | [\u1082-\u108D]
    | [\u108F-\u109D]
    | [\u135D-\u135F]
    | [\u1712-\u1714]
    | [\u1732-\u1734]
    | [\u1752-\u1753]
    | [\u1772-\u1773]
    | [\u17B4-\u17D3]
    | [\u17DD]
    | [\u17E0-\u17E9]
    | [\u180B-\u180E]
    | [\u1810-\u1819]
    | [\u1885-\u1886]
    | [\u18A9]
    | [\u1920-\u192B]
    | [\u1930-\u193B]
    | [\u1946-\u194F]
    | [\u19D0-\u19D9]
    | [\u1A17-\u1A1B]
    | [\u1A55-\u1A5E]
    | [\u1A60-\u1A7C]
    | [\u1A7F-\u1A89]
    | [\u1A90-\u1A99]
    | [\u1AB0-\u1ABD]
    | [\u1B00-\u1B04]
    | [\u1B34-\u1B44]
    | [\u1B50-\u1B59]
    | [\u1B6B-\u1B73]
    | [\u1B80-\u1B82]
    | [\u1BA1-\u1BAD]
    | [\u1BB0-\u1BB9]
    | [\u1BE6-\u1BF3]
    | [\u1C24-\u1C37]
    | [\u1C40-\u1C49]
    | [\u1C50-\u1C59]
    | [\u1CD0-\u1CD2]
    | [\u1CD4-\u1CE8]
    | [\u1CED]
    | [\u1CF2-\u1CF4]
    | [\u1CF7-\u1CF9]
    | [\u1DC0-\u1DF9]
    | [\u1DFB-\u1DFF]
    | [\u200B-\u200F]
    | [\u202A-\u202E]
    | [\u2060-\u2064]
    | [\u2066-\u206F]
    | [\u20D0-\u20DC]
    | [\u20E1]
    | [\u20E5-\u20F0]
    | [\u2CEF-\u2CF1]
    | [\u2D7F]
    | [\u2DE0-\u2DFF]
    | [\u302A-\u302F]
    | [\u3099-\u309A]
    | [\uA620-\uA629]
    | [\uA66F]
    | [\uA674-\uA67D]
    | [\uA69E-\uA69F]
    | [\uA6F0-\uA6F1]
    | [\uA802]
    | [\uA806]
    | [\uA80B]
    | [\uA823-\uA827]
    | [\uA880-\uA881]
    | [\uA8B4-\uA8C5]
    | [\uA8D0-\uA8D9]
    | [\uA8E0-\uA8F1]
    | [\uA900-\uA909]
    | [\uA926-\uA92D]
    | [\uA947-\uA953]
    | [\uA980-\uA983]
    | [\uA9B3-\uA9C0]
    | [\uA9D0-\uA9D9]
    | [\uA9E5]
    | [\uA9F0-\uA9F9]
    | [\uAA29-\uAA36]
    | [\uAA43]
    | [\uAA4C-\uAA4D]
    | [\uAA50-\uAA59]
    | [\uAA7B-\uAA7D]
    | [\uAAB0]
    | [\uAAB2-\uAAB4]
    | [\uAAB7-\uAAB8]
    | [\uAABE-\uAABF]
    | [\uAAC1]
    | [\uAAEB-\uAAEF]
    | [\uAAF5-\uAAF6]
    | [\uABE3-\uABEA]
    | [\uABEC-\uABED]
    | [\uABF0-\uABF9]
    | [\uFB1E]
    | [\uFE00-\uFE0F]
    | [\uFE20-\uFE2F]
    | [\uFEFF]
    | [\uFF10-\uFF19]
    | [\uFFF9-\uFFFB]
    ;

