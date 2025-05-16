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
    | query DOT K_TOSTRING LPAREN RPAREN
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
    : K_TX LPAREN RPAREN DOT K_BEGIN LPAREN RPAREN
    | K_TX LPAREN RPAREN DOT K_COMMIT LPAREN RPAREN
    | K_TX LPAREN RPAREN DOT K_ROLLBACK LPAREN RPAREN
    ;

rootTraversal
    : traversalSource DOT traversalSourceSpawnMethod
    | traversalSource DOT traversalSourceSpawnMethod DOT chainedTraversal
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
    : K_WITHBULK LPAREN booleanArgument RPAREN
    ;

traversalSourceSelfMethod_withPath
    : K_WITHPATH LPAREN RPAREN
    ;

traversalSourceSelfMethod_withSack
    : K_WITHSACK LPAREN genericArgument RPAREN
    | K_WITHSACK LPAREN genericArgument COMMA traversalBiFunction RPAREN
    ;

traversalSourceSelfMethod_withSideEffect
    : K_WITHSIDEEFFECT LPAREN stringLiteral COMMA genericArgument RPAREN
    | K_WITHSIDEEFFECT LPAREN stringLiteral COMMA genericArgument COMMA traversalBiFunction RPAREN
    ;

traversalSourceSelfMethod_withStrategies
    : K_WITHSTRATEGIES LPAREN traversalStrategy (COMMA traversalStrategyVarargs)? RPAREN
    ;

traversalSourceSelfMethod_withoutStrategies
    : K_WITHOUTSTRATEGIES LPAREN classType (COMMA classTypeList)? RPAREN
    ;

traversalSourceSelfMethod_with
    : K_WITH LPAREN stringLiteral RPAREN
    | K_WITH LPAREN stringLiteral COMMA genericArgument RPAREN
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
    : K_ADDE LPAREN stringArgument RPAREN
    | K_ADDE LPAREN nestedTraversal RPAREN
    ;

traversalSourceSpawnMethod_addV
    : K_ADDV LPAREN RPAREN
    | K_ADDV LPAREN stringArgument RPAREN
    | K_ADDV LPAREN nestedTraversal RPAREN
    ;

traversalSourceSpawnMethod_E
    : K_E LPAREN genericArgumentVarargs RPAREN
    ;

traversalSourceSpawnMethod_V
    : K_V LPAREN genericArgumentVarargs RPAREN
    ;

traversalSourceSpawnMethod_inject
    : K_INJECT LPAREN genericArgumentVarargs RPAREN
    ;

traversalSourceSpawnMethod_io
    : K_IO LPAREN stringLiteral RPAREN
    ;

traversalSourceSpawnMethod_mergeV
    : K_MERGEV LPAREN genericMapNullableArgument RPAREN #traversalSourceSpawnMethod_mergeV_Map
    | K_MERGEV LPAREN nestedTraversal RPAREN #traversalSourceSpawnMethod_mergeV_Traversal
    ;

traversalSourceSpawnMethod_mergeE
    : K_MERGEE LPAREN genericMapNullableArgument RPAREN #traversalSourceSpawnMethod_mergeE_Map
    | K_MERGEE LPAREN nestedTraversal RPAREN #traversalSourceSpawnMethod_mergeE_Traversal
    ;

traversalSourceSpawnMethod_call
    : K_CALL LPAREN RPAREN #traversalSourceSpawnMethod_call_empty
    | K_CALL LPAREN stringLiteral RPAREN #traversalSourceSpawnMethod_call_string
    | K_CALL LPAREN stringLiteral COMMA genericMapArgument RPAREN #traversalSourceSpawnMethod_call_string_map
    | K_CALL LPAREN stringLiteral COMMA nestedTraversal RPAREN #traversalSourceSpawnMethod_call_string_traversal
    | K_CALL LPAREN stringLiteral COMMA genericMapArgument COMMA nestedTraversal RPAREN #traversalSourceSpawnMethod_call_string_map_traversal
    ;

traversalSourceSpawnMethod_union
    : K_UNION LPAREN nestedTraversalList RPAREN
    ;

chainedTraversal
    : traversalMethod
    | chainedTraversal DOT traversalMethod
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
    | traversalMethod_discard
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
    : K_V LPAREN genericArgumentVarargs RPAREN
    ;

traversalMethod_E
    : K_E LPAREN genericArgumentVarargs RPAREN
    ;

traversalMethod_addE
    : K_ADDE LPAREN stringArgument RPAREN #traversalMethod_addE_String
    | K_ADDE LPAREN nestedTraversal RPAREN #traversalMethod_addE_Traversal
    ;

traversalMethod_addV
    : K_ADDV LPAREN RPAREN #traversalMethod_addV_Empty
    | K_ADDV LPAREN stringArgument RPAREN #traversalMethod_addV_String
    | K_ADDV LPAREN nestedTraversal RPAREN #traversalMethod_addV_Traversal
    ;

traversalMethod_aggregate
    : K_AGGREGATE LPAREN traversalScope COMMA stringLiteral RPAREN #traversalMethod_aggregate_Scope_String
    | K_AGGREGATE LPAREN stringLiteral RPAREN #traversalMethod_aggregate_String
    ;

traversalMethod_all
    : K_ALL LPAREN traversalPredicate RPAREN #traversalMethod_all_P
    ;

traversalMethod_and
    : K_AND LPAREN nestedTraversalList RPAREN
    ;

traversalMethod_any
    : K_ANY LPAREN traversalPredicate RPAREN #traversalMethod_any_P
    ;

traversalMethod_as
    : K_AS LPAREN stringLiteral (COMMA stringNullableLiteralVarargs)? RPAREN
    ;

traversalMethod_asDate
    : K_ASDATE LPAREN RPAREN
    ;

traversalMethod_asString
    : K_ASSTRING LPAREN RPAREN #traversalMethod_asString_Empty
    | K_ASSTRING LPAREN traversalScope RPAREN #traversalMethod_asString_Scope
    ;

traversalMethod_barrier
    : K_BARRIER LPAREN traversalSackMethod RPAREN #traversalMethod_barrier_Consumer
    | K_BARRIER LPAREN RPAREN #traversalMethod_barrier_Empty
    | K_BARRIER LPAREN integerLiteral RPAREN #traversalMethod_barrier_int
    ;

traversalMethod_both
    : K_BOTH LPAREN stringNullableArgumentVarargs RPAREN
    ;

traversalMethod_bothE
    : K_BOTHE LPAREN stringNullableArgumentVarargs RPAREN
    ;

traversalMethod_bothV
    : K_BOTHV LPAREN RPAREN
    ;

traversalMethod_branch
    : K_BRANCH LPAREN nestedTraversal RPAREN
    ;

traversalMethod_by
    : K_BY LPAREN traversalComparator RPAREN #traversalMethod_by_Comparator
    | K_BY LPAREN RPAREN #traversalMethod_by_Empty
    | K_BY LPAREN traversalFunction RPAREN #traversalMethod_by_Function
    | K_BY LPAREN traversalFunction COMMA traversalComparator RPAREN #traversalMethod_by_Function_Comparator
    | K_BY LPAREN traversalOrder RPAREN #traversalMethod_by_Order
    | K_BY LPAREN stringLiteral RPAREN #traversalMethod_by_String
    | K_BY LPAREN stringLiteral COMMA traversalComparator RPAREN #traversalMethod_by_String_Comparator
    | K_BY LPAREN traversalT RPAREN #traversalMethod_by_T
    | K_BY LPAREN nestedTraversal RPAREN #traversalMethod_by_Traversal
    | K_BY LPAREN nestedTraversal COMMA traversalComparator RPAREN #traversalMethod_by_Traversal_Comparator
    ;

traversalMethod_call
    : K_CALL LPAREN stringLiteral RPAREN #traversalMethod_call_string
    | K_CALL LPAREN stringLiteral COMMA genericMapArgument RPAREN #traversalMethod_call_string_map
    | K_CALL LPAREN stringLiteral COMMA nestedTraversal RPAREN #traversalMethod_call_string_traversal
    | K_CALL LPAREN stringLiteral COMMA genericMapArgument COMMA nestedTraversal RPAREN #traversalMethod_call_string_map_traversal
    ;

traversalMethod_cap
    : K_CAP LPAREN stringLiteral (COMMA stringNullableLiteralVarargs)? RPAREN
    ;

traversalMethod_choose
    : K_CHOOSE LPAREN traversalFunction RPAREN #traversalMethod_choose_Function
    | K_CHOOSE LPAREN traversalPredicate COMMA nestedTraversal RPAREN #traversalMethod_choose_Predicate_Traversal
    | K_CHOOSE LPAREN traversalPredicate COMMA nestedTraversal COMMA nestedTraversal RPAREN #traversalMethod_choose_Predicate_Traversal_Traversal
    | K_CHOOSE LPAREN nestedTraversal RPAREN #traversalMethod_choose_Traversal
    | K_CHOOSE LPAREN nestedTraversal COMMA nestedTraversal RPAREN #traversalMethod_choose_Traversal_Traversal
    | K_CHOOSE LPAREN nestedTraversal COMMA nestedTraversal COMMA nestedTraversal RPAREN #traversalMethod_choose_Traversal_Traversal_Traversal
    ;

traversalMethod_coalesce
    : K_COALESCE LPAREN nestedTraversalList RPAREN
    ;

traversalMethod_coin
    : K_COIN LPAREN floatArgument RPAREN
    ;

traversalMethod_combine
    : K_COMBINE LPAREN genericArgument RPAREN #traversalMethod_combine_Object
    ;

traversalMethod_concat
    : K_CONCAT LPAREN nestedTraversal (COMMA nestedTraversalList)? RPAREN #traversalMethod_concat_Traversal_Traversal
    | K_CONCAT LPAREN stringNullableLiteralVarargs RPAREN #traversalMethod_concat_String
    ;

traversalMethod_conjoin
    : K_CONJOIN LPAREN stringArgument RPAREN #traversalMethod_conjoin_String
    ;

traversalMethod_connectedComponent
    : K_CONNECTEDCOMPONENT LPAREN RPAREN
    ;

traversalMethod_constant
    : K_CONSTANT LPAREN genericArgument RPAREN
    ;

traversalMethod_count
    : K_COUNT LPAREN RPAREN #traversalMethod_count_Empty
    | K_COUNT LPAREN traversalScope RPAREN #traversalMethod_count_Scope
    ;

traversalMethod_cyclicPath
    : K_CYCLICPATH LPAREN RPAREN
    ;

traversalMethod_dateAdd
    : K_DATEADD LPAREN traversalDT COMMA integerLiteral RPAREN
    ;

traversalMethod_dateDiff
    : K_DATEDIFF LPAREN nestedTraversal RPAREN #traversalMethod_dateDiff_Traversal
    | K_DATEDIFF LPAREN dateLiteral RPAREN #traversalMethod_dateDiff_Date
    ;

traversalMethod_dedup
    : K_DEDUP LPAREN traversalScope (COMMA stringNullableLiteralVarargs)? RPAREN #traversalMethod_dedup_Scope_String
    | K_DEDUP LPAREN stringNullableLiteralVarargs RPAREN #traversalMethod_dedup_String
    ;

traversalMethod_difference
    : K_DIFFERENCE LPAREN genericArgument RPAREN #traversalMethod_difference_Object
    ;

traversalMethod_discard
    : 'discard' LPAREN RPAREN
    ;

traversalMethod_disjunct
    : K_DISJUNCT LPAREN genericArgument RPAREN #traversalMethod_disjunct_Object
    ;

traversalMethod_drop
    : K_DROP LPAREN RPAREN
    ;

traversalMethod_element
    : K_ELEMENT LPAREN RPAREN
    ;

traversalMethod_elementMap
    : K_ELEMENTMAP LPAREN stringNullableLiteralVarargs RPAREN
    ;

traversalMethod_emit
    : K_EMIT LPAREN RPAREN #traversalMethod_emit_Empty
    | K_EMIT LPAREN traversalPredicate RPAREN #traversalMethod_emit_Predicate
    | K_EMIT LPAREN nestedTraversal RPAREN #traversalMethod_emit_Traversal
    ;

traversalMethod_fail
    : K_FAIL LPAREN RPAREN #traversalMethod_fail_Empty
    | K_FAIL LPAREN stringLiteral RPAREN #traversalMethod_fail_String
    ;

traversalMethod_filter
    : K_FILTER LPAREN traversalPredicate RPAREN #traversalMethod_filter_Predicate
    | K_FILTER LPAREN nestedTraversal RPAREN #traversalMethod_filter_Traversal
    ;

traversalMethod_flatMap
    : K_FLATMAP LPAREN nestedTraversal RPAREN
    ;

traversalMethod_fold
    : K_FOLD LPAREN RPAREN #traversalMethod_fold_Empty
    | K_FOLD LPAREN genericArgument COMMA traversalBiFunction RPAREN #traversalMethod_fold_Object_BiFunction
    ;

traversalMethod_format
    : K_FORMAT LPAREN stringLiteral RPAREN #traversalMethod_format_String
    ;

traversalMethod_from
    : K_FROM LPAREN stringLiteral RPAREN #traversalMethod_from_String
    | K_FROM LPAREN structureVertexArgument RPAREN #traversalMethod_from_Vertex
    | K_FROM LPAREN nestedTraversal RPAREN #traversalMethod_from_Traversal
    ;

traversalMethod_group
    : K_GROUP LPAREN RPAREN #traversalMethod_group_Empty
    | K_GROUP LPAREN stringLiteral RPAREN #traversalMethod_group_String
    ;

traversalMethod_groupCount
    : K_GROUPCOUNT LPAREN RPAREN #traversalMethod_groupCount_Empty
    | K_GROUPCOUNT LPAREN stringLiteral RPAREN #traversalMethod_groupCount_String
    ;

traversalMethod_has
    : K_HAS LPAREN stringNullableLiteral RPAREN #traversalMethod_has_String
    | K_HAS LPAREN stringNullableLiteral COMMA genericArgument RPAREN #traversalMethod_has_String_Object
    | K_HAS LPAREN stringNullableLiteral COMMA traversalPredicate RPAREN #traversalMethod_has_String_P
    | K_HAS LPAREN stringNullableArgument COMMA stringNullableLiteral COMMA genericArgument RPAREN #traversalMethod_has_String_String_Object
    | K_HAS LPAREN stringNullableArgument COMMA stringNullableLiteral COMMA traversalPredicate RPAREN #traversalMethod_has_String_String_P
    | K_HAS LPAREN stringNullableLiteral COMMA nestedTraversal RPAREN #traversalMethod_has_String_Traversal
    | K_HAS LPAREN traversalT COMMA genericArgument RPAREN #traversalMethod_has_T_Object
    | K_HAS LPAREN traversalT COMMA traversalPredicate RPAREN #traversalMethod_has_T_P
    | K_HAS LPAREN traversalT COMMA nestedTraversal RPAREN #traversalMethod_has_T_Traversal
    ;

traversalMethod_hasId
    : K_HASID LPAREN genericArgument (COMMA genericArgumentVarargs)? RPAREN #traversalMethod_hasId_Object_Object
    | K_HASID LPAREN traversalPredicate RPAREN #traversalMethod_hasId_P
    ;

traversalMethod_hasKey
    : K_HASKEY LPAREN traversalPredicate RPAREN #traversalMethod_hasKey_P
    | K_HASKEY LPAREN stringNullableLiteral (COMMA stringNullableLiteralVarargs)? RPAREN #traversalMethod_hasKey_String_String
    ;

traversalMethod_hasLabel
    : K_HASLABEL LPAREN traversalPredicate RPAREN #traversalMethod_hasLabel_P
    | K_HASLABEL LPAREN stringNullableArgument (COMMA stringNullableArgumentVarargs)? RPAREN #traversalMethod_hasLabel_String_String
    ;

traversalMethod_hasNot
    : K_HASNOT LPAREN stringNullableLiteral RPAREN
    ;

traversalMethod_hasValue
    : K_HASVALUE LPAREN genericArgument (COMMA genericArgumentVarargs)? RPAREN #traversalMethod_hasValue_Object_Object
    | K_HASVALUE LPAREN traversalPredicate RPAREN #traversalMethod_hasValue_P
    ;

traversalMethod_id
    : K_ID LPAREN RPAREN
    ;

traversalMethod_identity
    : K_IDENTITY LPAREN RPAREN
    ;

traversalMethod_in
    : K_IN LPAREN stringNullableArgumentVarargs RPAREN
    ;

traversalMethod_inE
    : K_INE LPAREN stringNullableArgumentVarargs RPAREN
    ;

traversalMethod_intersect
    : K_INTERSECT LPAREN genericArgument RPAREN #traversalMethod_intersect_Object
    ;

traversalMethod_inV
    : K_INV LPAREN RPAREN
    ;

traversalMethod_index
    : K_INDEX LPAREN RPAREN
    ;

traversalMethod_inject
    : K_INJECT LPAREN genericArgumentVarargs RPAREN
    ;

traversalMethod_is
    : K_IS LPAREN genericArgument RPAREN #traversalMethod_is_Object
    | K_IS LPAREN traversalPredicate RPAREN #traversalMethod_is_P
    ;

traversalMethod_key
    : K_KEY LPAREN RPAREN
    ;

traversalMethod_label
    : K_LABEL LPAREN RPAREN
    ;

traversalMethod_length
    : K_LENGTH LPAREN RPAREN #traversalMethod_length_Empty
    | K_LENGTH LPAREN traversalScope RPAREN #traversalMethod_length_Scope
    ;

traversalMethod_limit
    : K_LIMIT LPAREN traversalScope COMMA integerArgument RPAREN #traversalMethod_limit_Scope_long
    | K_LIMIT LPAREN integerArgument RPAREN #traversalMethod_limit_long
    ;

traversalMethod_local
    : K_LOCAL LPAREN nestedTraversal RPAREN
    ;

traversalMethod_loops
    : K_LOOPS LPAREN RPAREN #traversalMethod_loops_Empty
    | K_LOOPS LPAREN stringLiteral RPAREN #traversalMethod_loops_String
    ;

traversalMethod_lTrim
    : K_LTRIM LPAREN RPAREN #traversalMethod_lTrim_Empty
    | K_LTRIM LPAREN traversalScope RPAREN #traversalMethod_lTrim_Scope
    ;

traversalMethod_map
    : K_MAP LPAREN nestedTraversal RPAREN
    ;

traversalMethod_match
    : K_MATCH LPAREN nestedTraversalList RPAREN
    ;

traversalMethod_math
    : K_MATH LPAREN stringLiteral RPAREN
    ;

traversalMethod_max
    : K_MAX LPAREN RPAREN #traversalMethod_max_Empty
    | K_MAX LPAREN traversalScope RPAREN #traversalMethod_max_Scope
    ;

traversalMethod_mean
    : K_MEAN LPAREN RPAREN #traversalMethod_mean_Empty
    | K_MEAN LPAREN traversalScope RPAREN #traversalMethod_mean_Scope
    ;

traversalMethod_merge
    : K_MERGE LPAREN genericArgument RPAREN #traversalMethod_merge_Object
    ;

traversalMethod_mergeV
    : K_MERGEV LPAREN RPAREN #traversalMethod_mergeV_empty
    | K_MERGEV LPAREN genericMapNullableArgument RPAREN #traversalMethod_mergeV_Map
    | K_MERGEV LPAREN nestedTraversal RPAREN #traversalMethod_mergeV_Traversal
    ;

traversalMethod_mergeE
    : K_MERGEE LPAREN RPAREN #traversalMethod_mergeE_empty
    | K_MERGEE LPAREN genericMapNullableArgument RPAREN #traversalMethod_mergeE_Map
    | K_MERGEE LPAREN nestedTraversal RPAREN #traversalMethod_mergeE_Traversal
    ;

traversalMethod_min
    : K_MIN LPAREN RPAREN #traversalMethod_min_Empty
    | K_MIN LPAREN traversalScope RPAREN #traversalMethod_min_Scope
    ;

traversalMethod_none
    : K_NONE LPAREN traversalPredicate RPAREN #traversalMethod_none_P
    ;

traversalMethod_not
    : K_NOT LPAREN nestedTraversal RPAREN
    ;

traversalMethod_option
    : K_OPTION LPAREN traversalPredicate COMMA nestedTraversal RPAREN #traversalMethod_option_Predicate_Traversal
    | K_OPTION LPAREN traversalMerge COMMA genericMapNullableArgument RPAREN #traversalMethod_option_Merge_Map
    | K_OPTION LPAREN traversalMerge COMMA genericMapNullableArgument COMMA traversalCardinality RPAREN #traversalMethod_option_Merge_Map_Cardinality
    | K_OPTION LPAREN traversalMerge COMMA nestedTraversal RPAREN #traversalMethod_option_Merge_Traversal
    | K_OPTION LPAREN genericArgument COMMA nestedTraversal RPAREN #traversalMethod_option_Object_Traversal
    | K_OPTION LPAREN nestedTraversal RPAREN #traversalMethod_option_Traversal
    ;

traversalMethod_optional
    : K_OPTIONAL LPAREN nestedTraversal RPAREN
    ;

traversalMethod_or
    : K_OR LPAREN nestedTraversalList RPAREN
    ;

traversalMethod_order
    : K_ORDER LPAREN RPAREN #traversalMethod_order_Empty
    | K_ORDER LPAREN traversalScope RPAREN #traversalMethod_order_Scope
    ;

traversalMethod_otherV
    : K_OTHERV LPAREN RPAREN
    ;

traversalMethod_out
    : K_OUT LPAREN stringNullableArgumentVarargs RPAREN
    ;

traversalMethod_outE
    : K_OUTE LPAREN stringNullableArgumentVarargs RPAREN
    ;

traversalMethod_outV
    : K_OUTV LPAREN RPAREN
    ;

traversalMethod_pageRank
    : K_PAGERANK LPAREN RPAREN #traversalMethod_pageRank_Empty
    | K_PAGERANK LPAREN floatArgument RPAREN #traversalMethod_pageRank_double
    ;

traversalMethod_path
    : K_PATH LPAREN RPAREN
    ;

traversalMethod_peerPressure
    : K_PEERPRESSURE LPAREN RPAREN
    ;

traversalMethod_product
    : K_PRODUCT LPAREN genericArgument RPAREN #traversalMethod_product_Object
    ;

traversalMethod_profile
    : K_PROFILE LPAREN RPAREN #traversalMethod_profile_Empty
    | K_PROFILE LPAREN stringLiteral RPAREN #traversalMethod_profile_String
    ;

traversalMethod_project
    : K_PROJECT LPAREN stringLiteral (COMMA stringNullableLiteralVarargs)? RPAREN
    ;

traversalMethod_properties
    : K_PROPERTIES LPAREN stringNullableLiteralVarargs RPAREN
    ;

traversalMethod_property
    : K_PROPERTY LPAREN traversalCardinality COMMA genericArgument COMMA genericArgument (COMMA genericArgumentVarargs)? RPAREN #traversalMethod_property_Cardinality_Object_Object_Object
    | K_PROPERTY LPAREN traversalCardinality COMMA genericMapNullableArgument RPAREN # traversalMethod_property_Cardinality_Object
    | K_PROPERTY LPAREN genericArgument COMMA genericArgument (COMMA genericArgumentVarargs)? RPAREN #traversalMethod_property_Object_Object_Object
    | K_PROPERTY LPAREN genericMapNullableArgument RPAREN # traversalMethod_property_Object
    ;

traversalMethod_propertyMap
    : K_PROPERTYMAP LPAREN stringNullableLiteralVarargs RPAREN
    ;

traversalMethod_range
    : K_RANGE LPAREN traversalScope COMMA integerArgument COMMA integerArgument RPAREN #traversalMethod_range_Scope_long_long
    | K_RANGE LPAREN integerArgument COMMA integerArgument RPAREN #traversalMethod_range_long_long
    ;

traversalMethod_read
    : K_READ LPAREN RPAREN
    ;

traversalMethod_repeat
    : K_REPEAT LPAREN stringLiteral COMMA nestedTraversal RPAREN #traversalMethod_repeat_String_Traversal
    | K_REPEAT LPAREN nestedTraversal RPAREN #traversalMethod_repeat_Traversal
    ;

traversalMethod_replace
    : K_REPLACE LPAREN stringNullableLiteral COMMA stringNullableLiteral RPAREN #traversalMethod_replace_String_String
    | K_REPLACE LPAREN traversalScope COMMA stringNullableLiteral COMMA stringNullableLiteral RPAREN #traversalMethod_replace_Scope_String_String
    ;

traversalMethod_reverse
    : K_REVERSE LPAREN RPAREN #traversalMethod_reverse_Empty
    ;

traversalMethod_rTrim
    : K_RTRIM LPAREN RPAREN #traversalMethod_rTrim_Empty
    | K_RTRIM LPAREN traversalScope RPAREN #traversalMethod_rTrim_Scope
    ;

traversalMethod_sack
    : K_SACK LPAREN traversalBiFunction RPAREN #traversalMethod_sack_BiFunction
    | K_SACK LPAREN RPAREN #traversalMethod_sack_Empty
    ;

traversalMethod_sample
    : K_SAMPLE LPAREN traversalScope COMMA integerLiteral RPAREN #traversalMethod_sample_Scope_int
    | K_SAMPLE LPAREN integerLiteral RPAREN #traversalMethod_sample_int
    ;

traversalMethod_select
    : K_SELECT LPAREN traversalColumn RPAREN #traversalMethod_select_Column
    | K_SELECT LPAREN traversalPop COMMA stringLiteral RPAREN #traversalMethod_select_Pop_String
    | K_SELECT LPAREN traversalPop COMMA stringLiteral COMMA stringLiteral (COMMA stringNullableLiteralVarargs)? RPAREN #traversalMethod_select_Pop_String_String_String
    | K_SELECT LPAREN traversalPop COMMA nestedTraversal RPAREN #traversalMethod_select_Pop_Traversal
    | K_SELECT LPAREN stringLiteral RPAREN #traversalMethod_select_String
    | K_SELECT LPAREN stringLiteral COMMA stringLiteral (COMMA stringNullableLiteralVarargs)? RPAREN #traversalMethod_select_String_String_String
    | K_SELECT LPAREN nestedTraversal RPAREN #traversalMethod_select_Traversal
    ;

traversalMethod_shortestPath
    : K_SHORTESTPATH LPAREN RPAREN
    ;

traversalMethod_sideEffect
    : K_SIDEEFFECT LPAREN nestedTraversal RPAREN
    ;

traversalMethod_simplePath
    : K_SIMPLEPATH LPAREN RPAREN
    ;

traversalMethod_skip
    : K_SKIP LPAREN traversalScope COMMA integerArgument RPAREN #traversalMethod_skip_Scope_long
    | K_SKIP LPAREN integerArgument RPAREN #traversalMethod_skip_long
    ;

traversalMethod_split
    : K_SPLIT LPAREN stringNullableLiteral RPAREN #traversalMethod_split_String
    | K_SPLIT LPAREN traversalScope COMMA stringNullableLiteral RPAREN #traversalMethod_split_Scope_String
    ;

traversalMethod_store
    : K_STORE LPAREN stringLiteral RPAREN
    ;

traversalMethod_subgraph
    : K_SUBGRAPH LPAREN stringLiteral RPAREN
    ;

traversalMethod_substring
    : K_SUBSTRING LPAREN integerLiteral RPAREN #traversalMethod_substring_int
    | K_SUBSTRING LPAREN traversalScope COMMA integerLiteral RPAREN #traversalMethod_substring_Scope_int
    | K_SUBSTRING LPAREN integerLiteral COMMA integerLiteral RPAREN #traversalMethod_substring_int_int
    | K_SUBSTRING LPAREN traversalScope COMMA integerLiteral COMMA integerLiteral RPAREN #traversalMethod_substring_Scope_int_int
    ;

traversalMethod_sum
    : K_SUM LPAREN RPAREN #traversalMethod_sum_Empty
    | K_SUM LPAREN traversalScope RPAREN #traversalMethod_sum_Scope
    ;

traversalMethod_tail
    : K_TAIL LPAREN RPAREN #traversalMethod_tail_Empty
    | K_TAIL LPAREN traversalScope RPAREN #traversalMethod_tail_Scope
    | K_TAIL LPAREN traversalScope COMMA integerArgument RPAREN #traversalMethod_tail_Scope_long
    | K_TAIL LPAREN integerArgument RPAREN #traversalMethod_tail_long
    ;

traversalMethod_timeLimit
    : K_TIMELIMIT LPAREN integerLiteral RPAREN
    ;

traversalMethod_times
    : K_TIMES LPAREN integerLiteral RPAREN
    ;

traversalMethod_to
    : K_TO LPAREN traversalDirection (COMMA stringNullableLiteralVarargs)? RPAREN #traversalMethod_to_Direction_String
    | K_TO LPAREN stringLiteral RPAREN #traversalMethod_to_String
    | K_TO LPAREN structureVertexArgument RPAREN #traversalMethod_to_Vertex
    | K_TO LPAREN nestedTraversal RPAREN #traversalMethod_to_Traversal
    ;

traversalMethod_toE
    : K_TOE LPAREN traversalDirection (COMMA stringNullableArgumentVarargs)? RPAREN
    ;

traversalMethod_toLower
    : K_TOLOWER LPAREN RPAREN #traversalMethod_toLower_Empty
    | K_TOLOWER LPAREN traversalScope RPAREN #traversalMethod_toLower_Scope
    ;

traversalMethod_toUpper
    : K_TOUPPER LPAREN RPAREN #traversalMethod_toUpper_Empty
    | K_TOUPPER LPAREN traversalScope RPAREN #traversalMethod_toUpper_Scope
    ;

traversalMethod_toV
    : K_TOV LPAREN traversalDirection RPAREN
    ;

traversalMethod_tree
    : K_TREE LPAREN RPAREN #traversalMethod_tree_Empty
    | K_TREE LPAREN stringLiteral RPAREN #traversalMethod_tree_String
    ;

traversalMethod_trim
    : K_TRIM LPAREN RPAREN #traversalMethod_trim_Empty
    | K_TRIM LPAREN traversalScope RPAREN #traversalMethod_trim_Scope
    ;

traversalMethod_unfold
    : K_UNFOLD LPAREN RPAREN
    ;

traversalMethod_union
    : K_UNION LPAREN nestedTraversalList RPAREN
    ;

traversalMethod_until
    : K_UNTIL LPAREN traversalPredicate RPAREN #traversalMethod_until_Predicate
    | K_UNTIL LPAREN nestedTraversal RPAREN #traversalMethod_until_Traversal
    ;

traversalMethod_value
    : K_VALUE LPAREN RPAREN
    ;

traversalMethod_valueMap
    : K_VALUEMAP LPAREN stringNullableLiteralVarargs RPAREN #traversalMethod_valueMap_String
    | K_VALUEMAP LPAREN booleanLiteral  (COMMA stringNullableLiteralVarargs)? RPAREN #traversalMethod_valueMap_boolean_String
    ;

traversalMethod_values
    : K_VALUES LPAREN stringNullableLiteralVarargs RPAREN
    ;

traversalMethod_where
    : K_WHERE LPAREN traversalPredicate RPAREN #traversalMethod_where_P
    | K_WHERE LPAREN stringLiteral COMMA traversalPredicate RPAREN #traversalMethod_where_String_P
    | K_WHERE LPAREN nestedTraversal RPAREN #traversalMethod_where_Traversal
    ;

traversalMethod_with
    : K_WITH LPAREN (withOptionKeys | stringLiteral) RPAREN #traversalMethod_with_String
    | K_WITH LPAREN (withOptionKeys | stringLiteral) COMMA (withOptionsValues | ioOptionsValues | genericArgument) RPAREN #traversalMethod_with_String_Object
    ;

traversalMethod_write
    : K_WRITE LPAREN RPAREN
    ;

/*********************************************
    ARGUMENT AND TERMINAL RULES
**********************************************/

// There is syntax available in the construction of a ReferenceVertex, that allows the label to not be specified.
// That use case is related to OLAP when the StarGraph does not preserve the label of adjacent vertices or other
// fail fast scenarios in that processing model. It is not relevant to the grammar however when a user is creating
// the Vertex to be used in a Traversal and therefore both id and label are required.
structureVertexLiteral
    : K_NEW? (K_VERTEX | K_REFERENCEVERTEX) LPAREN genericArgument COMMA stringArgument RPAREN
    ;

traversalStrategy
    : K_NEW? classType (LPAREN (configuration (COMMA configuration)*)? RPAREN)?
    ;

configuration
    : (keyword | nakedKey) COLON genericArgument
    ;

traversalScope
    : K_LOCAL | K_SCOPE DOT K_LOCAL
    | K_GLOBAL | K_SCOPE DOT K_GLOBAL
    ;

traversalBarrier
    : K_NORMSACK | K_BARRIERU DOT K_NORMSACK
    ;

traversalT
    : traversalTShort
    | traversalTLong
    ;

traversalTShort
    : K_ID
    | K_LABEL
    | K_KEY
    | K_VALUE
    ;

traversalTLong
    : K_T DOT K_ID
    | K_T DOT K_LABEL
    | K_T DOT K_KEY
    | K_T DOT K_VALUE
    ;

traversalMerge
    : K_ONCREATE | K_MERGEU DOT K_ONCREATE
    | K_ONMATCH | K_MERGEU DOT K_ONMATCH
    | K_OUTV | K_MERGEU DOT K_OUTV
    | K_INV | K_MERGEU DOT K_INV
    ;

traversalOrder
    : K_ASC  | K_ORDERU DOT K_ASC
    | K_DESC | K_ORDERU DOT K_DESC
    | K_SHUFFLE | K_ORDERU DOT K_SHUFFLE
    ;

traversalDirection
    : traversalDirectionShort
    | traversalDirectionLong
    ;

traversalDirectionShort
    : K_INU | K_FROM
    | K_OUTU | K_TO
    | K_BOTHU
    ;

traversalDirectionLong
    : K_DIRECTION DOT K_INU | K_DIRECTION DOT K_FROM
    | K_DIRECTION DOT K_OUTU | K_DIRECTION DOT K_TO
    | K_DIRECTION DOT K_BOTHU
    ;

traversalCardinality
    : (K_CARDINALITY DOT K_SINGLE | K_SINGLE) LPAREN genericLiteral RPAREN
    | (K_CARDINALITY DOT K_SET | K_SET) LPAREN genericLiteral RPAREN
    | (K_CARDINALITY DOT K_LIST | K_LIST) LPAREN genericLiteral RPAREN
    | K_SINGLE | K_CARDINALITY DOT K_SINGLE
    | K_SET | K_CARDINALITY DOT K_SET
    | K_LIST | K_CARDINALITY DOT K_LIST
    ;

traversalColumn
    : K_KEYS | K_COLUMN DOT K_KEYS
    | K_VALUES | K_COLUMN DOT K_VALUES
    ;

traversalPop
    : K_FIRST | K_POP DOT K_FIRST
    | K_LAST | K_POP DOT K_LAST
    | K_ALL | K_POP DOT K_ALL
    | K_MIXED | K_POP DOT K_MIXED
    ;

traversalOperator
    : K_ADDALL | K_OPERATOR DOT K_ADDALL
    | K_AND | K_OPERATOR DOT K_AND
    | K_ASSIGN | K_OPERATOR DOT K_ASSIGN
    | K_DIV | K_OPERATOR DOT K_DIV
    | K_MAX | K_OPERATOR DOT K_MAX
    | K_MIN | K_OPERATOR DOT K_MIN
    | K_MINUS | K_OPERATOR DOT K_MINUS
    | K_MULT | K_OPERATOR DOT K_MULT
    | K_OR | K_OPERATOR DOT K_OR
    | K_SUM | K_OPERATOR DOT K_SUM
    | K_SUMLONG | K_OPERATOR DOT K_SUMLONG
    ;

traversalPick
    : K_ANY | K_PICK DOT K_ANY
    | K_NONE | K_PICK DOT K_NONE
    ;

traversalDT
    : K_SECOND | K_DT DOT K_SECOND
    | K_MINUTE | K_DT DOT K_MINUTE
    | K_HOUR | K_DT DOT K_HOUR
    | K_DAY | K_DT DOT K_DAY
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
    | traversalPredicate DOT K_AND LPAREN traversalPredicate RPAREN
    | traversalPredicate DOT K_OR LPAREN traversalPredicate RPAREN
    | traversalPredicate DOT K_NEGATE LPAREN RPAREN
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

// Additional special rules that are derived from above
// These are used to restrict broad method signatures that accept lambdas
// to a smaller set.
traversalComparator
    : traversalOrder
    ;

traversalFunction
    : traversalT
    | traversalColumn
    ;

traversalBiFunction
    : traversalOperator
    ;

traversalPredicate_eq
    : (K_P DOT K_EQ | K_EQ) LPAREN genericArgument RPAREN
    ;

traversalPredicate_neq
    : (K_P DOT K_NEQ | K_NEQ) LPAREN genericArgument RPAREN
    ;

traversalPredicate_lt
    : (K_P DOT K_LT | K_LT) LPAREN genericArgument RPAREN
    ;

traversalPredicate_lte
    : (K_P DOT K_LTE | K_LTE) LPAREN genericArgument RPAREN
    ;

traversalPredicate_gt
    : (K_P DOT K_GT | K_GT) LPAREN genericArgument RPAREN
    ;

traversalPredicate_gte
    : (K_P DOT K_GTE | K_GTE) LPAREN genericArgument RPAREN
    ;

traversalPredicate_inside
    : (K_P DOT K_INSIDE | K_INSIDE) LPAREN genericArgument COMMA genericArgument RPAREN
    ;

traversalPredicate_outside
    : (K_P DOT K_OUTSIDE | K_OUTSIDE) LPAREN genericArgument COMMA genericArgument RPAREN
    ;

traversalPredicate_between
    : (K_P DOT K_BETWEEN | K_BETWEEN) LPAREN genericArgument COMMA genericArgument RPAREN
    ;

traversalPredicate_within
    : (K_P DOT K_WITHIN | K_WITHIN) LPAREN RPAREN
    | (K_P DOT K_WITHIN | K_WITHIN) LPAREN genericArgumentVarargs RPAREN
    ;

traversalPredicate_without
    : (K_P DOT K_WITHOUT | K_WITHOUT) LPAREN RPAREN
    | (K_P DOT K_WITHOUT | K_WITHOUT) LPAREN genericArgumentVarargs RPAREN
    ;

traversalPredicate_not
    : (K_P DOT K_NOT | K_NOT) LPAREN traversalPredicate RPAREN
    ;

traversalPredicate_containing
    : (K_TEXTP DOT K_CONTAINING | K_CONTAINING) LPAREN stringArgument RPAREN
    ;

traversalPredicate_notContaining
    : (K_TEXTP DOT K_NOTCONTAINING | K_NOTCONTAINING) LPAREN stringArgument RPAREN
    ;

traversalPredicate_startingWith
    : (K_TEXTP DOT K_STARTINGWITH | K_STARTINGWITH) LPAREN stringArgument RPAREN
    ;

traversalPredicate_notStartingWith
    : (K_TEXTP DOT K_NOTSTARTINGWITH | K_NOTSTARTINGWITH) LPAREN stringArgument RPAREN
    ;

traversalPredicate_endingWith
    : (K_TEXTP DOT K_ENDINGWITH | K_ENDINGWITH) LPAREN stringArgument RPAREN
    ;

traversalPredicate_notEndingWith
    : (K_TEXTP DOT K_NOTENDINGWITH | K_NOTENDINGWITH) LPAREN stringArgument RPAREN
    ;

traversalPredicate_regex
    : (K_TEXTP DOT K_REGEX | K_REGEX) LPAREN stringArgument RPAREN
    ;

traversalPredicate_notRegex
    : (K_TEXTP DOT K_NOTREGEX | K_NOTREGEX) LPAREN stringArgument RPAREN
    ;

traversalTerminalMethod_explain
    : K_EXPLAIN LPAREN RPAREN
    ;

traversalTerminalMethod_hasNext
    : K_HASNEXT LPAREN RPAREN
    ;

traversalTerminalMethod_iterate
    : K_ITERATE LPAREN RPAREN
    ;

traversalTerminalMethod_tryNext
    : K_TRYNEXT LPAREN RPAREN
    ;

traversalTerminalMethod_next
    : K_NEXT LPAREN RPAREN
    | K_NEXT LPAREN integerLiteral RPAREN
    ;

traversalTerminalMethod_toList
    : K_TOLIST LPAREN RPAREN
    ;

traversalTerminalMethod_toSet
    : K_TOSET LPAREN RPAREN
    ;

traversalTerminalMethod_toBulkSet
    : K_TOBULKSET LPAREN RPAREN
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
    : connectedComponentStringConstant DOT K_COMPONENT
    ;

connectedComponentConstants_edges
    : connectedComponentStringConstant DOT K_EDGES
    ;

connectedComponentConstants_propertyName
    : connectedComponentStringConstant DOT K_PROPERTYNAME
    ;

pageRankConstants_edges
    : pageRankStringConstant DOT K_EDGES
    ;

pageRankConstants_times
    : pageRankStringConstant DOT K_TIMES
    ;

pageRankConstants_propertyName
    : pageRankStringConstant DOT K_PROPERTYNAME
    ;

peerPressureConstants_edges
    : peerPressureStringConstant DOT K_EDGES
    ;

peerPressureConstants_times
    : peerPressureStringConstant DOT K_TIMES
    ;

peerPressureConstants_propertyName
    : peerPressureStringConstant DOT K_PROPERTYNAME
    ;

shortestPathConstants_target
    : shortestPathStringConstant DOT K_TARGET
    ;

shortestPathConstants_edges
    : shortestPathStringConstant DOT K_EDGES
    ;

shortestPathConstants_distance
    : shortestPathStringConstant DOT K_DISTANCE
    ;

shortestPathConstants_maxDistance
    : shortestPathStringConstant DOT K_MAXDISTANCE
    ;

shortestPathConstants_includeEdges
    : shortestPathStringConstant DOT K_INCLUDEEDGES
    ;

withOptionsConstants_tokens
    : withOptionsStringConstant DOT K_TOKENS
    ;

withOptionsConstants_none
    : withOptionsStringConstant DOT K_NONE
    ;

withOptionsConstants_ids
    : withOptionsStringConstant DOT K_IDS
    ;

withOptionsConstants_labels
    : withOptionsStringConstant DOT K_LABELS
    ;

withOptionsConstants_keys
    : withOptionsStringConstant DOT K_KEYS
    ;

withOptionsConstants_values
    : withOptionsStringConstant DOT K_VALUES
    ;

withOptionsConstants_all
    : withOptionsStringConstant DOT K_ALL
    ;

withOptionsConstants_indexer
    : withOptionsStringConstant DOT K_INDEXER
    ;

withOptionsConstants_list
    : withOptionsStringConstant DOT K_LIST
    ;

withOptionsConstants_map
    : withOptionsStringConstant DOT K_MAP
    ;

ioOptionsConstants_reader
    : ioOptionsStringConstant DOT K_READER
    ;

ioOptionsConstants_writer
    : ioOptionsStringConstant DOT K_WRITER
    ;

ioOptionsConstants_gryo
    : ioOptionsStringConstant DOT K_GRYO
    ;

ioOptionsConstants_graphson
    : ioOptionsStringConstant DOT K_GRAPHSON
    ;

ioOptionsConstants_graphml
    : ioOptionsStringConstant DOT K_GRAPHML
    ;

connectedComponentStringConstant
    : K_CONNECTEDCOMPONENTU
    ;

pageRankStringConstant
    : K_PAGERANKU
    ;

peerPressureStringConstant
    : K_PEERPRESSUREU
    ;

shortestPathStringConstant
    : K_SHORTESTPATHU
    ;

withOptionsStringConstant
    : K_WITHOPTOPTIONS
    ;

ioOptionsStringConstant
    : K_IOU
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

stringNullableArgumentVarargs
    : (stringNullableArgument (COMMA stringNullableArgument)*)?
    ;

dateArgument
    : dateLiteral
    | variable
    ;

genericArgument
    : genericLiteral
    | variable
    ;

genericArgumentVarargs
    : (genericArgument (COMMA genericArgument)*)?
    ;

genericMapArgument
    : genericMapLiteral
    | variable
    ;

genericMapNullableArgument
    : genericMapNullableLiteral
    | variable
    ;

nullableGenericLiteralMap
    : genericMapLiteral
    | nullLiteral
    ;

structureVertexArgument
    : structureVertexLiteral
    | variable
    ;

traversalStrategyVarargs
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

genericCollectionLiteral
    : LBRACK (genericLiteral (COMMA genericLiteral)*)? RBRACK
    ;

genericLiteralVarargs
    : genericLiteralExpr?
    ;

genericLiteralExpr
    : genericLiteral (COMMA genericLiteral)*
    ;

genericMapNullableLiteral
    : genericMapLiteral
    | nullLiteral
    ;

genericRangeLiteral
    : integerLiteral DOT DOT integerLiteral
    | stringLiteral DOT DOT stringLiteral
    ;

genericSetLiteral
    : LBRACE (genericLiteral (COMMA genericLiteral)*)? RBRACE
    ;

stringNullableLiteralVarargs
    : (stringNullableLiteral (COMMA stringNullableLiteral)*)?
    ;

genericLiteral
    : numericLiteral
    | booleanLiteral
    | stringLiteral
    | dateLiteral
    | nullLiteral
    // Allow the generic literal to match specific gremlin tokens also
    | traversalT
    | traversalCardinality
    | traversalDirection
    | traversalMerge
    | traversalPick
    | traversalDT
    | structureVertexLiteral
    | genericSetLiteral
    | genericCollectionLiteral
    | genericRangeLiteral
    | nestedTraversal
    | terminatedTraversal
    | uuidLiteral
    | genericMapLiteral
    ;

genericMapLiteral
    : LBRACK COLON RBRACK
    | LBRACK mapEntry (COMMA mapEntry)* RBRACK
    ;

mapKey
    : (LPAREN traversalT RPAREN | traversalTLong)
    | (LPAREN traversalDirection RPAREN | traversalDirectionLong)
    | (LPAREN genericSetLiteral RPAREN | genericSetLiteral)
    | (LPAREN genericCollectionLiteral RPAREN | genericCollectionLiteral)
    | (LPAREN genericMapLiteral RPAREN | genericMapLiteral)
    | (LPAREN stringLiteral RPAREN | stringLiteral)
    | (LPAREN numericLiteral RPAREN | numericLiteral)
    | (keyword | nakedKey)
    ;

mapEntry
    : mapKey COLON genericLiteral
    ;

stringLiteral
    : EmptyStringLiteral
    | NonEmptyStringLiteral
    ;

stringNullableLiteral
    : EmptyStringLiteral
    | NonEmptyStringLiteral
    | K_NULL
    ;

integerLiteral
    : IntegerLiteral
    ;

floatLiteral
    : FloatingPointLiteral
    | infLiteral
    | nanLiteral
    ;

numericLiteral
    : integerLiteral
    | floatLiteral
    ;

booleanLiteral
    : K_TRUE
    | K_FALSE
    ;

dateLiteral
    : K_DATETIME LPAREN stringArgument RPAREN
    | K_DATETIME LPAREN RPAREN
    | K_DATETIMEU LPAREN stringArgument RPAREN
    | K_DATETIMEU LPAREN RPAREN
    ;

nullLiteral
    : K_NULL
    ;

nanLiteral
    : K_NAN
    ;

infLiteral
    : K_INFINITY
    | SignedInfLiteral
    ;

uuidLiteral
    : K_UUID LPAREN RPAREN
    | K_UUID LPAREN stringLiteral RPAREN
    ;


nakedKey
    : Identifier
    ;

classType
    : Identifier
    ;

variable
    : Identifier
    ;

// every Gremlin keyword must be listed here or else these words will not be able to be used as
// Map/Configuration keys as naked identifiers like [all: 123]. without having that definition
// here that sort of Map definition will get a syntax error.
keyword
    : TRAVERSAL_ROOT // g - __ is not an allowable key in this context
    | K_ADDALL
    | K_ADDE
    | K_ADDV
    | K_AGGREGATE
    | K_ALL
    | K_AND
    | K_ANY
    | K_AS
    | K_ASC
    | K_ASDATE
    | K_ASSTRING
    | K_ASSIGN
    | K_BARRIER
    | K_BARRIERU
    | K_BEGIN
    | K_BETWEEN
    | K_BOTH
    | K_BOTHU
    | K_BOTHE
    | K_BOTHV
    | K_BRANCH
    | K_BY
    | K_CALL
    | K_CAP
    | K_CARDINALITY
    | K_CHOOSE
    | K_COALESCE
    | K_COIN
    | K_COLUMN
    | K_COMBINE
    | K_CONCAT
    | K_COMMIT
    | K_COMPONENT
    | K_CONJOIN
    | K_CONNECTEDCOMPONENT
    | K_CONNECTEDCOMPONENTU
    | K_CONSTANT
    | K_CONTAINING
    | K_COUNT
    | K_CYCLICPATH
    | K_DAY
    | K_DATEADD
    | K_DATEDIFF
    | K_DATETIME
    | K_DATETIMEU
    | K_DECR
    | K_DEDUP
    | K_DESC
    | K_DIFFERENCE
    | K_DIRECTION
    | K_DISJUNCT
    | K_DISTANCE
    | K_DIV
    | K_DROP
    | K_DT
    | K_E
    | K_EDGES
    | K_ELEMENTMAP
    | K_ELEMENT
    | K_EMIT
    | K_ENDINGWITH
    | K_EQ
    | K_EXPLAIN
    | K_FAIL
    | K_FALSE
    | K_FILTER
    | K_FIRST
    | K_FLATMAP
    | K_FOLD
    | K_FORMAT
    | K_FROM
    | K_GLOBAL
    | K_GT
    | K_GTE
    | K_GRAPHML
    | K_GRAPHSON
    | K_GROUP
    | K_GROUPCOUNT
    | K_GRYO
    | K_HAS
    | K_HASID
    | K_HASKEY
    | K_HASLABEL
    | K_HASNEXT
    | K_HASNOT
    | K_HASVALUE
    | K_HOUR
    | K_ID
    | K_IDENTITY
    | K_IDS
    | K_IN
    | K_INU
    | K_INCLUDEEDGES
    | K_INCR
    | K_INDEXER
    | K_INE
    | K_INDEX
    | K_INFINITY
    | K_INJECT
    | K_INSIDE
    | K_INTERSECT
    | K_INV
    | K_IO
    | K_IOU
    | K_IS
    | K_ITERATE
    | K_KEY
    | K_KEYS
    | K_LABELS
    | K_LABEL
    | K_LAST
    | K_LENGTH
    | K_LIMIT
    | K_LIST
    | K_LOCAL
    | K_LOOPS
    | K_LT
    | K_LTE
    | K_LTRIM
    | K_MAP
    | K_MATCH
    | K_MATH
    | K_MAX
    | K_MAXDISTANCE
    | K_MEAN
    | K_MERGE
    | K_MERGEU
    | K_MERGEE
    | K_MERGEV
    | K_MIN
    | K_MINUTE
    | K_MINUS
    | K_MIXED
    | K_MULT
    | K_NAN
    | K_NEGATE
    | K_NEW
    | K_NONE
    | K_NOTCONTAINING
    | K_NOTENDINGWITH
    | K_NOTREGEX
    | K_NOTSTARTINGWITH
    | K_NOT
    | K_NEQ
    | K_NEXT
    | K_NULL
    | K_NORMSACK
    | K_ONCREATE
    | K_ONMATCH
    | K_OPERATOR
    | K_OPTION
    | K_OPTIONAL
    | K_ORDER
    | K_ORDERU
    | K_OR
    | K_OTHERV
    | K_OUT
    | K_OUTU
    | K_OUTE
    | K_OUTSIDE
    | K_OUTV
    | K_P
    | K_PAGERANK
    | K_PAGERANKU
    | K_PATH
    | K_PEERPRESSURE
    | K_PEERPRESSUREU
    | K_PICK
    | K_POP
    | K_PROFILE
    | K_PROJECT
    | K_PROPERTIES
    | K_PROPERTYMAP
    | K_PROPERTYNAME
    | K_PROPERTY
    | K_PRODUCT
    | K_RANGE
    | K_READ
    | K_READER
    | K_REFERENCEVERTEX
    | K_REGEX
    | K_REPLACE
    | K_REPEAT
    | K_REVERSE
    | K_ROLLBACK
    | K_RTRIM
    | K_SACK
    | K_SAMPLE
    | K_SCOPE
    | K_SECOND
    | K_SELECT
    | K_SET
    | K_SHORTESTPATH
    | K_SHORTESTPATHU
    | K_SHUFFLE
    | K_SIDEEFFECT
    | K_SIMPLEPATH
    | K_SINGLE
    | K_SKIP
    | K_SPLIT
    | K_STARTINGWITH
    | K_STORE
    | K_SUBGRAPH
    | K_SUBSTRING
    | K_SUM
    | K_SUMLONG
    | K_T
    | K_TAIL
    | K_TARGET
    | K_TEXTP
    | K_TIMELIMIT
    | K_TIMES
    | K_TO
    | K_TOBULKSET
    | K_TOKENS
    | K_TOLIST
    | K_TOLOWER
    | K_TOSET
    | K_TOSTRING
    | K_TOUPPER
    | K_TOE
    | K_TOV
    | K_TREE
    | K_TRIM
    | K_TRUE
    | K_TRYNEXT
    | K_TX
    | K_UNFOLD
    | K_UNION
    | K_UNTIL
    | K_UUID
    | K_V
    | K_VALUEMAP
    | K_VALUES
    | K_VALUE
    | K_VERTEX
    | K_WHERE
    | K_WITH
    | K_WITHBULK
    | K_WITHIN
    | K_WITHOPTOPTIONS
    | K_WITHOUT
    | K_WITHOUTSTRATEGIES
    | K_WITHPATH
    | K_WITHSACK
    | K_WITHSIDEEFFECT
    | K_WITHSTRATEGIES
    | K_WRITE
    | K_WRITER
    ;

/*********************************************
    LEXER RULES
**********************************************/

// Lexer rules
// These rules are extracted from Java ANTLRv4 Grammar.
// Source: https://github.com/antlr/grammars-v4/blob/master/java8/Java8.g4

// §3.9 Keywords

K_ADDALL: 'addAll';
K_ADDE: 'addE';
K_ADDV: 'addV';
K_AGGREGATE: 'aggregate';
K_ALL: 'all';
K_AND: 'and';
K_ANY: 'any';
K_AS: 'as';
K_ASC: 'asc';
K_ASDATE: 'asDate';
K_ASSTRING: 'asString';
K_ASSIGN: 'assign';
K_BARRIER: 'barrier';
K_BARRIERU: 'Barrier';
K_BEGIN: 'begin';
K_BETWEEN: 'between';
K_BOTH: 'both';
K_BOTHU: 'BOTH';
K_BOTHE: 'bothE';
K_BOTHV: 'bothV';
K_BRANCH: 'branch';
K_BY: 'by';
K_CALL: 'call';
K_CAP: 'cap';
K_CARDINALITY: 'Cardinality';
K_CHOOSE: 'choose';
K_COALESCE: 'coalesce';
K_COIN: 'coin';
K_COLUMN: 'Column';
K_COMBINE: 'combine';
K_COMMIT: 'commit';
K_COMPONENT: 'component';
K_CONCAT: 'concat';
K_CONJOIN: 'conjoin';
K_CONNECTEDCOMPONENT: 'connectedComponent';
K_CONNECTEDCOMPONENTU: 'ConnectedComponent';
K_CONSTANT: 'constant';
K_CONTAINING: 'containing';
K_COUNT: 'count';
K_CYCLICPATH: 'cyclicPath';
K_DAY: 'day';
K_DATEADD: 'dateAdd';
K_DATEDIFF: 'dateDiff';
K_DATETIME: 'datetime';
K_DATETIMEU: 'DateTime';
K_DECR: 'decr';
K_DEDUP: 'dedup';
K_DESC: 'desc';
K_DIFFERENCE: 'difference';
K_DIRECTION: 'Direction';
K_DISJUNCT: 'disjunct';
K_DISTANCE: 'distance';
K_DIV: 'div';
K_DROP: 'drop';
K_DT: 'DT';
K_E: 'E';
K_EDGES: 'edges';
K_ELEMENTMAP: 'elementMap';
K_ELEMENT: 'element';
K_EMIT: 'emit';
K_ENDINGWITH: 'endingWith';
K_EQ: 'eq';
K_EXPLAIN: 'explain';
K_FAIL: 'fail';
K_FALSE: 'false';
K_FILTER: 'filter';
K_FIRST: 'first';
K_FLATMAP: 'flatMap';
K_FOLD: 'fold';
K_FORMAT: 'format';
K_FROM: 'from';
K_GLOBAL: 'global';
K_GT: 'gt';
K_GTE: 'gte';
K_GRAPHML: 'graphml';
K_GRAPHSON: 'graphson';
K_GROUPCOUNT: 'groupCount';
K_GROUP: 'group';
K_GRYO: 'gryo';
K_HAS: 'has';
K_HASID: 'hasId';
K_HASKEY: 'hasKey';
K_HASLABEL: 'hasLabel';
K_HASNEXT: 'hasNext';
K_HASNOT: 'hasNot';
K_HASVALUE: 'hasValue';
K_HOUR: 'hour';
K_ID: 'id';
K_IDENTITY: 'identity';
K_IDS: 'ids';
K_IN: 'in';
K_INU: 'IN';
K_INE: 'inE';
K_INCLUDEEDGES: 'includeEdges';
K_INCR: 'incr';
K_INDEXER: 'indexer';
K_INDEX: 'index';
K_INFINITY: 'Infinity';
K_INJECT: 'inject';
K_INSIDE: 'inside';
K_INTERSECT: 'intersect';
K_INV: 'inV';
K_IOU: 'IO';
K_IO: 'io';
K_IS: 'is';
K_ITERATE: 'iterate';
K_KEY: 'key';
K_KEYS: 'keys';
K_LABELS: 'labels';
K_LABEL: 'label';
K_LAST: 'last';
K_LENGTH: 'length';
K_LIMIT: 'limit';
K_LIST: 'list';
K_LOCAL: 'local';
K_LOOPS: 'loops';
K_LT: 'lt';
K_LTE: 'lte';
K_LTRIM: 'lTrim';
K_MAP: 'map';
K_MATCH: 'match';
K_MATH: 'math';
K_MAX: 'max';
K_MAXDISTANCE: 'maxDistance';
K_MEAN: 'mean';
K_MERGEU: 'Merge';
K_MERGE: 'merge';
K_MERGEE: 'mergeE';
K_MERGEV: 'mergeV';
K_MIN: 'min';
K_MINUTE: 'minute';
K_MINUS: 'minus';
K_MIXED: 'mixed';
K_MULT: 'mult';
K_NAN: 'NaN';
K_NEGATE: 'negate';
K_NEXT: 'next';
K_NONE: 'none';
K_NOTREGEX: 'notRegex';
K_NOTCONTAINING: 'notContaining';
K_NOTENDINGWITH: 'notEndingWith';
K_NOTSTARTINGWITH: 'notStartingWith';
K_NOT: 'not';
K_NEQ: 'neq';
K_NEW: 'new';
K_NORMSACK: 'normSack';
K_NULL: 'null';
K_ONCREATE: 'onCreate';
K_ONMATCH: 'onMatch';
K_OPERATOR: 'Operator';
K_OPTION: 'option';
K_OPTIONAL: 'optional';
K_ORDERU: 'Order';
K_ORDER: 'order';
K_OR: 'or';
K_OTHERV: 'otherV';
K_OUTU: 'OUT';
K_OUT: 'out';
K_OUTE: 'outE';
K_OUTSIDE: 'outside';
K_OUTV: 'outV';
K_P: 'P';
K_PAGERANKU: 'PageRank';
K_PAGERANK: 'pageRank';
K_PATH: 'path';
K_PEERPRESSUREU: 'PeerPressure';
K_PEERPRESSURE: 'peerPressure';
K_PICK: 'Pick';
K_POP: 'Pop';
K_PROFILE: 'profile';
K_PROJECT: 'project';
K_PROPERTIES: 'properties';
K_PROPERTYMAP: 'propertyMap';
K_PROPERTYNAME: 'propertyName';
K_PROPERTY: 'property';
K_PRODUCT: 'product';
K_RANGE: 'range';
K_READ: 'read';
K_READER: 'reader';
K_REFERENCEVERTEX: 'ReferenceVertex';
K_REGEX: 'regex';
K_REPLACE: 'replace';
K_REPEAT: 'repeat';
K_REVERSE: 'reverse';
K_ROLLBACK: 'rollback';
K_RTRIM: 'rTrim';
K_SACK: 'sack';
K_SAMPLE: 'sample';
K_SCOPE: 'Scope';
K_SECOND: 'second';
K_SELECT: 'select';
K_SET: 'set';
K_SHORTESTPATHU: 'ShortestPath';
K_SHORTESTPATH: 'shortestPath';
K_SHUFFLE: 'shuffle';
K_SIDEEFFECT: 'sideEffect';
K_SIMPLEPATH: 'simplePath';
K_SINGLE: 'single';
K_SKIP: 'skip';
K_SPLIT: 'split';
K_STARTINGWITH: 'startingWith';
K_STORE: 'store';
K_SUBGRAPH: 'subgraph';
K_SUBSTRING: 'substring';
K_SUM: 'sum';
K_SUMLONG: 'sumLong';
K_T: 'T';
K_TAIL: 'tail';
K_TARGET: 'target';
K_TEXTP: 'TextP';
K_TIMELIMIT: 'timeLimit';
K_TIMES: 'times';
K_TO: 'to';
K_TOBULKSET: 'toBulkSet';
K_TOKENS: 'tokens';
K_TOLIST: 'toList';
K_TOLOWER: 'toLower';
K_TOSET: 'toSet';
K_TOSTRING: 'toString';
K_TOUPPER: 'toUpper';
K_TOE: 'toE';
K_TOV: 'toV';
K_TREE: 'tree';
K_TRIM: 'trim';
K_TRUE: 'true';
K_TRYNEXT: 'tryNext';
K_TX: 'tx';
K_UNFOLD: 'unfold';
K_UNION: 'union';
K_UNTIL: 'until';
K_UUID: 'UUID';
K_V: 'V';
K_VALUEMAP: 'valueMap';
K_VALUES: 'values';
K_VALUE: 'value';
K_VERTEX: 'Vertex';
K_WHERE: 'where';
K_WITH: 'with';
K_WITHBULK: 'withBulk';
K_WITHIN: 'within';
K_WITHOPTOPTIONS: 'WithOptions';
K_WITHOUT: 'without';
K_WITHOUTSTRATEGIES: 'withoutStrategies';
K_WITHPATH: 'withPath';
K_WITHSACK: 'withSack';
K_WITHSIDEEFFECT: 'withSideEffect';
K_WITHSTRATEGIES: 'withStrategies';
K_WRITE: 'write';
K_WRITER: 'writer';

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

SignedInfLiteral
    : Sign? K_INFINITY
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

