# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

@StepClassMap @StepMergeE
Feature: Step - mergeE()

  # TEST INDEX
  # Use of inject() is meant to exercise mergeV() in a mid-traversal form rather than start step.
  # When the test name places a "1" following a "name" it just means that the test is using the id rather
  # than vertex reference.
  #
  # g_mergeEXlabel_knows_out_marko_in_vadasX
  # g_mergeEXlabel_knows_out_marko1_in_vadas1X
  # g_mergeEXlabel_knows_out_marko_in_vadasX_aliased_direction
  #   - mergeE(Map) specifying out/in for vertices in the match/create with no option()
  #   - vertices already exist
  #   - results in new edge
  # g_withSideEffectXa_label_knows_out_marko_in_vadasX_mergeEXselectXaXX
  #   - mergeE(Traversal) specifying out/in for vertices in the match/create with no option()
  #   - vertices already exist
  #   - results in new edge
  # g_mergeEXlabel_knows_out_marko_in_vadas_weight_05X_exists
  #   - mergeE(Map) specifying out/in for vertices in the match/create with no option()
  #   - vertices already exist as does an edge between them but the search will not match because of a missing property
  #   - results in new edge
  # g_mergeEXlabel_knows_out_marko_in_vadas_weight_05X
  #   - mergeE(Map) specifying out/in for vertices in the match/create with no option()
  #   - no vertices/edges in graph
  #   - results in error as vertices don't exist
  # g_mergeEXlabel_knows_out_marko_in_vadasX_optionXonCreate_created_YX_optionXonMatch_created_NX
  #   - mergeE(Map) specifying out/in for vertices in the match/create with option(Map)
  #   - no vertices/edges in graph
  #   - results in error as vertices don't exist
  # g_mergeEXlabel_knows_out_marko_in_vadasX_optionXonCreate_created_YX_optionXonMatch_created_NX_exists
  #   - mergeE(Map) specifying out/in for vertices in the match/create with option(Map)
  #   - vertices and edge already exist
  #   - results in the existing edge getting a new property
  # g_mergeEXlabel_knows_out_marko_in_vadasX_optionXonCreate_created_YX_optionXonMatch_created_NX_exists_updated
  #   - mergeE(Map) specifying out/in for vertices in the match/create with option(Map)
  #   - vertices and edge already exist
  #   - results in the existing edge getting an existing property updated
  # g_V_hasXperson_name_marko_X_mergeEXlabel_knowsX_optionXonCreate_created_YX_optionXonMatch_created_NX_exists_updated
  #   - mergeE(Map) using the vertex of the current traverser as reference and no out/in for match/create with option(Map)
  #   - vertices and two edges of the same label already exist with one edge having an existing property
  #   - results in one edge getting an existing property updated and the other getting a new property added
  # g_injectXlabel_knows_out_marko_in_vadasX_mergeE
  #   - mergeE() using Map from current traverser in the match/create with no option()
  #   - vertices already exist
  #   - results in a new edge
  # g_mergeEXlabel_knows_in_vadasX_optionXonCreate_created_YX_optionXonMatch_created_NX_exists_updated
  #   - mergeE(Map) specifying in vertex in the match/create with option(Map)
  #   - vertices exist and there are two edges such that only one will match on the out vertex
  #   - results in updating the property on the one matched edge
  # g_mergeEXout_vadasX_optionXonCreate_created_YX_optionXonMatch_created_NX_exists_updated
  #   - mergeE(Map) specifying no label and in vertex in the match/create with option(Map)
  #   - vertices exist and there are two edges such that only one will match on the out vertex
  #   - results in updating the property on the one matched edge
  # g_V_hasXperson_name_marko_X_mergeEXlabel_self_out_vadas1_in_vadas1X
  #   - mergeE(Map) specifying label and in/out vertex in the match/create with option(Map)
  #   - vertices exist with no edge and the match/create map overrides the marko vertex in the traverser
  #   - results in a new edge
  # g_withSideEffectXc_created_YX_withSideEffectXm_matchedX_mergeEXlabel_knows_out_marko_in_vadasX_optionXonCreate_selectXcXX_optionXonMatch_selectXmXX
  #   - mergeE(Map) specifying out/in for vertices in the match/create with option(Map)
  #   - vertices exist in the graph
  #   - results in one new edge
  # g_withSideEffectXc_created_YX_withSideEffectXm_matchedX_mergeEXlabel_knows_out_marko_in_vadasX_optionXonCreate_selectXcXX_optionXonMatch_selectXmXX_exists
  #   - mergeE(Map) specifying out/in for vertices in the match/create with option(Map)
  #   - vertices and edge already exist
  #   - results in the existing edge getting a new property
  # g_injectXlabel_knows_out_marko_in_vadas_label_self_out_vadas_in_vadasX
  #   - mergeE() using the map of current traverser as reference
  #   - vertices already exists
  #   - results in two new edges
  # g_mergeEXnullX
  # g_V_mergeEXnullX
  #   - mergeE(null) and no option
  #   - vertices already exists
  #   - Directions not specified - error
  # g_mergeEXemptyX
  #   - mergeE(empty) and no option
  #   - vertex present and no edges
  #   - results in error because empty map matches all edges and there are none so it creates but doesn't have a Vertex reference to use to create the edge
  # g_V_mergeEXemptyX
  #   - mergeE(empty) and no option
  #   - two vertices exist
  #   - results in one edge added to the first vertex and then the same edge being returned
  # g_mergeEXemptyX
  #   - mergeE(empty) and no option
  #   - vertices already exists
  #   - results in no matched edges and no new edge
  # g_mergeEXemptyX_exists
  #   - mergeE(empty) and no option
  #   - vertex with self-edge already exists
  #   - results in the matched self-edge
  # g_V_mergeEXemptyX_two_exist
  #   - mergeE(empty) and no option
  #   - two vertices exist
  #   - results in two new self edges, one for each vertex
  # g_V_mergeEXemptyX_optionXonCreate_nullX
  #   - mergeE(empty) with onCreate(null)
  #   - one existing vertex
  #   - results in no new edges because onCreate() was null
  # g_V_mergeEXlabel_selfX_optionXonMatch_nullX
  #   - mergeE(Map) with onMatch(null)
  #   - one existing vertex and one edge
  #   - results in one matched edge with no updates
  # g_V_mergeEXlabel_selfX_optionXonMatch_emptyX
  #   - mergeE(Map) with onMatch(null)
  #   - one existing vertex and one edge
  #   - results in one matched edge with no updates
  # g_withSideEffectXc_created_YX_withSideEffectXm_matchedX_mergeEXlabel_knows_out_marko_in_vadasX_optionXonCreate_selectXcXX_optionXonMatch_sideEffectXpropertiesXweightX_dropX_selectXmXX_exists
  #   - mergeE(Map) with onMatch(Map)
  #   - matches an edge
  #   - results in matched edge being updated with a side-effect that drops a property that is not updated in the Map

  Scenario: g_V_mergeEXlabel_selfX_optionXonMatch_emptyX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", "marko").property("age", 29).
        addE("self")
      """
    And using the parameter xx1 defined as "m[{\"t[label]\": \"self\"}]"
    And the traversal of
      """
      g.V().mergeE(xx1).option(Merge.onMatch,[:])
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 1 for count of "g.E()"
    And the graph should return 0 for count of "g.E().properties()"
    And the graph should return 1 for count of "g.V()"

  Scenario: g_V_mergeEXlabel_selfX_optionXonMatch_nullX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", "marko").property("age", 29).
        addE("self")
      """
    And using the parameter xx1 defined as "m[{\"t[label]\": \"self\"}]"
    And the traversal of
      """
      g.V().mergeE(xx1).option(Merge.onMatch,null)
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 1 for count of "g.E()"
    And the graph should return 0 for count of "g.E().properties()"
    And the graph should return 1 for count of "g.V()"

  Scenario: g_V_mergeEXemptyX_optionXonCreate_nullX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", "marko").property("age", 29)
      """
    And using the parameter xx1 defined as "m[{\"t[label]\": \"self\", \"D[OUT]\":\"M[outV]\", \"D[IN]\":\"M[inV]\"}]"
    And the traversal of
      """
      g.V().as("v").mergeE(xx1).option(Merge.onCreate,null).option(Merge.outV,select("v")).option(Merge.inV,select("v"))
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 1 for count of "g.E()"
    And the graph should return 1 for count of "g.V()"

  Scenario: g_mergeEXemptyX_exists
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", "marko").property("age", 29).
        addE("self")
      """
    And the traversal of
      """
      g.mergeE([:])
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 1 for count of "g.E()"
    And the graph should return 1 for count of "g.V()"

  Scenario: g_mergeEXemptyX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", "marko").property("age", 29)
      """
    And the traversal of
      """
      g.mergeE([:])
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "Out Vertex not specified in onCreate - edge cannot be created"

  Scenario: g_V_mergeEXemptyX_two_exist
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", "marko").property("age", 29).
        addV("person").property("name", "vadas").property("age", 27)
      """
    And using the parameter xx1 defined as "m[{\"t[label]\": \"self\", \"D[OUT]\":\"M[outV]\", \"D[IN]\":\"M[inV]\"}]"
    And the traversal of
      """
      g.V().as("v").mergeE(xx1).option(Merge.outV,select("v")).option(Merge.inV,select("v"))
      """
    When iterated to list
    Then the result should have a count of 2
    And the graph should return 2 for count of "g.E()"
    And the graph should return 2 for count of "g.V()"

  # null same as empty
  Scenario: g_mergeEXnullX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", "marko").property("age", 29)
      """
    And the traversal of
      """
      g.mergeE(null)
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "Out Vertex not specified in onCreate - edge cannot be created"

  # Directions not specified
  Scenario: g_V_mergeEXnullX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", "marko").property("age", 29)
      """
    And the traversal of
      """
      g.V().mergeE(null)
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "Out Vertex not specified in onCreate - edge cannot be created"

  @UserSuppliedVertexIds
  Scenario: g_mergeEXlabel_knows_out_marko_in_vadasX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property(T.id, 100).property("name", "marko").
        addV("person").property(T.id, 101).property("name", "vadas")
      """
    And using the parameter xx1 defined as "m[{\"t[label]\": \"knows\", \"D[OUT]\":\"v[100]\", \"D[IN]\":\"v[101]\"}]"
    And the traversal of
      """
      g.mergeE(xx1)
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 1 for count of "g.V().has(\"person\",\"name\",\"marko\").out(\"knows\").has(\"person\",\"name\",\"vadas\")"

  @UserSuppliedVertexIds
  Scenario: g_withSideEffectXa_label_knows_out_marko_in_vadasX_mergeEXselectXaXX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property(T.id, 100).property("name", "marko").
        addV("person").property(T.id, 101).property("name", "vadas")
      """
    And using the parameter xx1 defined as "m[{\"t[label]\": \"knows\", \"D[OUT]\":\"v[100]\", \"D[IN]\":\"v[101]\"}]"
    And the traversal of
      """
      g.withSideEffect("a",xx1).mergeE(__.select("a"))
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 1 for count of "g.V().has(\"person\",\"name\",\"marko\").out(\"knows\").has(\"person\",\"name\",\"vadas\")"

  @UserSuppliedVertexIds
  Scenario: g_mergeEXlabel_knows_out_marko1_in_vadas1X
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property(T.id, 100).property("name", "marko").
        addV("person").property(T.id, 101).property("name", "vadas")
      """
    And using the parameter xx1 defined as "m[{\"t[label]\": \"knows\", \"D[OUT]\":\"d[100].i\", \"D[IN]\":\"d[101].i\"}]"
    And the traversal of
      """
      g.mergeE(xx1)
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 1 for count of "g.V().has(\"person\",\"name\",\"marko\").out(\"knows\").has(\"person\",\"name\",\"vadas\")"

  @UserSuppliedVertexIds
  Scenario: g_mergeEXlabel_knows_out_marko_in_vadas_weight_05X_exists
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property(T.id, 100).property("name", "marko").as("a").
        addV("person").property(T.id, 101).property("name", "vadas").as("b").
        addE("knows").from("a").to("b")
      """
    And using the parameter xx1 defined as "m[{\"t[label]\": \"knows\", \"D[OUT]\":\"v[100]\", \"D[IN]\":\"v[101]\", \"weight\":\"d[0.5].d\"}]"
    And the traversal of
      """
      g.mergeE(xx1)
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 1 for count of "g.V().has(\"person\",\"name\",\"marko\").outE(\"knows\").has(\"weight\",0.5).inV().has(\"person\",\"name\",\"vadas\")"
    And the graph should return 2 for count of "g.V().has(\"person\",\"name\",\"marko\").out(\"knows\").has(\"person\",\"name\",\"vadas\")"

  @UserSuppliedVertexIds
  Scenario: g_mergeEXlabel_knows_out_marko_in_vadas_weight_05X
    Given the empty graph
    And using the parameter xx1 defined as "m[{\"t[label]\": \"knows\", \"D[OUT]\":\"v[100]\", \"D[IN]\":\"v[101]\", \"weight\":\"d[0.5].d\"}]"
    And the traversal of
      """
      g.mergeE(xx1)
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "Vertex id could not be resolved from mergeE"

  @UserSuppliedVertexIds
  Scenario: g_mergeEXlabel_knows_out_marko_in_vadasX_optionXonCreate_created_YX_optionXonMatch_created_NX
    Given the empty graph
    And using the parameter xx1 defined as "m[{\"t[label]\": \"knows\", \"D[OUT]\":\"v[100]\", \"D[IN]\":\"v[101]\"}]"
    And using the parameter xx2 defined as "m[{\"t[label]\": \"knows\", \"D[OUT]\":\"v[100]\", \"D[IN]\":\"v[101]\", \"created\":\"Y\"}]"
    And using the parameter xx3 defined as "m[{\"created\":\"N\"}]"
    And the traversal of
      """
      g.mergeE(xx1).option(Merge.onCreate,xx2).option(Merge.onMatch,xx3)
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "Vertex id could not be resolved from mergeE"

  @UserSuppliedVertexIds
  Scenario: g_mergeEXlabel_knows_out_marko_in_vadasX_optionXonCreate_created_YX_optionXonMatch_created_NX_exists
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property(T.id, 100).property("name", "marko").as("a").
        addV("person").property(T.id, 101).property("name", "vadas").as("b").
        addE("knows").from("a").to("b")
      """
    And using the parameter xx1 defined as "m[{\"t[label]\": \"knows\", \"D[OUT]\":\"v[100]\", \"D[IN]\":\"v[101]\"}]"
    And using the parameter xx2 defined as "m[{\"t[label]\": \"knows\", \"D[OUT]\":\"v[100]\", \"D[IN]\":\"v[101]\",\"created\":\"Y\"}]"
    And using the parameter xx3 defined as "m[{\"created\":\"N\"}]"
    And the traversal of
      """
      g.mergeE(xx1).option(Merge.onCreate,xx2).option(Merge.onMatch,xx3)
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 2 for count of "g.V()"
    And the graph should return 0 for count of "g.E().hasLabel(\"knows\").has(\"created\",\"Y\")"
    And the graph should return 1 for count of "g.E().hasLabel(\"knows\").has(\"created\",\"N\")"

  @UserSuppliedVertexIds
  Scenario: g_mergeEXlabel_knows_out_marko_in_vadasX_optionXonCreate_created_YX_optionXonMatch_created_NX_exists_updated
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property(T.id, 100).property("name", "marko").as("a").
        addV("person").property(T.id, 101).property("name", "vadas").as("b").
        addE("knows").from("a").to("b").property("created","Y")
      """
    And using the parameter xx1 defined as "m[{\"t[label]\": \"knows\", \"D[OUT]\":\"v[100]\", \"D[IN]\":\"v[101]\"}]"
    And using the parameter xx2 defined as "m[{\"t[label]\": \"knows\", \"D[OUT]\":\"v[100]\", \"D[IN]\":\"v[101]\",\"created\":\"Y\"}]"
    And using the parameter xx3 defined as "m[{\"created\":\"N\"}]"
    And the traversal of
      """
      g.mergeE(xx1).option(Merge.onCreate,xx2).option(Merge.onMatch,xx3)
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 2 for count of "g.V()"
    And the graph should return 0 for count of "g.E().hasLabel(\"knows\").has(\"created\",\"Y\")"
    And the graph should return 1 for count of "g.E().hasLabel(\"knows\").has(\"created\",\"N\")"

  @UserSuppliedVertexIds
  Scenario: g_V_hasXperson_name_marko_X_mergeEXlabel_knowsX_optionXonCreate_created_YX_optionXonMatch_created_NX_exists_updated
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property(T.id, 100).property("name", "marko").as("a").
        addV("person").property(T.id, 101).property("name", "vadas").as("b").
        addE("knows").from("a").to("b").property("created","Y").
        addE("knows").from("a").to("b")
      """
    And using the parameter xx1 defined as "m[{\"t[label]\": \"knows\"}]"
    And using the parameter xx2 defined as "m[{\"t[label]\": \"knows\", \"D[OUT]\":\"v[100]\", \"D[IN]\":\"v[101]\",\"created\":\"Y\"}]"
    And using the parameter xx3 defined as "m[{\"created\":\"N\"}]"
    And the traversal of
      """
      g.V().has("person","name","marko").mergeE(xx1).option(Merge.onCreate,xx2).option(Merge.onMatch,xx3)
      """
    When iterated to list
    Then the result should have a count of 2
    And the graph should return 2 for count of "g.V()"
    And the graph should return 2 for count of "g.E()"
    And the graph should return 0 for count of "g.E().hasLabel(\"knows\").has(\"created\",\"Y\")"
    And the graph should return 2 for count of "g.E().hasLabel(\"knows\").has(\"created\",\"N\")"

  @UserSuppliedVertexIds
  Scenario: g_injectXlabel_knows_out_marko_in_vadasX_mergeE
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property(T.id, 100).property("name", "marko").
        addV("person").property(T.id, 101).property("name", "vadas")
      """
    And using the parameter xx1 defined as "m[{\"t[label]\": \"knows\", \"D[OUT]\":\"v[100]\", \"D[IN]\":\"v[101]\"}]"
    And the traversal of
      """
      g.inject(xx1).mergeE()
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 1 for count of "g.V().has(\"person\",\"name\",\"marko\").out(\"knows\").has(\"person\",\"name\",\"vadas\")"

  @UserSuppliedVertexIds
  Scenario: g_mergeEXlabel_knows_in_vadasX_optionXonCreate_created_YX_optionXonMatch_created_NX_exists_updated
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property(T.id, 100).property("name", "marko").as("a").
        addV("person").property(T.id, 101).property("name", "vadas").as("b").
        addE("knows").from("a").to("b").property("created","Y").
        addE("knows").from("b").to("a").property("created","Y")
      """
    And using the parameter xx1 defined as "m[{\"t[label]\": \"knows\", \"D[IN]\":\"v[101]\"}]"
    And using the parameter xx2 defined as "m[{\"t[label]\": \"knows\", \"D[OUT]\":\"v[100]\", \"D[IN]\":\"v[101]\",\"created\":\"Y\"}]"
    And using the parameter xx3 defined as "m[{\"created\":\"N\"}]"
    And the traversal of
      """
      g.mergeE(xx1).option(Merge.onCreate,xx2).option(Merge.onMatch,xx3)
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 2 for count of "g.V()"
    And the graph should return 2 for count of "g.E()"
    And the graph should return 1 for count of "g.E().hasLabel(\"knows\").has(\"created\",\"Y\")"
    And the graph should return 1 for count of "g.E().hasLabel(\"knows\").has(\"created\",\"N\").inV().has(\"name\",\"vadas\")"

  @UserSuppliedVertexIds
  Scenario: g_mergeEXlabel_knows_out_vadasX_optionXonCreate_created_YX_optionXonMatch_created_NX_exists_updated
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property(T.id, 100).property("name", "marko").as("a").
        addV("person").property(T.id, 101).property("name", "vadas").as("b").
        addE("knows").from("a").to("b").property("created","Y").
        addE("knows").from("b").to("a").property("created","Y")
      """
    And using the parameter xx1 defined as "m[{\"t[label]\": \"knows\", \"D[OUT]\":\"v[101]\"}]"
    And using the parameter xx2 defined as "m[{\"t[label]\": \"knows\", \"D[OUT]\":\"v[100]\", \"D[IN]\":\"v[101]\",\"created\":\"Y\"}]"
    And using the parameter xx3 defined as "m[{\"created\":\"N\"}]"
    And the traversal of
      """
      g.mergeE(xx1).option(Merge.onCreate,xx2).option(Merge.onMatch,xx3)
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 2 for count of "g.V()"
    And the graph should return 2 for count of "g.E()"
    And the graph should return 1 for count of "g.E().hasLabel(\"knows\").has(\"created\",\"Y\")"
    And the graph should return 1 for count of "g.E().hasLabel(\"knows\").has(\"created\",\"N\").outV().has(\"name\",\"vadas\")"

  @UserSuppliedVertexIds
  Scenario: g_mergeEXout_vadasX_optionXonCreate_created_YX_optionXonMatch_created_NX_exists_updated
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property(T.id, 100).property("name", "marko").as("a").
        addV("person").property(T.id, 101).property("name", "vadas").as("b").
        addE("knows").from("a").to("b").property("created","Y").
        addE("knows").from("b").to("a").property("created","Y")
      """
    And using the parameter xx1 defined as "m[{\"D[OUT]\":\"v[101]\"}]"
    And using the parameter xx2 defined as "m[{\"t[label]\": \"knows\", \"D[OUT]\":\"v[100]\", \"D[IN]\":\"v[101]\",\"created\":\"Y\"}]"
    And using the parameter xx3 defined as "m[{\"created\":\"N\"}]"
    And the traversal of
      """
      g.mergeE(xx1).option(Merge.onCreate,xx2).option(Merge.onMatch,xx3)
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 2 for count of "g.V()"
    And the graph should return 2 for count of "g.E()"
    And the graph should return 1 for count of "g.E().hasLabel(\"knows\").has(\"created\",\"Y\")"
    And the graph should return 1 for count of "g.E().hasLabel(\"knows\").has(\"created\",\"N\").outV().has(\"name\",\"vadas\")"

  @UserSuppliedVertexIds
  Scenario: g_V_hasXperson_name_marko_X_mergeEXlabel_self_out_vadas1_in_vadas1X
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property(T.id, 100).property("name", "marko").
        addV("person").property(T.id, 101).property("name", "vadas")
      """
    And using the parameter xx1 defined as "m[{\"t[label]\": \"self\", \"D[OUT]\":\"d[101].i\", \"D[IN]\":\"d[101].i\"}]"
    And the traversal of
      """
      g.V().has("person","name","marko").mergeE(xx1)
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 2 for count of "g.V()"
    And the graph should return 1 for count of "g.E()"
    And the graph should return 2 for count of "g.E().hasLabel(\"self\").bothV().has(\"name\",\"vadas\")"

  @UserSuppliedVertexIds
  Scenario: g_withSideEffectXc_created_YX_withSideEffectXm_matchedX_mergeEXlabel_knows_out_marko_in_vadasX_optionXonCreate_selectXcXX_optionXonMatch_selectXmXX_exists
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property(T.id, 100).property("name", "marko").as("a").
        addV("person").property(T.id, 101).property("name", "vadas").as("b").
        addE("knows").from("a").to("b")
      """
    And using the parameter xx1 defined as "m[{\"t[label]\": \"knows\", \"D[OUT]\":\"v[100]\", \"D[IN]\":\"v[101]\"}]"
    And using the parameter xx2 defined as "m[{\"t[label]\": \"knows\", \"D[OUT]\":\"v[100]\", \"D[IN]\":\"v[101]\",\"created\":\"Y\"}]"
    And using the parameter xx3 defined as "m[{\"created\":\"N\"}]"
    And the traversal of
      """
      g.withSideEffect("c",xx2).withSideEffect("m",xx3).
        mergeE(xx1).option(Merge.onCreate,__.select("c")).option(Merge.onMatch,__.select("m"))
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 2 for count of "g.V()"
    And the graph should return 0 for count of "g.E().hasLabel(\"knows\").has(\"created\",\"Y\")"
    And the graph should return 1 for count of "g.E().hasLabel(\"knows\").has(\"created\",\"N\")"

  @UserSuppliedVertexIds
  Scenario: g_withSideEffectXc_created_YX_withSideEffectXm_matchedX_mergeEXlabel_knows_out_marko_in_vadasX_optionXonCreate_selectXcXX_optionXonMatch_selectXmXX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property(T.id, 100).property("name", "marko").as("a").
        addV("person").property(T.id, 101).property("name", "vadas").as("b")
      """
    And using the parameter xx1 defined as "m[{\"t[label]\": \"knows\", \"D[OUT]\":\"v[100]\", \"D[IN]\":\"v[101]\"}]"
    And using the parameter xx2 defined as "m[{\"t[label]\": \"knows\", \"D[OUT]\":\"v[100]\", \"D[IN]\":\"v[101]\",\"created\":\"Y\"}]"
    And using the parameter xx3 defined as "m[{\"created\":\"N\"}]"
    And the traversal of
      """
      g.withSideEffect("c",xx2).withSideEffect("m",xx3).
        mergeE(xx1).option(Merge.onCreate, __.select("c")).option(Merge.onMatch, __.select("m"))
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 2 for count of "g.V()"
    And the graph should return 1 for count of "g.E()"
    And the graph should return 1 for count of "g.E().hasLabel(\"knows\").has(\"created\",\"Y\")"
    And the graph should return 0 for count of "g.E().hasLabel(\"knows\").has(\"created\",\"N\")"

  @UserSuppliedVertexIds
  Scenario: g_withSideEffectXc_created_YX_withSideEffectXm_matchedX_mergeEXlabel_knows_out_marko1_in_vadas1X_optionXonCreate_selectXcXX_optionXonMatch_selectXmXX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property(T.id, 100).property("name", "marko").as("a").
        addV("person").property(T.id, 101).property("name", "vadas").as("b")
      """
    And using the parameter xx1 defined as "m[{\"t[label]\": \"knows\", \"D[OUT]\":\"d[100].i\", \"D[IN]\":\"d[101].i\"}]"
    And using the parameter xx2 defined as "m[{\"t[label]\": \"knows\", \"D[OUT]\":\"d[100].i\", \"D[IN]\":\"d[101].i\",\"created\":\"Y\"}]"
    And using the parameter xx3 defined as "m[{\"created\":\"N\"}]"
    And the traversal of
      """
      g.withSideEffect("c",xx2).withSideEffect("m",xx3).
        mergeE(xx1).option(Merge.onCreate, __.select("c")).option(Merge.onMatch, __.select("m"))
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 2 for count of "g.V()"
    And the graph should return 1 for count of "g.E()"
    And the graph should return 1 for count of "g.E().hasLabel(\"knows\").has(\"created\",\"Y\")"
    And the graph should return 0 for count of "g.E().hasLabel(\"knows\").has(\"created\",\"N\")"

  @UserSuppliedVertexIds
  Scenario: g_mergeEXlabel_knows_out_marko_in_vadasX_aliased_direction
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property(T.id, 100).property("name", "marko").
        addV("person").property(T.id, 101).property("name", "vadas")
      """
    And using the parameter xx1 defined as "m[{\"t[label]\": \"knows\", \"D[from]\":\"v[100]\", \"D[to]\":\"v[101]\"}]"
    And the traversal of
      """
      g.mergeE(xx1)
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 1 for count of "g.V().has(\"person\",\"name\",\"marko\").out(\"knows\").has(\"person\",\"name\",\"vadas\")"

  @UserSuppliedVertexIds
  Scenario: g_injectXlabel_knows_out_marko_in_vadas_label_self_out_vadas_in_vadasX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property(T.id, 100).property("name", "marko").
        addV("person").property(T.id, 101).property("name", "vadas")
      """
    And using the parameter xx1 defined as "m[{\"t[label]\": \"knows\", \"D[OUT]\":\"v[100]\", \"D[IN]\":\"v[101]\"}]"
    And using the parameter xx2 defined as "m[{\"t[label]\": \"self\", \"D[OUT]\":\"v[101]\", \"D[IN]\":\"v[101]\"}]"
    And the traversal of
      """
      g.inject(xx1, xx2).mergeE()
      """
    When iterated to list
    Then the result should have a count of 2
    And the graph should return 2 for count of "g.V()"
    And the graph should return 2 for count of "g.E()"
    And the graph should return 1 for count of "g.V().has(\"person\",\"name\",\"marko\").out(\"knows\").has(\"person\",\"name\",\"vadas\")"
    And the graph should return 1 for count of "g.V().has(\"person\",\"name\",\"vadas\").out(\"self\").has(\"person\",\"name\",\"vadas\")"

  @UserSuppliedVertexIds
  Scenario: g_withSideEffectXc_created_YX_withSideEffectXm_matchedX_mergeEXlabel_knows_out_marko_in_vadasX_optionXonCreate_selectXcXX_optionXonMatch_sideEffectXpropertiesXweightX_dropX_selectXmXX_exists
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property(T.id, 100).property("name", "marko").as("a").
        addV("person").property(T.id, 101).property("name", "vadas").as("b").
        addE("knows").property("weight", 0.5).from("a").to("b")
      """
    And using the parameter xx1 defined as "m[{\"t[label]\": \"knows\", \"D[OUT]\":\"v[100]\", \"D[IN]\":\"v[101]\"}]"
    And using the parameter xx2 defined as "m[{\"t[label]\": \"knows\", \"D[OUT]\":\"v[100]\", \"D[IN]\":\"v[101]\",\"created\":\"Y\"}]"
    And using the parameter xx3 defined as "m[{\"created\":\"N\"}]"
    And the traversal of
      """
      g.withSideEffect("c",xx2).withSideEffect("m",xx3).
        mergeE(xx1).
          option(Merge.onCreate, __.select("c")).
          option(Merge.onMatch, __.sideEffect(__.properties("weight").drop()).select("m"))
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 2 for count of "g.V()"
    And the graph should return 0 for count of "g.E().hasLabel(\"knows\").has(\"created\",\"Y\")"
    And the graph should return 1 for count of "g.E().hasLabel(\"knows\").has(\"created\",\"N\")"
    And the graph should return 0 for count of "g.E().hasLabel(\"knows\").has(\"weight\")"


  @UserSuppliedVertexIds
  Scenario: g_mergeE_with_outVinV_options_map
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property(T.id, 1).property("name", "a")
       .addV("person").property(T.id, 2).property("name", "b")
      """
    And using the parameter xx1 defined as "m[{ \"D[OUT]\": \"M[outV]\", \"D[IN]\": \"M[inV]\", \"t[label]\": \"knows\"}]"
    And using the parameter xx2 defined as "m[{\"t[id]\": 1}]"
    And using the parameter xx3 defined as "m[{\"t[id]\": 2}]"
    And the traversal of
      """
      g.mergeE(xx1).option(outV, xx2).option(inV, xx3)
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 2 for count of "g.V()"
    And the graph should return 1 for count of "g.V(1).out(\"knows\").hasId(2)"

  @UserSuppliedVertexIds
  Scenario: g_mergeE_with_outVinV_options_select
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property(T.id, 1).property("name", "a")
       .addV("person").property(T.id, 2).property("name", "b")
      """
    And using the parameter xx1 defined as "m[{ \"D[OUT]\": \"M[outV]\", \"D[IN]\": \"M[inV]\", \"t[label]\": \"knows\"}]"
    And the traversal of
      """
      g.V(1).as("x").V(2).as("y").mergeE(xx1).option(outV, select("x")).option(inV, select("y"))
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 2 for count of "g.V()"
    And the graph should return 1 for count of "g.V(1).out(\"knows\").hasId(2)"

  # onCreate inherits from merge and can specify an eid
  @UserSuppliedVertexIds
  @UserSuppliedEdgeIds
  Scenario: g_mergeE_with_eid_specified_and_inheritance_1
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property(T.id, 1).property("name", "a")
       .addV("person").property(T.id, 2).property("name", "b")
      """
    And using the parameter xx1 defined as "m[{\"D[OUT]\": \"v[1]\", \"D[IN]\": \"v[2]\", \"t[label]\": \"knows\"}]"
    And using the parameter xx2 defined as "m[{\"t[id]\": 201}]"
    And the traversal of
      """
      g.mergeE(xx1).option(onCreate, xx2)
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 2 for count of "g.V()"
    And the graph should return 1 for count of "g.E()"
    And the graph should return 1 for count of "g.E(201)"
    And the graph should return 1 for count of "g.V(1).out(\"knows\").hasId(2)"

  # onCreate inherits from merge and can specify an eid
  @UserSuppliedVertexIds
  @UserSuppliedEdgeIds
  Scenario: g_mergeE_with_eid_specified_and_inheritance_2
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property(T.id, 1).property("name", "a")
       .addV("person").property(T.id, 2).property("name", "b")
      """
    And using the parameter xx1 defined as "m[{\"t[id]\": 201}]"
    And using the parameter xx2 defined as "m[{\"D[OUT]\": \"v[1]\", \"D[IN]\": \"v[2]\", \"t[label]\": \"knows\"}]"
    And the traversal of
      """
      g.mergeE(xx1).option(onCreate, xx2)
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 2 for count of "g.V()"
    And the graph should return 1 for count of "g.E()"
    And the graph should return 1 for count of "g.E(201)"
    And the graph should return 1 for count of "g.V(1).out(\"knows\").hasId(2)"

  # cannot override Direction.OUT in onCreate
  @UserSuppliedVertexIds
  Scenario: g_mergeE_outV_override_prohibited
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property(T.id, 1).property("name", "a")
       .addV("person").property(T.id, 2).property("name", "b")
      """
    And using the parameter xx1 defined as "m[{\"D[OUT]\" : \"v[1]\", \"D[IN]\" : \"v[2]\", \"t[label]\": \"knows\"}]"
    And using the parameter xx2 defined as "m[{\"D[OUT]\" : \"v[2]\"}]"
    And the traversal of
      """
      g.mergeE(xx1).option(onCreate, xx2)
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "option(onCreate) cannot override values from merge() argument"

  # cannot override Direction.IN in onCreate
  @UserSuppliedVertexIds
  Scenario: g_mergeE_inV_override_prohibited
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property(T.id, 1).property("name", "a")
       .addV("person").property(T.id, 2).property("name", "b")
      """
    And using the parameter xx1 defined as "m[{\"D[OUT]\" : \"v[1]\", \"D[IN]\" : \"v[2]\", \"t[label]\": \"knows\"}]"
    And using the parameter xx2 defined as "m[{\"D[IN]\" : \"v[1]\"}]"
    And the traversal of
      """
      g.mergeE(xx1).option(onCreate, xx2)
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "option(onCreate) cannot override values from merge() argument"

  # cannot override T.label in onCreate
  @UserSuppliedVertexIds
  Scenario: g_mergeE_label_override_prohibited
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property(T.id, 1).property("name", "a")
       .addV("person").property(T.id, 2).property("name", "b")
      """
    And using the parameter xx1 defined as "m[{\"D[OUT]\" : \"v[1]\", \"D[IN]\" : \"v[2]\", \"t[label]\": \"knows\"}]"
    And using the parameter xx2 defined as "m[{\"t[label]\": \"likes\"}]"
    And the traversal of
      """
      g.mergeE(xx1).option(onCreate, xx2)
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "option(onCreate) cannot override values from merge() argument"

  # cannot override T.id in onCreate
  @UserSuppliedVertexIds
  @UserSuppliedEdgeIds
  Scenario: g_mergeE_id_override_prohibited
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property(T.id, 1).property("name", "a")
       .addV("person").property(T.id, 2).property("name", "b")
      """
    And using the parameter xx1 defined as "m[{\"D[OUT]\" : \"v[1]\", \"D[IN]\" : \"v[2]\", \"t[label]\": \"knows\", \"t[id]\": \"101\"}]"
    And using the parameter xx2 defined as "m[{\"t[id]\": \"201\"}]"
    And the traversal of
      """
      g.mergeE(xx1).option(onCreate, xx2)
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "option(onCreate) cannot override values from merge() argument"

  # combining mergeV and mergeE when vertices do not exist
  @UserSuppliedVertexIds
  Scenario: g_mergeV_mergeE_combination_new_vertices
    Given the empty graph
    And using the parameter xx1 defined as "m[{\"t[id]\": 1, \"t[label]\": \"person\"}]"
    And using the parameter xx2 defined as "m[{\"t[id]\": 2, \"t[label]\": \"person\"}]"
    And using the parameter xx3 defined as "m[{\"D[OUT]\": \"M[outV]\", \"D[IN]\": \"M[inV]\", \"t[label]\": \"knows\"}]"
    And the traversal of
      """
      g.mergeV(xx1).as("outV").mergeV(xx2).as("inV").mergeE(xx3).option(outV, select("outV")).option(inV, select("inV"))
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 2 for count of "g.V()"
    And the graph should return 1 for count of "g.E()"
    And the graph should return 1 for count of "g.V(1).out(\"knows\").hasId(2)"

  # combining mergeV and mergeE when vertices exist
  @UserSuppliedVertexIds
  Scenario: g_mergeV_mergeE_combination_existing_vertices
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property(T.id, 1)
       .addV("person").property(T.id, 2)
      """
    And using the parameter xx1 defined as "m[{\"t[id]\": 1, \"t[label]\": \"person\"}]"
    And using the parameter xx2 defined as "m[{\"t[id]\": 2, \"t[label]\": \"person\"}]"
    And using the parameter xx3 defined as "m[{\"D[OUT]\": \"M[outV]\", \"D[IN]\": \"M[inV]\", \"t[label]\": \"knows\"}]"
    And the traversal of
      """
      g.mergeV(xx1).as("outV").mergeV(xx2).as("inV").mergeE(xx3).option(outV, select("outV")).option(inV, select("inV"))
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 2 for count of "g.V()"
    And the graph should return 1 for count of "g.E()"
    And the graph should return 1 for count of "g.V(1).out(\"knows\").hasId(2)"

