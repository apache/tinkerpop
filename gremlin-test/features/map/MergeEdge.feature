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

  Scenario: g_V_mergeEXlabel_self_weight_05X
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", "marko").property("age", 29)
      """
    And using the parameter xx1 defined as "m[{\"t[label]\": \"self\", \"weight\":\"d[0.5].d\"}]"
    And the traversal of
      """
      g.V().mergeE(xx1)
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 1 for count of "g.E().hasLabel(\"self\").has(\"weight\",0.5)"

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
    And the graph should return 2 for count of "g.V().has(\"person\",\"name\",\"marko\").outE(\"knows\").inV().has(\"person\",\"name\",\"vadas\")"

  @UserSuppliedVertexIds
  Scenario: g_mergeEXlabel_knows_out_marko_in_vadas_weight_05X
    Given the empty graph
    And using the parameter xx1 defined as "m[{\"t[label]\": \"knows\", \"D[OUT]\":\"v[100]\", \"D[IN]\":\"v[101]\", \"weight\":\"d[0.5].d\"}]"
    And the traversal of
      """
      g.mergeE(xx1)
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 2 for count of "g.V()"
    And the graph should return 1 for count of "g.E().hasLabel(\"knows\").has(\"weight\",0.5)"

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
    Then the result should have a count of 1
    And the graph should return 2 for count of "g.V()"
    And the graph should return 1 for count of "g.E().hasLabel(\"knows\").has(\"created\",\"Y\")"
    And the graph should return 0 for count of "g.E().hasLabel(\"knows\").has(\"created\",\"N\")"

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
  Scenario: g_VX100X_VX101X_mergeEXlabel_knows_out_marko_in_vadasX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property(T.id, 100).property("name", "marko")
      """
    And using the parameter xx1 defined as "m[{\"t[label]\": \"knows\", \"D[OUT]\":\"v[100]\", \"D[IN]\":\"v[101]\"}]"
    And the traversal of
      """
      g.V(100).V(101).mergeE(xx1)
      """
    When iterated to list
    Then the result should have a count of 0
    And the graph should return 1 for count of "g.V()"
    And the graph should return 0 for count of "g.E()"

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