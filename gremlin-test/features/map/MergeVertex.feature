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

@StepClassMap @StepMergeV
Feature: Step - mergeV()

  Scenario: g_mergeVXlabel_person_name_stephenX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", "marko").property("age", 29).as("marko")
      """
    And using the parameter xx1 defined as "m[{\"t[label]\": \"person\", \"name\":\"stephen\"}]"
    And the traversal of
      """
      g.mergeV(xx1)
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 1 for count of "g.V().has(\"person\",\"name\",\"stephen\")"

  Scenario: g_mergeVXlabel_person_name_markoX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", "marko").property("age", 29).as("marko")
      """
    And using the parameter xx1 defined as "m[{\"t[label]\": \"person\", \"name\":\"marko\"}]"
    And the traversal of
      """
      g.mergeV(xx1)
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 1 for count of "g.V().has(\"person\",\"name\",\"marko\")"

  Scenario: g_mergeVXlabel_person_name_stephenX_optionXonCreate_label_person_name_stephen_age_19X_option
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", "marko").property("age", 29).as("marko")
      """
    And using the parameter xx1 defined as "m[{\"t[label]\": \"person\", \"name\":\"stephen\"}]"
    And using the parameter xx2 defined as "m[{\"t[label]\": \"person\", \"name\":\"stephen\", \"age\": \"d[19].i\"}]"
    And the traversal of
      """
      g.mergeV(xx1).option(Merge.onCreate,xx2)
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 1 for count of "g.V().has(\"person\",\"name\",\"stephen\").has(\"age\", 19)"

  Scenario: g_mergeVXlabel_person_name_markoX_optionXonMatch_age_19X_option
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", "marko").property("age", 29).as("marko")
      """
    And using the parameter xx1 defined as "m[{\"t[label]\": \"person\", \"name\":\"marko\"}]"
    And using the parameter xx2 defined as "m[{\"age\": \"d[19].i\"}]"
    And the traversal of
      """
      g.mergeV(xx1).option(Merge.onMatch,xx2)
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 1 for count of "g.V().has(\"person\",\"name\",\"marko\").has(\"age\", 19)"

  Scenario: g_withSideEffectXc_label_person_name_stephenX_withSideEffectXm_label_person_name_stephen_age_19X_mergeVXselectXcXX_optionXonCreate_selectXmXX_option
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", "marko").property("age", 29).as("marko")
      """
    And using the parameter xx1 defined as "m[{\"t[label]\": \"person\", \"name\":\"stephen\"}]"
    And using the parameter xx2 defined as "m[{\"t[label]\": \"person\", \"name\":\"stephen\", \"age\": \"d[19].i\"}]"
    And the traversal of
      """
      g.withSideEffect("c", xx1).
        withSideEffect("m", xx2).
        mergeV(__.select("c")).option(Merge.onCreate, __.select("m"))
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 1 for count of "g.V().has(\"person\",\"name\",\"stephen\").has(\"age\", 19)"

  Scenario: g_withSideEffectXc_label_person_name_markoX_withSideEffectXm_age_19X_mergeVXselectXcXX_optionXonMatch_selectXmXX_option
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", "marko").property("age", 29).as("marko")
      """
    And using the parameter xx1 defined as "m[{\"t[label]\": \"person\", \"name\":\"marko\"}]"
    And using the parameter xx2 defined as "m[{\"age\": \"d[19].i\"}]"
    And the traversal of
      """
      g.withSideEffect("c", xx1).
        withSideEffect("m", xx2).
        mergeV(__.select("c")).option(Merge.onMatch, __.select("m"))
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 1 for count of "g.V().has(\"person\",\"name\",\"marko\").has(\"age\", 19)"

  @MultiMetaProperties
  Scenario: g_mergeVXlabel_person_name_markoX_propertyXname_vadas_acl_publicX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", "marko").property("age", 29).as("marko")
      """
    And using the parameter xx1 defined as "m[{\"t[label]\": \"person\", \"name\":\"marko\"}]"
    And the traversal of
      """
      g.mergeV(xx1).property("name","vadas","acl","public")
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 1 for count of "g.V().properties(\"name\").hasValue(\"vadas\").has(\"acl\",\"public\")"

  Scenario: g_injectX0X_mergeVXlabel_person_name_stephenX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", "marko").property("age", 29).as("marko")
      """
    And using the parameter xx1 defined as "m[{\"t[label]\": \"person\", \"name\":\"stephen\"}]"
    And the traversal of
      """
      g.inject(0).mergeV(xx1)
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 1 for count of "g.V().has(\"person\",\"name\",\"stephen\")"

  Scenario: g_injectX0X_mergeVXlabel_person_name_markoX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", "marko").property("age", 29).as("marko")
      """
    And using the parameter xx1 defined as "m[{\"t[label]\": \"person\", \"name\":\"marko\"}]"
    And the traversal of
      """
      g.inject(0).mergeV(xx1)
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 1 for count of "g.V().has(\"person\",\"name\",\"marko\")"

  Scenario: g_injectX0X_mergeVXlabel_person_name_stephenX_optionXonCreate_label_person_name_stephen_age_19X_option
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", "marko").property("age", 29).as("marko")
      """
    And using the parameter xx1 defined as "m[{\"t[label]\": \"person\", \"name\":\"stephen\"}]"
    And using the parameter xx2 defined as "m[{\"t[label]\": \"person\", \"name\":\"stephen\", \"age\": \"d[19].i\"}]"
    And the traversal of
      """
      g.inject(0).mergeV(xx1).option(Merge.onCreate,xx2)
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 1 for count of "g.V().has(\"person\",\"name\",\"stephen\").has(\"age\", 19)"

  Scenario: g_injectX0X_mergeVXlabel_person_name_markoX_optionXonMatch_age_19X_option
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", "marko").property("age", 29).as("marko")
      """
    And using the parameter xx1 defined as "m[{\"t[label]\": \"person\", \"name\":\"marko\"}]"
    And using the parameter xx2 defined as "m[{\"age\": \"d[19].i\"}]"
    And the traversal of
      """
      g.inject(0).mergeV(xx1).option(Merge.onMatch,xx2)
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 1 for count of "g.V().has(\"person\",\"name\",\"marko\").has(\"age\", 19)"

  Scenario: g_withSideEffectXc_label_person_name_stephenX_withSideEffectXm_label_person_name_stephen_age_19X_injectX0X_mergeVXselectXcXX_optionXonCreate_selectXmXX_option
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", "marko").property("age", 29).as("marko")
      """
    And using the parameter xx1 defined as "m[{\"t[label]\": \"person\", \"name\":\"stephen\"}]"
    And using the parameter xx2 defined as "m[{\"t[label]\": \"person\", \"name\":\"stephen\", \"age\": \"d[19].i\"}]"
    And the traversal of
      """
      g.withSideEffect("c", xx1).
        withSideEffect("m", xx2).
        inject(0).mergeV(__.select("c")).option(Merge.onCreate, __.select("m"))
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 1 for count of "g.V().has(\"person\",\"name\",\"stephen\").has(\"age\", 19)"

  Scenario: g_withSideEffectXc_label_person_name_markoX_withSideEffectXm_age_19X_injectX0X_mergeVXselectXcXX_optionXonMatch_selectXmXX_option
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", "marko").property("age", 29).as("marko")
      """
    And using the parameter xx1 defined as "m[{\"t[label]\": \"person\", \"name\":\"marko\"}]"
    And using the parameter xx2 defined as "m[{\"age\": \"d[19].i\"}]"
    And the traversal of
      """
      g.withSideEffect("c", xx1).
        withSideEffect("m", xx2).
        inject(0).mergeV(__.select("c")).option(Merge.onMatch, __.select("m"))
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 1 for count of "g.V().has(\"person\",\"name\",\"marko\").has(\"age\", 19)"

  @MultiMetaProperties
  Scenario: g_injectX0X_mergeVXlabel_person_name_markoX_propertyXname_vadas_acl_publicX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", "marko").property("age", 29).as("marko")
      """
    And using the parameter xx1 defined as "m[{\"t[label]\": \"person\", \"name\":\"marko\"}]"
    And the traversal of
      """
      g.inject(0).mergeV(xx1).property("name","vadas","acl","public")
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 1 for count of "g.V().properties(\"name\").hasValue(\"vadas\").has(\"acl\",\"public\")"

  Scenario: g_injectXlabel_person_name_marko_label_person_name_stephenX_mergeVXidentityX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", "marko").property("age", 29).as("marko")
      """
    And using the parameter xx1 defined as "m[{\"t[label]\": \"person\", \"name\":\"marko\"}]"
    And using the parameter xx2 defined as "m[{\"t[label]\": \"person\", \"name\":\"stephen\"}]"
    And the traversal of
      """
      g.inject(xx1, xx2).mergeV(__.identity())
      """
    When iterated to list
    Then the result should have a count of 2
    And the graph should return 1 for count of "g.V().has(\"person\",\"name\",\"stephen\")"
    And the graph should return 1 for count of "g.V().has(\"person\",\"name\",\"marko\")"
    And the graph should return 2 for count of "g.V()"

  Scenario: g_injectXlabel_person_name_marko_label_person_name_stephenX_mergeV
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", "marko").property("age", 29).as("marko")
      """
    And using the parameter xx1 defined as "m[{\"t[label]\": \"person\", \"name\":\"marko\"}]"
    And using the parameter xx2 defined as "m[{\"t[label]\": \"person\", \"name\":\"stephen\"}]"
    And the traversal of
      """
      g.inject(xx1, xx2).mergeV()
      """
    When iterated to list
    Then the result should have a count of 2
    And the graph should return 1 for count of "g.V().has(\"person\",\"name\",\"stephen\")"
    And the graph should return 1 for count of "g.V().has(\"person\",\"name\",\"marko\")"
    And the graph should return 2 for count of "g.V()"