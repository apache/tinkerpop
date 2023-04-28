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

  # TEST INDEX
  # Use of inject() is meant to exercise mergeV() in a mid-traversal form rather than start step
  #
  # g_mergeVXlabel_person_name_stephenX
  # g_injectX0X_mergeVXlabel_person_name_stephenX
  #   - mergeV(Map) and no option()
  #   - results in new vertex
  # g_mergeVXlabel_person_name_markoX
  # g_injectX0X_mergeVXlabel_person_name_markoX
  #   - mergeV(Map) and no option()
  #   - results in found vertex
  # g_mergeVXlabel_person_name_stephenX_optionXonCreate_label_person_name_stephen_age_19X_option
  # g_injectX0X_mergeVXlabel_person_name_stephenX_optionXonCreate_label_person_name_stephen_age_19X_option
  #   - mergeV(Map) using onCreate(Map)
  #   - results in new vertex
  # g_mergeVXlabel_person_name_markoX_optionXonMatch_age_19X_option
  # g_injectX0X_mergeVXlabel_person_name_markoX_optionXonMatch_age_19X_option
  #   - mergeV(Map) using onMatch(Map)
  #   - results in update vertex
  # g_withSideEffectXc_label_person_name_stephenX_withSideEffectXm_label_person_name_stephen_age_19X_mergeVXselectXcXX_optionXonCreate_selectXmXX_option
  # g_withSideEffectXc_label_person_name_stephenX_withSideEffectXm_label_person_name_stephen_age_19X_injectX0X_mergeVXselectXcXX_optionXonCreate_selectXmXX_option
  #   - mergeV(Traversal) grabbing side-effect Map with onCreate(Traversal)
  #   - results in new vertex
  # g_withSideEffectXc_label_person_name_markoX_withSideEffectXm_age_19X_mergeVXselectXcXX_optionXonMatch_selectXmXX_option
  # g_withSideEffectXc_label_person_name_markoX_withSideEffectXm_age_19X_injectX0X_mergeVXselectXcXX_optionXonMatch_selectXmXX_option
  #   - mergeV(Traversal) grabbing side-effect Map with onMatch(Traversal)
  #   - results in updated vertex
  # g_mergeVXlabel_person_name_markoX_propertyXname_vadas_acl_publicX
  # g_injectX0X_mergeVXlabel_person_name_markoX_propertyXname_vadas_acl_publicX
  #   - mergeV(Map) with no option and call to AddPropertyStep
  #   - results in updated vertex and added meta-property
  # g_injectXlabel_person_name_marko_label_person_name_stephenX_mergeVXidentityX
  #   - mergeV(Traversal) grabbing current Map from traverser for the search criteria with no option()
  #   - result in one found vertex and one new vertex
  # g_injectXlabel_person_name_marko_label_person_name_stephenX_mergeV
  #   - mergeV() which assumes incoming Map on the traverser for the search criteria with no option()
  #   - results in one found vertex and one new vertex
  # g_mergeVXlabel_person_name_stephenX_propertyXlist_name_steveX
  #   - mergeV() which assumes finding vertex with list cardinality property and call AddPropertyStep
  #   - results in updated vertex with additional list cardinality property
  # g_mergeXlabel_person_name_vadasX_optionXonMatch_age_35X
  #   - mergeV(Map) using onMatch(Map)
  #   - results in updating two matched vertices
  # g_V_mapXmergeXlabel_person_name_joshXX
  #   - mergeV(Map) with no option() - testing child traversal usage
  #   - results in one new vertex and one existing vertex that was just created
  # g_mergeVXnullX
  # g_V_mergeVXnullX
  #   - mergeV(null) with no option()
  #   - results in no new vertex and nothing returned
  # g_mergeVXemptyX
  # g_V_mergeVXemptyX
  #   - mergeV(empty) with no option()
  #   - results in matched vertex with no updates
  # g_mergeVXemptyX_no_existing
  # g_injectX0X_mergeVXemptyX_no_existing
  #   - mergeV(empty) with no option()
  #   - results in not matched updates and a creation of a vertex with default values
  # g_mergeVXnullX_optionXonCreate_emptyX
  # g_V_mergeVXnullX_optionXonCreate_emptyX
  #   - mergeV(null) with onCreate(empty)
  #   - results in no matches and creates a default vertex
  # g_mergeVXlabel_person_name_stephenX_optionXonCreate_nullX
  # g_V_mergeVXlabel_person_name_stephenX_optionXonCreate_nullX
  #   - mergeV(Map) with onCreate(null)
  #   - results in no match and no vertex creation
  # g_mergeVXnullX_optionXonCreate_label_null_name_markoX
  # g_V_mergeVXnullX_optionXonCreate_label_null_name_markoX
  #   - mergeV(null) with onCreate(Map) where Map has a null label
  #   - results in error
  # g_mergeVXemptyX_optionXonMatch_nullX
  # g_V_mergeVXemptyX_optionXonMatch_nullX
  #   - mergeV(empty) with onMatch(null)
  #   - results in a match and no vertex update
  # g_V_mergeVXemptyX_two_exist
  #  - mergeV(empty) with no option()
  #  - results in matching two vertices
  # g_withSideEffectXc_label_person_name_markoX_withSideEffectXm_age_19X_mergeVXselectXcXX_optionXonMatch_sideEffectXpropertiesXageX_dropX_selectXmXX_option
  #  - mergeV() that has a match to a vertex with multiproperties
  #  - results in all multiproperties removed to be replaced with one property
  # g_withSideEffectXm_age_19X_V_hasXperson_name_markoX_mergeVXselectXcXX_optionXonMatch_sideEffectXpropertiesXageX_dropX_selectXmXX_option
  #  - mergeV() that has a match to a vertex with multiproperties
  #  - results in all multiproperties removed to be replaced with one property

  Scenario: g_mergeVXemptyX_optionXonMatch_nullX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", "marko").property("age", 29)
      """
    And the traversal of
      """
      g.mergeV([:]).option(Merge.onMatch, null)
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 1 for count of "g.V().has(\"person\",\"name\",\"marko\").has(\"age\",29)"

  Scenario: g_V_mergeVXemptyX_optionXonMatch_nullX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", "marko").property("age", 29)
      """
    And the traversal of
      """
      g.V().mergeV([:]).option(Merge.onMatch, null)
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 1 for count of "g.V().has(\"person\",\"name\",\"marko\").has(\"age\",29)"

  Scenario: g_mergeVXnullX_optionXonCreate_label_null_name_markoX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", "marko").property("age", 29)
      """
    And using the parameter xx1 defined as "m[{\"t[label]\": null, \"name\":\"marko\"}]"
    And the traversal of
      """
      g.mergeV(xx1)
      """
    When iterated to list
    Then the traversal will raise an error

  Scenario: g_V_mergeVXnullX_optionXonCreate_label_null_name_markoX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", "marko").property("age", 29)
      """
    And using the parameter xx1 defined as "m[{\"t[label]\": null, \"name\":\"marko\"}]"
    And the traversal of
      """
      g.V().mergeV(xx1)
      """
    When iterated to list
    Then the traversal will raise an error

  Scenario: g_mergeVXlabel_person_name_stephenX_optionXonCreate_nullX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", "marko").property("age", 29)
      """
    And using the parameter xx1 defined as "m[{\"t[label]\": \"person\", \"name\":\"stephen\"}]"
    And the traversal of
      """
      g.mergeV(xx1).option(Merge.onCreate, null)
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 2 for count of "g.V()"
    And the graph should return 1 for count of "g.V().has(\"person\",\"name\",\"marko\")"
    And the graph should return 1 for count of "g.V().has(\"person\",\"name\",\"stephen\")"

  Scenario: g_V_mergeVXlabel_person_name_stephenX_optionXonCreate_nullX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", "marko").property("age", 29)
      """
    And using the parameter xx1 defined as "m[{\"t[label]\": \"person\", \"name\":\"stephen\"}]"
    And the traversal of
      """
      g.V().mergeV(xx1).option(Merge.onCreate, null)
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 2 for count of "g.V()"
    And the graph should return 1 for count of "g.V().has(\"person\",\"name\",\"marko\")"
    And the graph should return 1 for count of "g.V().has(\"person\",\"name\",\"stephen\")"

  Scenario: g_mergeVXnullX_optionXonCreate_emptyX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", "marko").property("age", 29)
      """
    And the traversal of
      """
      g.mergeV(null).option(Merge.onCreate,[:])
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 1 for count of "g.V()"

  Scenario: g_V_mergeVXnullX_optionXonCreate_emptyX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", "marko").property("age", 29)
      """
    And the traversal of
      """
      g.V().mergeV(null).option(Merge.onCreate,[:])
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 1 for count of "g.V()"

  Scenario: g_mergeVXemptyX_no_existing
    Given the empty graph
    And the traversal of
      """
      g.mergeV([:])
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 1 for count of "g.V()"

  Scenario: g_injectX0X_mergeVXemptyX_no_existing
    Given the empty graph
    And the traversal of
      """
      g.inject(0).mergeV([:])
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 1 for count of "g.V()"

  Scenario: g_mergeVXemptyX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", "marko").property("age", 29)
      """
    And the traversal of
      """
      g.mergeV([:])
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 1 for count of "g.V().has(\"person\",\"name\",\"marko\").has(\"age\",29)"

  Scenario: g_V_mergeVXemptyX_two_exist
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", "marko").property("age", 29).
        addV("person").property("name", "vadas").property("age", 27)
      """
    And the traversal of
      """
      g.V().mergeV([:])
      """
    When iterated to list
    Then the result should have a count of 4
    And the graph should return 2 for count of "g.V()"
    And the graph should return 1 for count of "g.V().has(\"person\",\"name\",\"marko\").has(\"age\",29)"
    And the graph should return 1 for count of "g.V().has(\"person\",\"name\",\"vadas\").has(\"age\",27)"

  Scenario: g_mergeVXnullX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", "marko").property("age", 29)
      """
    And the traversal of
      """
      g.mergeV(null)
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 1 for count of "g.V()"

  Scenario: g_V_mergeVXnullX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", "marko").property("age", 29)
      """
    And the traversal of
      """
      g.V().mergeV(null)
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 1 for count of "g.V()"

  Scenario: g_mergeVXlabel_person_name_stephenX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", "marko").property("age", 29)
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
      g.addV("person").property("name", "marko").property("age", 29)
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
      g.addV("person").property("name", "marko").property("age", 29)
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
      g.addV("person").property("name", "marko").property("age", 29)
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
      g.addV("person").property("name", "marko").property("age", 29)
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
      g.addV("person").property("name", "marko").property("age", 29)
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
      g.addV("person").property("name", "marko").property("age", 29)
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
      g.addV("person").property("name", "marko").property("age", 29)
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
      g.addV("person").property("name", "marko").property("age", 29)
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
      g.addV("person").property("name", "marko").property("age", 29)
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
      g.addV("person").property("name", "marko").property("age", 29)
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
      g.addV("person").property("name", "marko").property("age", 29)
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
      g.addV("person").property("name", "marko").property("age", 29)
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
      g.addV("person").property("name", "marko").property("age", 29)
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
      g.addV("person").property("name", "marko").property("age", 29)
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
      g.addV("person").property("name", "marko").property("age", 29)
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

  @MultiMetaProperties
  Scenario: g_mergeVXlabel_person_name_stephenX_propertyXlist_name_steveX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property(list, "name", "stephen")
      """
    And using the parameter xx1 defined as "m[{\"t[label]\": \"person\", \"name\":\"stephen\"}]"
    And the traversal of
      """
      g.mergeV(xx1).property(Cardinality.list,"name","steve")
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 1 for count of "g.V()"
    And the graph should return 1 for count of "g.V().properties(\"name\").hasValue(\"steve\")"
    And the graph should return 1 for count of "g.V().properties(\"name\").hasValue(\"stephen\")"
    And the graph should return 2 for count of "g.V().properties(\"name\")"

  Scenario: g_mergeXlabel_person_name_vadasX_optionXonMatch_age_35X
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", "vadas").property("age", 29).
        addV("person").property("name", "vadas").property("age", 27)
      """
    And using the parameter xx1 defined as "m[{\"t[label]\": \"person\", \"name\":\"vadas\"}]"
    And using the parameter xx2 defined as "m[{\"age\":\"d[35].i\"}]"
    And the traversal of
      """
      g.mergeV(xx1).option(Merge.onMatch, xx2)
      """
    When iterated to list
    Then the result should have a count of 2
    And the graph should return 2 for count of "g.V().has(\"age\",35)"
    And the graph should return 2 for count of "g.V()"

  Scenario: g_V_mapXmergeXlabel_person_name_joshXX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", "vadas").property("age", 29).
        addV("person").property("name", "stephen").property("age", 27)
      """
    And using the parameter xx1 defined as "m[{\"t[label]\": \"person\", \"name\":\"josh\"}]"
    And the traversal of
      """
      g.V().map(__.mergeV(xx1))
      """
    When iterated to list
    Then the result should have a count of 2
    And the graph should return 1 for count of "g.V().has(\"person\",\"name\",\"josh\")"
    And the graph should return 3 for count of "g.V()"

  @MultiMetaProperties
  Scenario: g_withSideEffectXc_label_person_name_markoX_withSideEffectXm_age_19X_mergeVXselectXcXX_optionXonMatch_sideEffectXpropertiesXageX_dropX_selectXmXX_option
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", "marko").property(Cardinality.list, "age", 29).property(Cardinality.list, "age", 31).property(Cardinality.list, "age", 32)
      """
    And using the parameter xx1 defined as "m[{\"t[label]\": \"person\", \"name\":\"marko\"}]"
    And using the parameter xx2 defined as "m[{\"age\": \"d[19].i\"}]"
    And the traversal of
      """
      g.withSideEffect("c", xx1).
        withSideEffect("m", xx2).
        mergeV(__.select("c")).
          option(Merge.onMatch, __.sideEffect(__.properties("age").drop()).select("m"))
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 1 for count of "g.V().has(\"person\",\"name\",\"marko\").has(\"age\", 19)"
    And the graph should return 1 for count of "g.V().has(\"person\",\"name\",\"marko\").has(\"age\")"

  @MultiMetaProperties
  Scenario: g_withSideEffectXm_age_19X_V_hasXperson_name_markoX_mergeVXselectXcXX_optionXonMatch_sideEffectXpropertiesXageX_dropX_selectXmXX_option
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", "marko").property(Cardinality.list, "age", 29).property(Cardinality.list, "age", 31).property(Cardinality.list, "age", 32)
      """
    And using the parameter xx1 defined as "m[{\"age\": \"d[19].i\"}]"
    And the traversal of
      """
      g.withSideEffect("m", xx1).
        V().has("person", "name", "marko").
        mergeV([:]).
          option(Merge.onMatch, __.sideEffect(__.properties("age").drop()).select("m"))
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 1 for count of "g.V().has(\"person\",\"name\",\"marko\").has(\"age\", 19)"
    And the graph should return 1 for count of "g.V().has(\"person\",\"name\",\"marko\").has(\"age\")"

  # onCreate inheritance from merge
  @UserSuppliedVertexIds
  Scenario: g_mergeV_onCreate_inheritance_existing
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", "mike").property(T.id, 1)
      """
    And using the parameter xx1 defined as "m[{\"t[id]\": 1}]"
    And using the parameter xx2 defined as "m[{\"t[label]\": \"person\", \"name\":\"mike\"}]"
    And the traversal of
      """
      g.mergeV(xx1).option(Merge.onCreate, xx2)
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 1 for count of "g.V()"
    And the graph should return 1 for count of "g.V(1).has(\"person\",\"name\",\"mike\")"

  # onCreate inheritance from merge
  @UserSuppliedVertexIds
  Scenario: g_mergeV_onCreate_inheritance_new_1
    Given the empty graph
    And using the parameter xx1 defined as "m[{\"t[id]\": 1}]"
    And using the parameter xx2 defined as "m[{\"t[label]\": \"person\", \"name\":\"mike\"}]"
    And the traversal of
      """
      g.mergeV(xx1).option(Merge.onCreate, xx2)
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 1 for count of "g.V()"
    And the graph should return 1 for count of "g.V(1).has(\"person\",\"name\",\"mike\")"

  # onCreate inheritance from merge
  @UserSuppliedVertexIds
  Scenario: g_mergeV_onCreate_inheritance_new_2
    Given the empty graph
    And using the parameter xx1 defined as "m[{\"t[label]\": \"person\", \"name\":\"mike\"}]"
    And using the parameter xx2 defined as "m[{\"t[id]\": 1}]"
    And the traversal of
      """
      g.mergeV(xx1).option(Merge.onCreate, xx2)
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 1 for count of "g.V()"
    And the graph should return 1 for count of "g.V(1).has(\"person\",\"name\",\"mike\")"

  # cannot override T.label in onCreate
  Scenario: g_mergeV_label_override_prohibited
    Given the empty graph
    And using the parameter xx1 defined as "m[{\"t[label]\": \"a\"}]"
    And using the parameter xx2 defined as "m[{\"t[label]\": \"b\"}]"
    And the traversal of
      """
      g.mergeV(xx1).option(onCreate, xx2)
      """
    When iterated to list
    Then the traversal will raise an error

  # cannot override T.id in onCreate
  @UserSuppliedVertexIds
  Scenario: g_mergeV_id_override_prohibited
    Given the empty graph
    And using the parameter xx1 defined as "m[{\"t[id]\": 1}]"
    And using the parameter xx2 defined as "m[{\"t[id]\": 2}]"
    And the traversal of
      """
      g.mergeV(xx1).option(onCreate, xx2)
      """
    When iterated to list
    Then the traversal will raise an error

  # cannot use hidden namespace for id key
  Scenario: g_mergeV_hidden_id_key_prohibited
    Given the empty graph
    And using the parameter xx1 defined as "m[{\"~id\": 1}]"
    And the traversal of
      """
      g.mergeV(xx1)
      """
    When iterated to list
    Then the traversal will raise an error

  # cannot use hidden namespace for label key
  Scenario: g_mergeV_hidden_label_key_prohibited
    Given the empty graph
    And using the parameter xx1 defined as "m[{\"~label\":\"vertex\"}]"
    And the traversal of
      """
      g.mergeV(xx1)
      """
    When iterated to list
    Then the traversal will raise an error

  # cannot use hidden namespace for label value
  Scenario: g_mergeV_hidden_label_value_prohibited
    Given the empty graph
    And using the parameter xx1 defined as "m[{\"t[label]\":\"~vertex\"}]"
    And the traversal of
      """
      g.mergeV(xx1)
      """
    When iterated to list
    Then the traversal will raise an error

  # cannot use hidden namespace for id key for onCreate
  Scenario: g_mergeV_hidden_id_key_onCreate_prohibited
    Given the empty graph
    And using the parameter xx1 defined as "m[{\"~id\": 1}]"
    And the traversal of
      """
      g.mergeV([:]).option(Merge.onCreate, xx1)
      """
    When iterated to list
    Then the traversal will raise an error

  # cannot use hidden namespace for label key for onCreate
  Scenario: g_mergeV_hidden_label_key_onCreate_prohibited
    Given the empty graph
    And using the parameter xx1 defined as "m[{\"~label\":\"vertex\"}]"
    And the traversal of
      """
      g.mergeV([:]).option(Merge.onCreate, xx1)
      """
    When iterated to list
    Then the traversal will raise an error

  # cannot use hidden namespace for label value for onCreate
  Scenario: g_mergeV_hidden_label_value_onCreate_prohibited
    Given the empty graph
    And using the parameter xx1 defined as "m[{\"t[label]\":\"~vertex\"}]"
    And the traversal of
      """
      g.mergeV([:]).option(Merge.onCreate, xx1)
      """
    When iterated to list
    Then the traversal will raise an error

  # cannot use hidden namespace for id key for onMatch
  Scenario: g_mergeV_hidden_id_key_onMatch_matched_prohibited
    Given the empty graph
    And the graph initializer of
      """
      g.addV("vertex")
      """
    And using the parameter xx1 defined as "m[{\"~id\": 1}]"
    And the traversal of
      """
      g.mergeV([:]).option(Merge.onMatch, xx1)
      """
    When iterated to list
    Then the traversal will raise an error

  # cannot use hidden namespace for label key for onMatch
  Scenario: g_mergeV_hidden_label_key_matched_onMatch_matched_prohibited
    Given the empty graph
    And the graph initializer of
      """
      g.addV("vertex")
      """
    And using the parameter xx1 defined as "m[{\"~label\":\"vertex\"}]"
    And the traversal of
      """
      g.mergeV([:]).option(Merge.onMatch, xx1)
      """
    When iterated to list
    Then the traversal will raise an error

  # cannot use hidden namespace for label value for onMatch
  Scenario: g_mergeV_hidden_label_value_onMatch_matched_prohibited
    Given the empty graph
    And the graph initializer of
      """
      g.addV("vertex")
      """
    And using the parameter xx1 defined as "m[{\"t[label]\":\"~vertex\"}]"
    And the traversal of
      """
      g.mergeV([:]).option(Merge.onMatch, xx1)
      """
    When iterated to list
    Then the traversal will raise an error
