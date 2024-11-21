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

@StepClassMap @StepAddV
Feature: Step - addV()

  Scenario: g_VX1X_addVXanimalX_propertyXage_selectXaX_byXageXX_propertyXname_puppyX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", "marko").property("age", 29).as("marko").
        addV("person").property("name", "vadas").property("age", 27).as("vadas").
        addV("software").property("name", "lop").property("lang", "java").as("lop").
        addV("person").property("name","josh").property("age", 32).as("josh").
        addV("software").property("name", "ripple").property("lang", "java").as("ripple").
        addV("person").property("name", "peter").property("age", 35).as('peter').
        addE("knows").from("marko").to("vadas").property("weight", 0.5d).
        addE("knows").from("marko").to("josh").property("weight", 1.0d).
        addE("created").from("marko").to("lop").property("weight", 0.4d).
        addE("created").from("josh").to("ripple").property("weight", 1.0d).
        addE("created").from("josh").to("lop").property("weight", 0.4d).
        addE("created").from("peter").to("lop").property("weight", 0.2d)
      """
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).as("a").addV("animal").property("age", __.select("a").by("age")).property("name", "puppy")
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 1 for count of "g.V().has(\"animal\",\"age\",29)"

  Scenario: g_V_addVXanimalX_propertyXage_0X
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", "marko").property("age", 29).as("marko").
        addV("person").property("name", "vadas").property("age", 27).as("vadas").
        addV("software").property("name", "lop").property("lang", "java").as("lop").
        addV("person").property("name","josh").property("age", 32).as("josh").
        addV("software").property("name", "ripple").property("lang", "java").as("ripple").
        addV("person").property("name", "peter").property("age", 35).as('peter').
        addE("knows").from("marko").to("vadas").property("weight", 0.5d).
        addE("knows").from("marko").to("josh").property("weight", 1.0d).
        addE("created").from("marko").to("lop").property("weight", 0.4d).
        addE("created").from("josh").to("ripple").property("weight", 1.0d).
        addE("created").from("josh").to("lop").property("weight", 0.4d).
        addE("created").from("peter").to("lop").property("weight", 0.2d)
      """
    And the traversal of
      """
      g.V().addV("animal").property("age", 0)
      """
    When iterated to list
    Then the result should have a count of 6
    And the graph should return 6 for count of "g.V().has(\"animal\",\"age\",0)"

  Scenario: g_V_addVXanimalvarX_propertyXage_0varX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", "marko").property("age", 29).as("marko").
        addV("person").property("name", "vadas").property("age", 27).as("vadas").
        addV("software").property("name", "lop").property("lang", "java").as("lop").
        addV("person").property("name","josh").property("age", 32).as("josh").
        addV("software").property("name", "ripple").property("lang", "java").as("ripple").
        addV("person").property("name", "peter").property("age", 35).as('peter').
        addE("knows").from("marko").to("vadas").property("weight", 0.5d).
        addE("knows").from("marko").to("josh").property("weight", 1.0d).
        addE("created").from("marko").to("lop").property("weight", 0.4d).
        addE("created").from("josh").to("ripple").property("weight", 1.0d).
        addE("created").from("josh").to("lop").property("weight", 0.4d).
        addE("created").from("peter").to("lop").property("weight", 0.2d)
      """
    And using the parameter xx1 defined as "animal"
    And using the parameter xx2 defined as "d[0].i"
    And the traversal of
      """
      g.V().addV(xx1).property("age", xx2)
      """
    When iterated to list
    Then the result should have a count of 6
    And the graph should return 6 for count of "g.V().has(\"animal\",\"age\",0)"

  Scenario: g_addVXpersonX_propertyXname_stephenX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", "marko").property("age", 29).as("marko").
        addV("person").property("name", "vadas").property("age", 27).as("vadas").
        addV("software").property("name", "lop").property("lang", "java").as("lop").
        addV("person").property("name","josh").property("age", 32).as("josh").
        addV("software").property("name", "ripple").property("lang", "java").as("ripple").
        addV("person").property("name", "peter").property("age", 35).as('peter').
        addE("knows").from("marko").to("vadas").property("weight", 0.5d).
        addE("knows").from("marko").to("josh").property("weight", 1.0d).
        addE("created").from("marko").to("lop").property("weight", 0.4d).
        addE("created").from("josh").to("ripple").property("weight", 1.0d).
        addE("created").from("josh").to("lop").property("weight", 0.4d).
        addE("created").from("peter").to("lop").property("weight", 0.2d)
      """
    And the traversal of
      """
      g.addV("person").property("name", "stephen")
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 1 for count of "g.V().has(\"person\",\"name\",\"stephen\")"

  Scenario: g_addVXpersonvarX_propertyXname_stephenvarX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", "marko").property("age", 29).as("marko").
        addV("person").property("name", "vadas").property("age", 27).as("vadas").
        addV("software").property("name", "lop").property("lang", "java").as("lop").
        addV("person").property("name","josh").property("age", 32).as("josh").
        addV("software").property("name", "ripple").property("lang", "java").as("ripple").
        addV("person").property("name", "peter").property("age", 35).as('peter').
        addE("knows").from("marko").to("vadas").property("weight", 0.5d).
        addE("knows").from("marko").to("josh").property("weight", 1.0d).
        addE("created").from("marko").to("lop").property("weight", 0.4d).
        addE("created").from("josh").to("ripple").property("weight", 1.0d).
        addE("created").from("josh").to("lop").property("weight", 0.4d).
        addE("created").from("peter").to("lop").property("weight", 0.2d)
      """
    And using the parameter xx1 defined as "person"
    And using the parameter xx2 defined as "stephen"
    And the traversal of
      """
      g.addV(xx1).property("name", xx2)
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 1 for count of "g.V().has(\"person\",\"name\",\"stephen\")"

  Scenario: g_V_hasLabelXpersonX_propertyXname_nullX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", "marko").property("age", 29).as("marko").
        addV("person").property("name", "vadas").property("age", 27).as("vadas").
        addV("software").property("name", "lop").property("lang", "java").as("lop").
        addV("person").property("name","josh").property("age", 32).as("josh").
        addV("software").property("name", "ripple").property("lang", "java").as("ripple").
        addV("person").property("name", "peter").property("age", 35).as('peter').
        addE("knows").from("marko").to("vadas").property("weight", 0.5d).
        addE("knows").from("marko").to("josh").property("weight", 1.0d).
        addE("created").from("marko").to("lop").property("weight", 0.4d).
        addE("created").from("josh").to("ripple").property("weight", 1.0d).
        addE("created").from("josh").to("lop").property("weight", 0.4d).
        addE("created").from("peter").to("lop").property("weight", 0.2d)
      """
    And the traversal of
      """
      g.V().hasLabel("person").property(Cardinality.single, "name", null)
      """
    When iterated to list
    Then the result should have a count of 4
    And the graph should return 2 for count of "g.V().properties(\"name\")"

  Scenario: g_addVXpersonX_propertyXsingle_name_stephenX_propertyXsingle_name_stephenmX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", "marko").property("age", 29).as("marko").
        addV("person").property("name", "vadas").property("age", 27).as("vadas").
        addV("software").property("name", "lop").property("lang", "java").as("lop").
        addV("person").property("name","josh").property("age", 32).as("josh").
        addV("software").property("name", "ripple").property("lang", "java").as("ripple").
        addV("person").property("name", "peter").property("age", 35).as('peter').
        addE("knows").from("marko").to("vadas").property("weight", 0.5d).
        addE("knows").from("marko").to("josh").property("weight", 1.0d).
        addE("created").from("marko").to("lop").property("weight", 0.4d).
        addE("created").from("josh").to("ripple").property("weight", 1.0d).
        addE("created").from("josh").to("lop").property("weight", 0.4d).
        addE("created").from("peter").to("lop").property("weight", 0.2d)
      """
    And the traversal of
      """
      g.addV("person").property(Cardinality.single, "name", "stephen").property(Cardinality.single, "name", "stephenm")
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 0 for count of "g.V().has(\"person\",\"name\",\"stephen\")"
    And the graph should return 1 for count of "g.V().has(\"person\",\"name\",\"stephenm\")"

  @MetaProperties
  Scenario: get_g_addVXpersonX_propertyXsingle_name_stephenX_propertyXsingle_name_stephenm_since_2010X
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", "marko").property("age", 29).as("marko").
        addV("person").property("name", "vadas").property("age", 27).as("vadas").
        addV("software").property("name", "lop").property("lang", "java").as("lop").
        addV("person").property("name","josh").property("age", 32).as("josh").
        addV("software").property("name", "ripple").property("lang", "java").as("ripple").
        addV("person").property("name", "peter").property("age", 35).as('peter').
        addE("knows").from("marko").to("vadas").property("weight", 0.5d).
        addE("knows").from("marko").to("josh").property("weight", 1.0d).
        addE("created").from("marko").to("lop").property("weight", 0.4d).
        addE("created").from("josh").to("ripple").property("weight", 1.0d).
        addE("created").from("josh").to("lop").property("weight", 0.4d).
        addE("created").from("peter").to("lop").property("weight", 0.2d)
      """
    And the traversal of
      """
      g.addV("person").property(Cardinality.single, "name", "stephen").property(Cardinality.single, "name", "stephenm", "since", 2010)
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 0 for count of "g.V().has(\"person\",\"name\",\"stephen\")"
    And the graph should return 1 for count of "g.V().has(\"person\",\"name\",\"stephenm\")"
    And the graph should return 1 for count of "g.V().has(\"person\",\"name\",\"stephenm\").properties(\"name\").has(\"since\",2010)"

  @MetaProperties
  Scenario: g_V_hasXname_markoX_propertyXfriendWeight_outEXknowsX_weight_sum__acl_privateX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", "marko").property("age", 29).as("marko").
        addV("person").property("name", "vadas").property("age", 27).as("vadas").
        addV("software").property("name", "lop").property("lang", "java").as("lop").
        addV("person").property("name","josh").property("age", 32).as("josh").
        addV("software").property("name", "ripple").property("lang", "java").as("ripple").
        addV("person").property("name", "peter").property("age", 35).as('peter').
        addE("knows").from("marko").to("vadas").property("weight", 0.5d).
        addE("knows").from("marko").to("josh").property("weight", 1.0d).
        addE("created").from("marko").to("lop").property("weight", 0.4d).
        addE("created").from("josh").to("ripple").property("weight", 1.0d).
        addE("created").from("josh").to("lop").property("weight", 0.4d).
        addE("created").from("peter").to("lop").property("weight", 0.2d)
      """
    And the traversal of
      """
      g.V().has("name", "marko").property("friendWeight", __.outE("knows").values("weight").sum(), "acl", "private")
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 1 for count of "g.V().has(\"person\",\"name\",\"marko\").has(\"friendWeight\", 1.5)"
    And the graph should return 1 for count of "g.V().has(\"person\",\"name\",\"marko\").properties(\"friendWeight\").has(\"acl\",\"private\")"
    And the graph should return 1 for count of "g.V().has(\"person\",\"name\",\"marko\").properties(\"friendWeight\").count()"

  @MultiProperties
  Scenario: g_addVXanimalX_propertyXname_mateoX_propertyXname_gateoX_propertyXname_cateoX_propertyXage_5X
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", "marko").property("age", 29).as("marko").
        addV("person").property("name", "vadas").property("age", 27).as("vadas").
        addV("software").property("name", "lop").property("lang", "java").as("lop").
        addV("person").property("name","josh").property("age", 32).as("josh").
        addV("software").property("name", "ripple").property("lang", "java").as("ripple").
        addV("person").property("name", "peter").property("age", 35).as('peter').
        addE("knows").from("marko").to("vadas").property("weight", 0.5d).
        addE("knows").from("marko").to("josh").property("weight", 1.0d).
        addE("created").from("marko").to("lop").property("weight", 0.4d).
        addE("created").from("josh").to("ripple").property("weight", 1.0d).
        addE("created").from("josh").to("lop").property("weight", 0.4d).
        addE("created").from("peter").to("lop").property("weight", 0.2d)
      """
    And the traversal of
      """
      g.addV("animal").property("name", "mateo").property("name", "gateo").property("name", "cateo").property("age", 5)
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 1 for count of "g.V().hasLabel(\"animal\").has(\"name\",\"mateo\").has(\"name\", \"gateo\").has(\"name\", \"cateo\").has(\"age\",5)"

  Scenario: g_withSideEffectXa_markoX_addV_propertyXname_selectXaXX_name
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", "marko").property("age", 29).as("marko").
        addV("person").property("name", "vadas").property("age", 27).as("vadas").
        addV("software").property("name", "lop").property("lang", "java").as("lop").
        addV("person").property("name","josh").property("age", 32).as("josh").
        addV("software").property("name", "ripple").property("lang", "java").as("ripple").
        addV("person").property("name", "peter").property("age", 35).as('peter').
        addE("knows").from("marko").to("vadas").property("weight", 0.5d).
        addE("knows").from("marko").to("josh").property("weight", 1.0d).
        addE("created").from("marko").to("lop").property("weight", 0.4d).
        addE("created").from("josh").to("ripple").property("weight", 1.0d).
        addE("created").from("josh").to("lop").property("weight", 0.4d).
        addE("created").from("peter").to("lop").property("weight", 0.2d)
      """
    And the traversal of
      """
      g.withSideEffect("a", "marko").addV().property("name", __.select("a")).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko |
    And the graph should return 2 for count of "g.V().has(\"name\",\"marko\")"

  @MultiProperties @MetaProperties
  Scenario: g_addVXpersonX_propertyXsingle_name_stephenX_propertyXsingle_name_stephenm_since_2010X
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", "marko").property("age", 29).as("marko").
        addV("person").property("name", "vadas").property("age", 27).as("vadas").
        addV("software").property("name", "lop").property("lang", "java").as("lop").
        addV("person").property("name","josh").property("age", 32).as("josh").
        addV("software").property("name", "ripple").property("lang", "java").as("ripple").
        addV("person").property("name", "peter").property("age", 35).as('peter').
        addE("knows").from("marko").to("vadas").property("weight", 0.5d).
        addE("knows").from("marko").to("josh").property("weight", 1.0d).
        addE("created").from("marko").to("lop").property("weight", 0.4d).
        addE("created").from("josh").to("ripple").property("weight", 1.0d).
        addE("created").from("josh").to("lop").property("weight", 0.4d).
        addE("created").from("peter").to("lop").property("weight", 0.2d)
      """
    And the traversal of
      """
      g.addV("person").property(Cardinality.single, "name", "stephen").property(Cardinality.single, "name", "stephenm", "since", 2010)
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 0 for count of "g.V().has(\"name\",\"stephen\")"
    And the graph should return 1 for count of "g.V().has(\"name\",\"stephenm\")"
    And the graph should return 1 for count of "g.V().has(\"name\",\"stephenm\").properties(\"name\").has(\"since\",2010)"

  @MultiProperties
  Scenario: g_V_addVXanimalX_propertyXname_valuesXnameXX_propertyXname_an_animalX_propertyXvaluesXnameX_labelX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", "marko").property("age", 29).as("marko").
        addV("person").property("name", "vadas").property("age", 27).as("vadas").
        addV("software").property("name", "lop").property("lang", "java").as("lop").
        addV("person").property("name","josh").property("age", 32).as("josh").
        addV("software").property("name", "ripple").property("lang", "java").as("ripple").
        addV("person").property("name", "peter").property("age", 35).as('peter').
        addE("knows").from("marko").to("vadas").property("weight", 0.5d).
        addE("knows").from("marko").to("josh").property("weight", 1.0d).
        addE("created").from("marko").to("lop").property("weight", 0.4d).
        addE("created").from("josh").to("ripple").property("weight", 1.0d).
        addE("created").from("josh").to("lop").property("weight", 0.4d).
        addE("created").from("peter").to("lop").property("weight", 0.2d)
      """
    And the traversal of
      """
      g.V().addV("animal").property("name", __.values("name")).property("name", "an animal").property(__.values("name"), __.label())
      """
    When iterated to list
    Then the result should have a count of 6
    And the graph should return 1 for count of "g.V().hasLabel(\"animal\").has(\"name\",\"marko\").has(\"name\",\"an animal\").has(\"marko\",\"person\")"
    And the graph should return 1 for count of "g.V().hasLabel(\"animal\").has(\"name\",\"vadas\").has(\"name\",\"an animal\").has(\"vadas\",\"person\")"
    And the graph should return 1 for count of "g.V().hasLabel(\"animal\").has(\"name\",\"lop\").has(\"name\",\"an animal\").has(\"lop\",\"software\")"
    And the graph should return 1 for count of "g.V().hasLabel(\"animal\").has(\"name\",\"josh\").has(\"name\",\"an animal\").has(\"josh\",\"person\")"
    And the graph should return 1 for count of "g.V().hasLabel(\"animal\").has(\"name\",\"ripple\").has(\"name\",\"an animal\").has(\"ripple\",\"software\")"
    And the graph should return 1 for count of "g.V().hasLabel(\"animal\").has(\"name\",\"peter\").has(\"name\",\"an animal\").has(\"peter\",\"person\")"

  Scenario: g_withSideEffectXa_testX_V_hasLabelXsoftwareX_propertyXtemp_selectXaXX_valueMapXname_tempX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", "marko").property("age", 29).as("marko").
        addV("person").property("name", "vadas").property("age", 27).as("vadas").
        addV("software").property("name", "lop").property("lang", "java").as("lop").
        addV("person").property("name","josh").property("age", 32).as("josh").
        addV("software").property("name", "ripple").property("lang", "java").as("ripple").
        addV("person").property("name", "peter").property("age", 35).as('peter').
        addE("knows").from("marko").to("vadas").property("weight", 0.5d).
        addE("knows").from("marko").to("josh").property("weight", 1.0d).
        addE("created").from("marko").to("lop").property("weight", 0.4d).
        addE("created").from("josh").to("ripple").property("weight", 1.0d).
        addE("created").from("josh").to("lop").property("weight", 0.4d).
        addE("created").from("peter").to("lop").property("weight", 0.2d)
      """
    And the traversal of
      """
      g.withSideEffect("a", "test").V().hasLabel("software").property("temp", __.select("a")).valueMap("name", "temp")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"temp": ["test"], "name": ["lop"]}] |
      | m[{"temp": ["test"], "name": ["ripple"]}] |

  Scenario: g_withSideEffectXa_nameX_addV_propertyXselectXaX_markoX_name
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", "marko").property("age", 29).as("marko").
        addV("person").property("name", "vadas").property("age", 27).as("vadas").
        addV("software").property("name", "lop").property("lang", "java").as("lop").
        addV("person").property("name","josh").property("age", 32).as("josh").
        addV("software").property("name", "ripple").property("lang", "java").as("ripple").
        addV("person").property("name", "peter").property("age", 35).as('peter').
        addE("knows").from("marko").to("vadas").property("weight", 0.5d).
        addE("knows").from("marko").to("josh").property("weight", 1.0d).
        addE("created").from("marko").to("lop").property("weight", 0.4d).
        addE("created").from("josh").to("ripple").property("weight", 1.0d).
        addE("created").from("josh").to("lop").property("weight", 0.4d).
        addE("created").from("peter").to("lop").property("weight", 0.2d)
      """
    And the traversal of
      """
      g.withSideEffect("a", "name").addV().property(__.select("a"), "marko").values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko |
    And the graph should return 2 for count of "g.V().has(\"name\",\"marko\")"

  Scenario: g_V_asXaX_hasXname_markoX_outXcreatedX_asXbX_addVXselectXaX_labelX_propertyXtest_selectXbX_labelX_valueMap_withXtokensX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", "marko").property("age", 29).as("marko").
        addV("person").property("name", "vadas").property("age", 27).as("vadas").
        addV("software").property("name", "lop").property("lang", "java").as("lop").
        addV("person").property("name","josh").property("age", 32).as("josh").
        addV("software").property("name", "ripple").property("lang", "java").as("ripple").
        addV("person").property("name", "peter").property("age", 35).as('peter').
        addE("knows").from("marko").to("vadas").property("weight", 0.5d).
        addE("knows").from("marko").to("josh").property("weight", 1.0d).
        addE("created").from("marko").to("lop").property("weight", 0.4d).
        addE("created").from("josh").to("ripple").property("weight", 1.0d).
        addE("created").from("josh").to("lop").property("weight", 0.4d).
        addE("created").from("peter").to("lop").property("weight", 0.2d)
      """
    And the traversal of
      """
      g.V().as("a").has("name", "marko").out("created").as("b").addV(__.select("a").label()).property("test", __.select("b").label()).valueMap().with(WithOptions.tokens)
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 1 for count of "g.V().has(\"person\",\"test\",\"software\")"

  Scenario: g_addVXV_hasXname_markoX_propertiesXnameX_keyX_label
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", "marko").property("age", 29).as("marko").
        addV("person").property("name", "vadas").property("age", 27).as("vadas").
        addV("software").property("name", "lop").property("lang", "java").as("lop").
        addV("person").property("name","josh").property("age", 32).as("josh").
        addV("software").property("name", "ripple").property("lang", "java").as("ripple").
        addV("person").property("name", "peter").property("age", 35).as('peter').
        addE("knows").from("marko").to("vadas").property("weight", 0.5d).
        addE("knows").from("marko").to("josh").property("weight", 1.0d).
        addE("created").from("marko").to("lop").property("weight", 0.4d).
        addE("created").from("josh").to("ripple").property("weight", 1.0d).
        addE("created").from("josh").to("lop").property("weight", 0.4d).
        addE("created").from("peter").to("lop").property("weight", 0.2d)
      """
    And the traversal of
      """
      g.addV(__.V().has("name", "marko").properties("name").key()).label()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | name |

  Scenario: g_addV_propertyXlabel_personX
    Given the empty graph
    And the traversal of
      """
      g.addV().property(T.label, "person")
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 1 for count of "g.V().hasLabel(\"person\")"

  Scenario: g_addV_propertyXlabel_personvarX
    Given the empty graph
    And using the parameter xx1 defined as "person"
    And the traversal of
      """
      g.addV().property(T.label, xx1)
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 1 for count of "g.V().hasLabel(\"person\")"

  @UserSuppliedVertexIds
  Scenario: g_addV_propertyXid_1X
    Given the empty graph
    And the traversal of
      """
      g.addV().property(T.id, 1)
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 1 for count of "g.V().hasId(\"1\")"

  @UserSuppliedVertexIds
  Scenario: g_addV_propertyXidvar_1varX
    Given the empty graph
    And using the parameter xx1 defined as "1"
    And the traversal of
      """
      g.addV().property(T.id, xx1)
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 1 for count of "g.V().hasId(\"1\")"

  Scenario: g_addV_propertyXmapX
    Given the empty graph
    And the traversal of
      """
      g.addV().property(["name": "foo", "age": 42 ])
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 1 for count of "g.V().has(\"name\",\"foo\")"

  Scenario: g_addV_propertyXsingle_mapX
    Given the empty graph
    And the traversal of
      """
      g.addV().property(Cardinality.single, ["name": "foo", "age": 42 ])
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 1 for count of "g.V().has(\"name\",\"foo\")"

  @MultiProperties
  Scenario: g_V_hasXname_fooX_propertyXname_setXbarX_age_43X
    Given the empty graph
    And the graph initializer of
      """
      g.addV().property(Cardinality.single, "name", "foo").property("age", 42)
      """
    And the traversal of
      """
      g.V().has('name','foo').property(["name": Cardinality.set("bar"), "age": 43 ])
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 1 for count of "g.V().has(\"name\",\"foo\")"
    And the graph should return 1 for count of "g.V().has(\"name\",\"bar\")"
    And the graph should return 1 for count of "g.V().has(\"age\",43)"
    And the graph should return 0 for count of "g.V().has(\"age\",42)"

  @MultiProperties
  Scenario: g_V_hasXname_fooX_propertyXset_name_bar_age_singleX43XX
    Given the empty graph
    And the graph initializer of
      """
      g.addV().property(Cardinality.single, "name", "foo").property("age", 42)
      """
    And the traversal of
      """
      g.V().has('name','foo').property(Cardinality.set, ["name":"bar", "age": Cardinality.single(43) ])
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 1 for count of "g.V().has(\"name\",\"foo\")"
    And the graph should return 1 for count of "g.V().has(\"name\",\"bar\")"
    And the graph should return 1 for count of "g.V().has(\"age\",43)"
    And the graph should return 0 for count of "g.V().has(\"age\",42)"

  Scenario: g_addV_propertyXnullX
    Given the empty graph
    And the traversal of
      """
      g.addV("person").property(null)
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 0 for count of "g.V().hasLabel(\"person\").values()"


  Scenario: g_addV_propertyXemptyX
    Given the empty graph
    And the traversal of
      """
      g.addV("person").property([:])
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 0 for count of "g.V().hasLabel(\"person\").values()"


  Scenario: g_addV_propertyXset_nullX
    Given the empty graph
    And the traversal of
      """
      g.addV("foo").property(set, null)
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 0 for count of "g.V().hasLabel(\"foo\").values()"


  Scenario: g_addV_propertyXset_emptyX
    Given the empty graph
    And the traversal of
      """
      g.addV("foo").property(set, [:])
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 0 for count of "g.V().hasLabel(\"person\").values()"

  @AllowNullPropertyValues
  Scenario: g_addVXpersonX_propertyXname_joshX_propertyXage_nullX
    Given the empty graph
    And the traversal of
      """
      g.addV("person").property("name", "josh").property("age", null)
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 1 for count of "g.V().has(\"person\",\"age\",null)"

  @AllowNullPropertyValues @MetaProperties
  Scenario: g_addVXpersonX_propertyXname_markoX_propertyXfriendWeight_null_acl_nullX
    Given the empty graph
    And the traversal of
      """
      g.addV("person").property("name", "marko").property("friendWeight", null, "acl", null)
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 1 for count of "g.V().has(\"person\",\"name\",\"marko\").has(\"friendWeight\", null)"
    And the graph should return 1 for count of "g.V().has(\"person\",\"name\",\"marko\").properties(\"friendWeight\").has(\"acl\",null)"
    And the graph should return 1 for count of "g.V().has(\"person\",\"name\",\"marko\").properties(\"friendWeight\").count()"

  Scenario: g_V_hasXperson_name_aliceX_propertyXsingle_age_unionXage_constantX1XX_sumX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", "alice").property(single, "age", 50)
      """
    And the traversal of
      """
      g.V().has("person","name","alice").property("age", __.union(__.values("age"), constant(1)).sum())
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 0 for count of "g.V().has(\"person\",\"age\",50)"
    And the graph should return 1 for count of "g.V().has(\"person\",\"age\",51)"

  Scenario: g_V_limitX3X_addVXsoftwareX_aggregateXa1X_byXlabelX_aggregateXa2X_byXlabelX_capXa1_a2X_selectXa_bX_byXunfoldX_foldX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", "marko").property("age", 29).as("marko").
        addV("person").property("name", "vadas").property("age", 27).as("vadas").
        addV("software").property("name", "lop").property("lang", "java").as("lop").
        addV("person").property("name","josh").property("age", 32).as("josh").
        addV("software").property("name", "ripple").property("lang", "java").as("ripple").
        addV("person").property("name", "peter").property("age", 35).as('peter').
        addE("knows").from("marko").to("vadas").property("weight", 0.5d).
        addE("knows").from("marko").to("josh").property("weight", 1.0d).
        addE("created").from("marko").to("lop").property("weight", 0.4d).
        addE("created").from("josh").to("ripple").property("weight", 1.0d).
        addE("created").from("josh").to("lop").property("weight", 0.4d).
        addE("created").from("peter").to("lop").property("weight", 0.2d)
      """
    And the traversal of
      """
      g.V().limit(3).addV("software").aggregate("a1").by(T.label).aggregate("a2").by(T.label).cap("a1", "a2").
        select("a1","a2").by(unfold().fold())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a1": ["software", "software", "software"], "a2": ["software", "software", "software"]}] |