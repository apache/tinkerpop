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

Feature: Step - addV()

  Scenario: g_VX1X_addVXanimalX_propertyXage_selectXaX_byXageXX_propertyXname_puppyX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property(T.id, 1).property("name", "marko").property("age", 29).as("marko").
        addV("person").property(T.id, 2).property("name", "vadas").property("age", 27).as("vadas").
        addV("software").property(T.id, 3).property("name", "lop").property("lang", "java").as("lop").
        addV("person").property(T.id, 4).property("name","josh").property("age", 32).as("josh").
        addV("software").property(T.id, 5).property("name", "ripple").property("lang", "java").as("ripple").
        addV("person").property(T.id, 6).property("name", "peter").property("age", 35).as('peter').
        addE("knows").from("marko").to("vadas").property(T.id, 7).property("weight", 0.5).
        addE("knows").from("marko").to("josh").property(T.id, 8).property("weight", 1.0).
        addE("created").from("marko").to("lop").property(T.id, 9).property("weight", 0.4).
        addE("created").from("josh").to("ripple").property(T.id, 10).property("weight", 1.0).
        addE("created").from("josh").to("lop").property(T.id, 11).property("weight", 0.4).
        addE("created").from("peter").to("lop").property(T.id, 12).property("weight", 0.2)
      """
    And using the parameter v1Id defined as "v[marko].id"
    And the traversal of
      """
      g.V(v1Id).as("a").addV("animal").property("age", __.select("a").by("age")).property("name", "puppy")
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 1 for count of "g.V().has(\"animal\",\"age\",29)"

  Scenario: g_V_addVXanimalX_propertyXage_0X
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property(T.id, 1).property("name", "marko").property("age", 29).as("marko").
        addV("person").property(T.id, 2).property("name", "vadas").property("age", 27).as("vadas").
        addV("software").property(T.id, 3).property("name", "lop").property("lang", "java").as("lop").
        addV("person").property(T.id, 4).property("name","josh").property("age", 32).as("josh").
        addV("software").property(T.id, 5).property("name", "ripple").property("lang", "java").as("ripple").
        addV("person").property(T.id, 6).property("name", "peter").property("age", 35).as('peter').
        addE("knows").from("marko").to("vadas").property(T.id, 7).property("weight", 0.5).
        addE("knows").from("marko").to("josh").property(T.id, 8).property("weight", 1.0).
        addE("created").from("marko").to("lop").property(T.id, 9).property("weight", 0.4).
        addE("created").from("josh").to("ripple").property(T.id, 10).property("weight", 1.0).
        addE("created").from("josh").to("lop").property(T.id, 11).property("weight", 0.4).
        addE("created").from("peter").to("lop").property(T.id, 12).property("weight", 0.2)
      """
    And the traversal of
      """
      g.V().addV("animal").property("age", 0)
      """
    When iterated to list
    Then the result should have a count of 6
    And the graph should return 6 for count of "g.V().has(\"animal\",\"age\",0)"

  Scenario: g_addVXpersonX_propertyXname_stephenX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property(T.id, 1).property("name", "marko").property("age", 29).as("marko").
        addV("person").property(T.id, 2).property("name", "vadas").property("age", 27).as("vadas").
        addV("software").property(T.id, 3).property("name", "lop").property("lang", "java").as("lop").
        addV("person").property(T.id, 4).property("name","josh").property("age", 32).as("josh").
        addV("software").property(T.id, 5).property("name", "ripple").property("lang", "java").as("ripple").
        addV("person").property(T.id, 6).property("name", "peter").property("age", 35).as('peter').
        addE("knows").from("marko").to("vadas").property(T.id, 7).property("weight", 0.5).
        addE("knows").from("marko").to("josh").property(T.id, 8).property("weight", 1.0).
        addE("created").from("marko").to("lop").property(T.id, 9).property("weight", 0.4).
        addE("created").from("josh").to("ripple").property(T.id, 10).property("weight", 1.0).
        addE("created").from("josh").to("lop").property(T.id, 11).property("weight", 0.4).
        addE("created").from("peter").to("lop").property(T.id, 12).property("weight", 0.2)
      """
    And the traversal of
      """
      g.addV("person").property("name", "stephen")
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 1 for count of "g.V().has(\"person\",\"name\",\"stephen\")"

  Scenario: g_V_hasLabelXpersonX_propertyXname_nullX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property(T.id, 1).property("name", "marko").property("age", 29).as("marko").
        addV("person").property(T.id, 2).property("name", "vadas").property("age", 27).as("vadas").
        addV("software").property(T.id, 3).property("name", "lop").property("lang", "java").as("lop").
        addV("person").property(T.id, 4).property("name","josh").property("age", 32).as("josh").
        addV("software").property(T.id, 5).property("name", "ripple").property("lang", "java").as("ripple").
        addV("person").property(T.id, 6).property("name", "peter").property("age", 35).as('peter').
        addE("knows").from("marko").to("vadas").property(T.id, 7).property("weight", 0.5).
        addE("knows").from("marko").to("josh").property(T.id, 8).property("weight", 1.0).
        addE("created").from("marko").to("lop").property(T.id, 9).property("weight", 0.4).
        addE("created").from("josh").to("ripple").property(T.id, 10).property("weight", 1.0).
        addE("created").from("josh").to("lop").property(T.id, 11).property("weight", 0.4).
        addE("created").from("peter").to("lop").property(T.id, 12).property("weight", 0.2)
      """
    And the traversal of
      """
      g.V().hasLabel("person").property("name", null)
      """
    When iterated to list
    Then the result should have a count of 4
    And the graph should return 2 for count of "g.V().properties(\"name\")"

  Scenario: g_addVXpersonX_propertyXsingle_name_stephenX_propertyXsingle_name_stephenmX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property(T.id, 1).property("name", "marko").property("age", 29).as("marko").
        addV("person").property(T.id, 2).property("name", "vadas").property("age", 27).as("vadas").
        addV("software").property(T.id, 3).property("name", "lop").property("lang", "java").as("lop").
        addV("person").property(T.id, 4).property("name","josh").property("age", 32).as("josh").
        addV("software").property(T.id, 5).property("name", "ripple").property("lang", "java").as("ripple").
        addV("person").property(T.id, 6).property("name", "peter").property("age", 35).as('peter').
        addE("knows").from("marko").to("vadas").property(T.id, 7).property("weight", 0.5).
        addE("knows").from("marko").to("josh").property(T.id, 8).property("weight", 1.0).
        addE("created").from("marko").to("lop").property(T.id, 9).property("weight", 0.4).
        addE("created").from("josh").to("ripple").property(T.id, 10).property("weight", 1.0).
        addE("created").from("josh").to("lop").property(T.id, 11).property("weight", 0.4).
        addE("created").from("peter").to("lop").property(T.id, 12).property("weight", 0.2)
      """
    And the traversal of
      """
      g.addV("person").property(Cardinality.single, "name", "stephen").property(Cardinality.single, "name", "stephenm")
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 0 for count of "g.V().has(\"person\",\"name\",\"stephen\")"
    And the graph should return 1 for count of "g.V().has(\"person\",\"name\",\"stephenm\")"

  Scenario: get_g_addVXpersonX_propertyXsingle_name_stephenX_propertyXsingle_name_stephenm_since_2010X
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property(T.id, 1).property("name", "marko").property("age", 29).as("marko").
        addV("person").property(T.id, 2).property("name", "vadas").property("age", 27).as("vadas").
        addV("software").property(T.id, 3).property("name", "lop").property("lang", "java").as("lop").
        addV("person").property(T.id, 4).property("name","josh").property("age", 32).as("josh").
        addV("software").property(T.id, 5).property("name", "ripple").property("lang", "java").as("ripple").
        addV("person").property(T.id, 6).property("name", "peter").property("age", 35).as('peter').
        addE("knows").from("marko").to("vadas").property(T.id, 7).property("weight", 0.5).
        addE("knows").from("marko").to("josh").property(T.id, 8).property("weight", 1.0).
        addE("created").from("marko").to("lop").property(T.id, 9).property("weight", 0.4).
        addE("created").from("josh").to("ripple").property(T.id, 10).property("weight", 1.0).
        addE("created").from("josh").to("lop").property(T.id, 11).property("weight", 0.4).
        addE("created").from("peter").to("lop").property(T.id, 12).property("weight", 0.2)
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

  Scenario: g_V_hasXname_markoX_propertyXfriendWeight_outEXknowsX_weight_sum__acl_privateX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property(T.id, 1).property("name", "marko").property("age", 29).as("marko").
        addV("person").property(T.id, 2).property("name", "vadas").property("age", 27).as("vadas").
        addV("software").property(T.id, 3).property("name", "lop").property("lang", "java").as("lop").
        addV("person").property(T.id, 4).property("name","josh").property("age", 32).as("josh").
        addV("software").property(T.id, 5).property("name", "ripple").property("lang", "java").as("ripple").
        addV("person").property(T.id, 6).property("name", "peter").property("age", 35).as('peter').
        addE("knows").from("marko").to("vadas").property(T.id, 7).property("weight", 0.5).
        addE("knows").from("marko").to("josh").property(T.id, 8).property("weight", 1.0).
        addE("created").from("marko").to("lop").property(T.id, 9).property("weight", 0.4).
        addE("created").from("josh").to("ripple").property(T.id, 10).property("weight", 1.0).
        addE("created").from("josh").to("lop").property(T.id, 11).property("weight", 0.4).
        addE("created").from("peter").to("lop").property(T.id, 12).property("weight", 0.2)
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

  Scenario: g_addVXanimalX_propertyXname_mateoX_propertyXname_gateoX_propertyXname_cateoX_propertyXage_5X
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property(T.id, 1).property("name", "marko").property("age", 29).as("marko").
        addV("person").property(T.id, 2).property("name", "vadas").property("age", 27).as("vadas").
        addV("software").property(T.id, 3).property("name", "lop").property("lang", "java").as("lop").
        addV("person").property(T.id, 4).property("name","josh").property("age", 32).as("josh").
        addV("software").property(T.id, 5).property("name", "ripple").property("lang", "java").as("ripple").
        addV("person").property(T.id, 6).property("name", "peter").property("age", 35).as('peter').
        addE("knows").from("marko").to("vadas").property(T.id, 7).property("weight", 0.5).
        addE("knows").from("marko").to("josh").property(T.id, 8).property("weight", 1.0).
        addE("created").from("marko").to("lop").property(T.id, 9).property("weight", 0.4).
        addE("created").from("josh").to("ripple").property(T.id, 10).property("weight", 1.0).
        addE("created").from("josh").to("lop").property(T.id, 11).property("weight", 0.4).
        addE("created").from("peter").to("lop").property(T.id, 12).property("weight", 0.2)
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
      g.addV("person").property(T.id, 1).property("name", "marko").property("age", 29).as("marko").
        addV("person").property(T.id, 2).property("name", "vadas").property("age", 27).as("vadas").
        addV("software").property(T.id, 3).property("name", "lop").property("lang", "java").as("lop").
        addV("person").property(T.id, 4).property("name","josh").property("age", 32).as("josh").
        addV("software").property(T.id, 5).property("name", "ripple").property("lang", "java").as("ripple").
        addV("person").property(T.id, 6).property("name", "peter").property("age", 35).as('peter').
        addE("knows").from("marko").to("vadas").property(T.id, 7).property("weight", 0.5).
        addE("knows").from("marko").to("josh").property(T.id, 8).property("weight", 1.0).
        addE("created").from("marko").to("lop").property(T.id, 9).property("weight", 0.4).
        addE("created").from("josh").to("ripple").property(T.id, 10).property("weight", 1.0).
        addE("created").from("josh").to("lop").property(T.id, 11).property("weight", 0.4).
        addE("created").from("peter").to("lop").property(T.id, 12).property("weight", 0.2)
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

  Scenario: g_addVXpersonX_propertyXsingle_name_stephenX_propertyXsingle_name_stephenm_since_2010X
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property(T.id, 1).property("name", "marko").property("age", 29).as("marko").
        addV("person").property(T.id, 2).property("name", "vadas").property("age", 27).as("vadas").
        addV("software").property(T.id, 3).property("name", "lop").property("lang", "java").as("lop").
        addV("person").property(T.id, 4).property("name","josh").property("age", 32).as("josh").
        addV("software").property(T.id, 5).property("name", "ripple").property("lang", "java").as("ripple").
        addV("person").property(T.id, 6).property("name", "peter").property("age", 35).as('peter').
        addE("knows").from("marko").to("vadas").property(T.id, 7).property("weight", 0.5).
        addE("knows").from("marko").to("josh").property(T.id, 8).property("weight", 1.0).
        addE("created").from("marko").to("lop").property(T.id, 9).property("weight", 0.4).
        addE("created").from("josh").to("ripple").property(T.id, 10).property("weight", 1.0).
        addE("created").from("josh").to("lop").property(T.id, 11).property("weight", 0.4).
        addE("created").from("peter").to("lop").property(T.id, 12).property("weight", 0.2)
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

  Scenario: g_V_addVXanimalX_propertyXname_valuesXnameXX_propertyXname_an_animalX_propertyXvaluesXnameX_labelX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property(T.id, 1).property("name", "marko").property("age", 29).as("marko").
        addV("person").property(T.id, 2).property("name", "vadas").property("age", 27).as("vadas").
        addV("software").property(T.id, 3).property("name", "lop").property("lang", "java").as("lop").
        addV("person").property(T.id, 4).property("name","josh").property("age", 32).as("josh").
        addV("software").property(T.id, 5).property("name", "ripple").property("lang", "java").as("ripple").
        addV("person").property(T.id, 6).property("name", "peter").property("age", 35).as('peter').
        addE("knows").from("marko").to("vadas").property(T.id, 7).property("weight", 0.5).
        addE("knows").from("marko").to("josh").property(T.id, 8).property("weight", 1.0).
        addE("created").from("marko").to("lop").property(T.id, 9).property("weight", 0.4).
        addE("created").from("josh").to("ripple").property(T.id, 10).property("weight", 1.0).
        addE("created").from("josh").to("lop").property(T.id, 11).property("weight", 0.4).
        addE("created").from("peter").to("lop").property(T.id, 12).property("weight", 0.2)
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
      g.addV("person").property(T.id, 1).property("name", "marko").property("age", 29).as("marko").
        addV("person").property(T.id, 2).property("name", "vadas").property("age", 27).as("vadas").
        addV("software").property(T.id, 3).property("name", "lop").property("lang", "java").as("lop").
        addV("person").property(T.id, 4).property("name","josh").property("age", 32).as("josh").
        addV("software").property(T.id, 5).property("name", "ripple").property("lang", "java").as("ripple").
        addV("person").property(T.id, 6).property("name", "peter").property("age", 35).as('peter').
        addE("knows").from("marko").to("vadas").property(T.id, 7).property("weight", 0.5).
        addE("knows").from("marko").to("josh").property(T.id, 8).property("weight", 1.0).
        addE("created").from("marko").to("lop").property(T.id, 9).property("weight", 0.4).
        addE("created").from("josh").to("ripple").property(T.id, 10).property("weight", 1.0).
        addE("created").from("josh").to("lop").property(T.id, 11).property("weight", 0.4).
        addE("created").from("peter").to("lop").property(T.id, 12).property("weight", 0.2)
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
      g.addV("person").property(T.id, 1).property("name", "marko").property("age", 29).as("marko").
        addV("person").property(T.id, 2).property("name", "vadas").property("age", 27).as("vadas").
        addV("software").property(T.id, 3).property("name", "lop").property("lang", "java").as("lop").
        addV("person").property(T.id, 4).property("name","josh").property("age", 32).as("josh").
        addV("software").property(T.id, 5).property("name", "ripple").property("lang", "java").as("ripple").
        addV("person").property(T.id, 6).property("name", "peter").property("age", 35).as('peter').
        addE("knows").from("marko").to("vadas").property(T.id, 7).property("weight", 0.5).
        addE("knows").from("marko").to("josh").property(T.id, 8).property("weight", 1.0).
        addE("created").from("marko").to("lop").property(T.id, 9).property("weight", 0.4).
        addE("created").from("josh").to("ripple").property(T.id, 10).property("weight", 1.0).
        addE("created").from("josh").to("lop").property(T.id, 11).property("weight", 0.4).
        addE("created").from("peter").to("lop").property(T.id, 12).property("weight", 0.2)
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
      g.addV("person").property(T.id, 1).property("name", "marko").property("age", 29).as("marko").
        addV("person").property(T.id, 2).property("name", "vadas").property("age", 27).as("vadas").
        addV("software").property(T.id, 3).property("name", "lop").property("lang", "java").as("lop").
        addV("person").property(T.id, 4).property("name","josh").property("age", 32).as("josh").
        addV("software").property(T.id, 5).property("name", "ripple").property("lang", "java").as("ripple").
        addV("person").property(T.id, 6).property("name", "peter").property("age", 35).as('peter').
        addE("knows").from("marko").to("vadas").property(T.id, 7).property("weight", 0.5).
        addE("knows").from("marko").to("josh").property(T.id, 8).property("weight", 1.0).
        addE("created").from("marko").to("lop").property(T.id, 9).property("weight", 0.4).
        addE("created").from("josh").to("ripple").property(T.id, 10).property("weight", 1.0).
        addE("created").from("josh").to("lop").property(T.id, 11).property("weight", 0.4).
        addE("created").from("peter").to("lop").property(T.id, 12).property("weight", 0.2)
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
      g.addV("person").property(T.id, 1).property("name", "marko").property("age", 29).as("marko").
        addV("person").property(T.id, 2).property("name", "vadas").property("age", 27).as("vadas").
        addV("software").property(T.id, 3).property("name", "lop").property("lang", "java").as("lop").
        addV("person").property(T.id, 4).property("name","josh").property("age", 32).as("josh").
        addV("software").property(T.id, 5).property("name", "ripple").property("lang", "java").as("ripple").
        addV("person").property(T.id, 6).property("name", "peter").property("age", 35).as('peter').
        addE("knows").from("marko").to("vadas").property(T.id, 7).property("weight", 0.5).
        addE("knows").from("marko").to("josh").property(T.id, 8).property("weight", 1.0).
        addE("created").from("marko").to("lop").property(T.id, 9).property("weight", 0.4).
        addE("created").from("josh").to("ripple").property(T.id, 10).property("weight", 1.0).
        addE("created").from("josh").to("lop").property(T.id, 11).property("weight", 0.4).
        addE("created").from("peter").to("lop").property(T.id, 12).property("weight", 0.2)
      """
    And the traversal of
      """
      g.addV(__.V().has("name", "marko").properties("name").key()).label()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | name |

  Scenario: g_addVXnullX_propertyXid_nullX
    Given the empty graph
    And the traversal of
      """
      g.addV(null).property(T.id, null)
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 1 for count of "g.V().hasLabel(\"vertex\")"

  Scenario: g_addV_propertyXlabel_personX
    Given the empty graph
    And the traversal of
      """
      g.addV().property(T.label, "person")
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 1 for count of "g.V().hasLabel(\"person\")"
