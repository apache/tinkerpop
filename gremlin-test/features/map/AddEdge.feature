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

Feature: Step - addE()

  Scenario: g_VX1X_asXaX_outXcreatedX_addOutEXcreatedBy_aX
    Given an unsupported test
    Then nothing should happen because
      """
      This test is deprecated.
      """

  Scenario: g_VX1X_asXaX_outXcreatedX_addEXcreatedByX_toXaX
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
      g.V(v1Id).as("a").out("created").addE("createdBy").to("a")
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 7 for count of "g.E()"
    And the graph should return 1 for count of "g.V(v1Id).inE()"

  Scenario: g_VX1X_asXaX_outXcreatedX_addOutEXcreatedBy_a_weight_2X
    Given an unsupported test
    Then nothing should happen because
      """
      This test is deprecated.
      """

  Scenario: g_VX1X_asXaX_outXcreatedX_addEXcreatedByX_toXaX_propertyXweight_2X
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
      g.V(v1Id).as("a").out("created").addE("createdBy").to("a").property("weight", 2.0)
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 7 for count of "g.E()"
    And the graph should return 4 for count of "g.V(v1Id).bothE()"
    And the graph should return 1 for count of "g.V(v1Id).inE().has(\"weight\", 2.0)"

  Scenario: g_withSideEffectXx__g_V_toListX_addOutEXexistsWith_x_time_nowX
    Given an unsupported test
    Then nothing should happen because
      """
      This test is marked as @Ignored in the test suite.
      """

  Scenario: g_V_aggregateXxX_asXaX_selectXxX_unfold_addEXexistsWithX_toXaX_propertyXtime_nowX
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
    And using the parameter v2Id defined as "v[vadas].id"
    And using the parameter v3Id defined as "v[lop].id"
    And using the parameter v4Id defined as "v[josh].id"
    And using the parameter v5Id defined as "v[ripple].id"
    And using the parameter v6Id defined as "v[peter].id"
    And the traversal of
      """
      g.V().aggregate("x").as("a").select("x").unfold().addE("existsWith").to("a").property("time", "now")
      """
    When iterated to list
    Then the result should have a count of 36
    And the graph should return 42 for count of "g.E()"
    And the graph should return 15 for count of "g.V(v1Id).bothE()"
    And the graph should return 6 for count of "g.V(v1Id).inE(\"existsWith\")"
    And the graph should return 6 for count of "g.V(v1Id).outE(\"existsWith\")"
    And the graph should return 12 for count of "g.V(v1Id).bothE(\"existsWith\").has(\"time\",\"now\")"
    And the graph should return 13 for count of "g.V(v2Id).bothE()"
    And the graph should return 6 for count of "g.V(v2Id).inE(\"existsWith\")"
    And the graph should return 6 for count of "g.V(v2Id).outE(\"existsWith\")"
    And the graph should return 12 for count of "g.V(v2Id).bothE(\"existsWith\").has(\"time\",\"now\")"
    And the graph should return 15 for count of "g.V(v3Id).bothE()"
    And the graph should return 6 for count of "g.V(v3Id).inE(\"existsWith\")"
    And the graph should return 6 for count of "g.V(v3Id).outE(\"existsWith\")"
    And the graph should return 12 for count of "g.V(v3Id).bothE(\"existsWith\").has(\"time\",\"now\")"
    And the graph should return 15 for count of "g.V(v4Id).bothE()"
    And the graph should return 6 for count of "g.V(v4Id).inE(\"existsWith\")"
    And the graph should return 6 for count of "g.V(v4Id).outE(\"existsWith\")"
    And the graph should return 12 for count of "g.V(v4Id).bothE(\"existsWith\").has(\"time\",\"now\")"
    And the graph should return 13 for count of "g.V(v5Id).bothE()"
    And the graph should return 6 for count of "g.V(v5Id).inE(\"existsWith\")"
    And the graph should return 6 for count of "g.V(v5Id).outE(\"existsWith\")"
    And the graph should return 12 for count of "g.V(v5Id).bothE(\"existsWith\").has(\"time\",\"now\")"
    And the graph should return 13 for count of "g.V(v6Id).bothE()"
    And the graph should return 6 for count of "g.V(v6Id).inE(\"existsWith\")"
    And the graph should return 6 for count of "g.V(v6Id).outE(\"existsWith\")"
    And the graph should return 12 for count of "g.V(v6Id).bothE(\"existsWith\").has(\"time\",\"now\")"

  Scenario: g_V_asXaX_outXcreatedX_inXcreatedX_whereXneqXaXX_asXbX_selectXa_bX_addInEXa_codeveloper_b_year_2009X
    Given an unsupported test
    Then nothing should happen because
      """
      This test is deprecated.
      """

  Scenario: g_V_asXaX_outXcreatedX_inXcreatedX_whereXneqXaXX_asXbX_addEXcodeveloperX_fromXaX_toXbX_propertyXyear_2009X
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
    And using the parameter v2Id defined as "v[vadas].id"
    And using the parameter v4Id defined as "v[josh].id"
    And using the parameter v6Id defined as "v[peter].id"
    And the traversal of
      """
      g.V().as("a").out("created").in("created").where(P.neq("a")).as("b").addE("codeveloper").from("a").to("b").property("year", 2009)
      """
    When iterated to list
    Then the result should have a count of 6
    And the graph should return 12 for count of "g.E()"
    And the graph should return 7 for count of "g.V(v1Id).bothE()"
    And the graph should return 2 for count of "g.V(v1Id).inE(\"codeveloper\")"
    And the graph should return 2 for count of "g.V(v1Id).outE(\"codeveloper\")"
    And the graph should return 4 for count of "g.V(v1Id).bothE(\"codeveloper\").has(\"year\",2009)"
    And the graph should return 1 for count of "g.V(v2Id).bothE()"
    And the graph should return 7 for count of "g.V(v4Id).bothE()"
    And the graph should return 2 for count of "g.V(v4Id).inE(\"codeveloper\")"
    And the graph should return 2 for count of "g.V(v4Id).outE(\"codeveloper\")"
    And the graph should return 4 for count of "g.V(v4Id).bothE(\"codeveloper\").has(\"year\",2009)"
    And the graph should return 5 for count of "g.V(v6Id).bothE()"
    And the graph should return 2 for count of "g.V(v6Id).inE(\"codeveloper\")"
    And the graph should return 2 for count of "g.V(v6Id).outE(\"codeveloper\")"
    And the graph should return 4 for count of "g.V(v6Id).bothE(\"codeveloper\").has(\"year\",2009)"

  Scenario: g_V_asXaX_inXcreatedX_addEXcreatedByX_fromXaX_propertyXyear_2009X_propertyXacl_publicX
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
    And using the parameter v2Id defined as "v[vadas].id"
    And using the parameter v3Id defined as "v[lop].id"
    And using the parameter v4Id defined as "v[josh].id"
    And using the parameter v5Id defined as "v[ripple].id"
    And using the parameter v6Id defined as "v[peter].id"
    And the traversal of
      """
      g.V().as("a").in("created").addE("createdBy").from("a").property("year", 2009).property("acl", "public")
      """
    When iterated to list
    Then the result should have a count of 4
    And the graph should return 10 for count of "g.E()"
    And the graph should return 4 for count of "g.V(v1Id).bothE()"
    And the graph should return 1 for count of "g.V(v1Id).inE(\"createdBy\")"
    And the graph should return 0 for count of "g.V(v1Id).outE(\"createdBy\")"
    And the graph should return 1 for count of "g.V(v1Id).bothE(\"createdBy\").has(\"year\",2009).has(\"acl\", \"public\")"
    And the graph should return 1 for count of "g.V(v2Id).bothE()"
    And the graph should return 6 for count of "g.V(v3Id).bothE()"
    And the graph should return 0 for count of "g.V(v3Id).inE(\"createdBy\")"
    And the graph should return 3 for count of "g.V(v3Id).outE(\"createdBy\")"
    And the graph should return 3 for count of "g.V(v3Id).bothE(\"createdBy\").has(\"year\",2009).has(\"acl\", \"public\")"
    And the graph should return 5 for count of "g.V(v4Id).bothE()"
    And the graph should return 2 for count of "g.V(v4Id).inE(\"createdBy\")"
    And the graph should return 0 for count of "g.V(v4Id).outE(\"createdBy\")"
    And the graph should return 2 for count of "g.V(v4Id).bothE(\"createdBy\").has(\"year\",2009).has(\"acl\", \"public\")"
    And the graph should return 2 for count of "g.V(v5Id).bothE()"
    And the graph should return 0 for count of "g.V(v5Id).inE(\"createdBy\")"
    And the graph should return 1 for count of "g.V(v5Id).outE(\"createdBy\")"
    And the graph should return 1 for count of "g.V(v5Id).bothE(\"createdBy\").has(\"year\",2009).has(\"acl\", \"public\")"
    And the graph should return 2 for count of "g.V(v6Id).bothE()"
    And the graph should return 1 for count of "g.V(v6Id).inE(\"createdBy\")"
    And the graph should return 0 for count of "g.V(v6Id).outE(\"createdBy\")"
    And the graph should return 1 for count of "g.V(v6Id).bothE(\"createdBy\").has(\"year\",2009).has(\"acl\", \"public\")"

  Scenario: g_V_asXaX_inXcreatedX_addInEXcreatedBy_a_year_2009_acl_publicX
    Given an unsupported test
    Then nothing should happen because
      """
      This test is deprecated.
      """

  Scenario: g_withSideEffectXb_bX_VXaX_addEXknowsX_toXbX_propertyXweight_0_5X
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
    And using the parameter v1 defined as "v[marko]"
    And using the parameter v6 defined as "v[peter]"
    And the traversal of
      """
      g.withSideEffect("b", v6).V(v1).addE("knows").to("b").property("weight", 0.5)
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 7 for count of "g.E()"
    And the graph should return 4 for count of "g.V(v1).bothE()"
    And the graph should return 0 for count of "g.V(v1).inE(\"knows\")"
    And the graph should return 3 for count of "g.V(v1).outE(\"knows\")"
    And the graph should return 2 for count of "g.V(v1).bothE(\"knows\").has(\"weight\",0.5)"
    And the graph should return 2 for count of "g.V(v6).bothE()"
    And the graph should return 1 for count of "g.V(v6).inE(\"knows\")"
    And the graph should return 0 for count of "g.V(v6).outE(\"knows\")"
    And the graph should return 1 for count of "g.V(v6).bothE(\"knows\").has(\"weight\",0.5)"

  Scenario: g_addV_asXfirstX_repeatXaddEXnextX_toXaddVX_inVX_timesX5X_addEXnextX_toXselectXfirstXX
    Given the empty graph
    And the traversal of
      """
      g.addV().as("first").repeat(__.addE("next").to(__.addV()).inV()).times(5).addE("next").to(__.select("first"))
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 6 for count of "g.V()"
    And the graph should return 6 for count of "g.E()"
    And the graph should return 6 for count of "g.E().hasLabel(\"next\")"
    And the graph should return 2 for count of "g.V().limit(1).bothE()"
    And the graph should return 1 for count of "g.V().limit(1).inE()"
    And the graph should return 1 for count of "g.V().limit(1).outE()"

  Scenario: g_V_hasXname_markoX_asXaX_outEXcreatedX_asXbX_inV_addEXselectXbX_labelX_toXaX
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
    And using the parameter v1 defined as "v[marko]"
    And the traversal of
      """
      g.V().has("name", "marko").as("a").outE("created").as("b").inV().addE(__.select("b").label()).to("a")
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 7 for count of "g.E()"
    And the graph should return 4 for count of "g.V(v1).bothE()"
    And the graph should return 1 for count of "g.V(v1).inE(\"created\")"
    And the graph should return 1 for count of "g.V(v1).in(\"created\").has(\"name\",\"lop\")"
    And the graph should return 1 for count of "g.V(v1).outE(\"created\")"

  Scenario: g_addEXV_outE_label_groupCount_orderXlocalX_byXvalues_descX_selectXkeysX_unfold_limitX1XX_fromXV_hasXname_vadasXX_toXV_hasXname_lopXX
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
    And using the parameter v2 defined as "v[vadas]"
    And the traversal of
      """
      g.addE(__.V().outE().label().groupCount().order(Scope.local).by(Column.values, Order.desc).select(Column.keys).unfold().limit(1)).from(__.V().has("name", "vadas")).to(__.V().has("name", "lop"))
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 7 for count of "g.E()"
    And the graph should return 2 for count of "g.V(v2).bothE()"
    And the graph should return 1 for count of "g.V(v2).inE(\"knows\")"
    And the graph should return 1 for count of "g.V(v2).outE(\"created\")"
    And the graph should return 1 for count of "g.V(v2).out(\"created\").has(\"name\",\"lop\")"

  Scenario: g_addEXknowsX_fromXaX_toXbX_propertyXweight_0_1X
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
    And using the parameter v1 defined as "v[marko]"
    And using the parameter v6 defined as "v[peter]"
    And using the parameter dotOne defined as "d[0.1].d"
    And the traversal of
      """
      g.addE("knows").from(v1).to(v6).property("weight", dotOne)
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 7 for count of "g.E()"
    And the graph should return 3 for count of "g.V(v1).outE(\"knows\")"
    And the graph should return 1 for count of "g.V(v1).out(\"knows\").has(\"name\",\"peter\")"

  Scenario: g_VXaX_addEXknowsX_toXbX_propertyXweight_0_1X
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
    And using the parameter v1 defined as "v[marko]"
    And using the parameter v6 defined as "v[peter]"
    And using the parameter dotOne defined as "d[0.1].d"
    And the traversal of
      """
      g.V(v1).addE("knows").to(v6).property("weight", dotOne)
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 7 for count of "g.E()"
    And the graph should return 3 for count of "g.V(v1).outE(\"knows\")"
    And the graph should return 1 for count of "g.V(v1).out(\"knows\").has(\"name\",\"peter\")"