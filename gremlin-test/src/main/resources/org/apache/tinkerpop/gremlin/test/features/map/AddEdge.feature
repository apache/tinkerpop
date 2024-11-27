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

@StepClassMap @StepAddE
Feature: Step - addE()

  Scenario: g_VX1X_asXaX_outXcreatedX_addEXcreatedByX_toXaX
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
      g.V(vid1).as("a").out("created").addE("createdBy").to("a")
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 7 for count of "g.E()"
    And the graph should return 1 for count of "g.V(vid1).inE()"

  Scenario: g_VX1X_asXaX_outXcreatedX_addEXcreatedByX_toXaX_propertyXweight_2X
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
      g.V(vid1).as("a").out("created").addE("createdBy").to("a").property("weight", 2.0d)
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 7 for count of "g.E()"
    And the graph should return 4 for count of "g.V(vid1).bothE()"
    And the graph should return 1 for count of "g.V(vid1).inE().has(\"weight\", 2.0d)"

  Scenario: g_V_outE_propertyXweight_nullX
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
      g.V().outE().property("weight", null)
      """
    When iterated to list
    Then the result should have a count of 6
    And the graph should return 0 for count of "g.E().properties(\"weight\")"

  Scenario: g_V_aggregateXxX_asXaX_selectXxX_unfold_addEXexistsWithX_toXaX_propertyXtime_nowX
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
    And using the parameter vid2 defined as "v[vadas].id"
    And using the parameter vid3 defined as "v[lop].id"
    And using the parameter vid4 defined as "v[josh].id"
    And using the parameter vid5 defined as "v[ripple].id"
    And using the parameter vid6 defined as "v[peter].id"
    And the traversal of
      """
      g.V().aggregate("x").as("a").select("x").unfold().addE("existsWith").to("a").property("time", "now")
      """
    When iterated to list
    Then the result should have a count of 36
    And the graph should return 42 for count of "g.E()"
    And the graph should return 15 for count of "g.V(vid1).bothE()"
    And the graph should return 6 for count of "g.V(vid1).inE(\"existsWith\")"
    And the graph should return 6 for count of "g.V(vid1).outE(\"existsWith\")"
    And the graph should return 12 for count of "g.V(vid1).bothE(\"existsWith\").has(\"time\",\"now\")"
    And the graph should return 13 for count of "g.V(vid2).bothE()"
    And the graph should return 6 for count of "g.V(vid2).inE(\"existsWith\")"
    And the graph should return 6 for count of "g.V(vid2).outE(\"existsWith\")"
    And the graph should return 12 for count of "g.V(vid2).bothE(\"existsWith\").has(\"time\",\"now\")"
    And the graph should return 15 for count of "g.V(vid3).bothE()"
    And the graph should return 6 for count of "g.V(vid3).inE(\"existsWith\")"
    And the graph should return 6 for count of "g.V(vid3).outE(\"existsWith\")"
    And the graph should return 12 for count of "g.V(vid3).bothE(\"existsWith\").has(\"time\",\"now\")"
    And the graph should return 15 for count of "g.V(vid4).bothE()"
    And the graph should return 6 for count of "g.V(vid4).inE(\"existsWith\")"
    And the graph should return 6 for count of "g.V(vid4).outE(\"existsWith\")"
    And the graph should return 12 for count of "g.V(vid4).bothE(\"existsWith\").has(\"time\",\"now\")"
    And the graph should return 13 for count of "g.V(vid5).bothE()"
    And the graph should return 6 for count of "g.V(vid5).inE(\"existsWith\")"
    And the graph should return 6 for count of "g.V(vid5).outE(\"existsWith\")"
    And the graph should return 12 for count of "g.V(vid5).bothE(\"existsWith\").has(\"time\",\"now\")"
    And the graph should return 13 for count of "g.V(vid6).bothE()"
    And the graph should return 6 for count of "g.V(vid6).inE(\"existsWith\")"
    And the graph should return 6 for count of "g.V(vid6).outE(\"existsWith\")"
    And the graph should return 12 for count of "g.V(vid6).bothE(\"existsWith\").has(\"time\",\"now\")"

  Scenario: g_V_asXaX_outXcreatedX_inXcreatedX_whereXneqXaXX_asXbX_addEXcodeveloperX_fromXaX_toXbX_propertyXyear_2009X
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
    And using the parameter vid2 defined as "v[vadas].id"
    And using the parameter vid4 defined as "v[josh].id"
    And using the parameter vid6 defined as "v[peter].id"
    And the traversal of
      """
      g.V().as("a").out("created").in("created").where(P.neq("a")).as("b").addE("codeveloper").from("a").to("b").property("year", 2009)
      """
    When iterated to list
    Then the result should have a count of 6
    And the graph should return 12 for count of "g.E()"
    And the graph should return 7 for count of "g.V(vid1).bothE()"
    And the graph should return 2 for count of "g.V(vid1).inE(\"codeveloper\")"
    And the graph should return 2 for count of "g.V(vid1).outE(\"codeveloper\")"
    And the graph should return 4 for count of "g.V(vid1).bothE(\"codeveloper\").has(\"year\",2009)"
    And the graph should return 1 for count of "g.V(vid2).bothE()"
    And the graph should return 7 for count of "g.V(vid4).bothE()"
    And the graph should return 2 for count of "g.V(vid4).inE(\"codeveloper\")"
    And the graph should return 2 for count of "g.V(vid4).outE(\"codeveloper\")"
    And the graph should return 4 for count of "g.V(vid4).bothE(\"codeveloper\").has(\"year\",2009)"
    And the graph should return 5 for count of "g.V(vid6).bothE()"
    And the graph should return 2 for count of "g.V(vid6).inE(\"codeveloper\")"
    And the graph should return 2 for count of "g.V(vid6).outE(\"codeveloper\")"
    And the graph should return 4 for count of "g.V(vid6).bothE(\"codeveloper\").has(\"year\",2009)"

  Scenario: g_V_asXaX_inXcreatedX_addEXcreatedByX_fromXaX_propertyXyear_2009X_propertyXacl_publicX
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
    And using the parameter vid2 defined as "v[vadas].id"
    And using the parameter vid3 defined as "v[lop].id"
    And using the parameter vid4 defined as "v[josh].id"
    And using the parameter vid5 defined as "v[ripple].id"
    And using the parameter vid6 defined as "v[peter].id"
    And the traversal of
      """
      g.V().as("a").in("created").addE("createdBy").from("a").property("year", 2009).property("acl", "public")
      """
    When iterated to list
    Then the result should have a count of 4
    And the graph should return 10 for count of "g.E()"
    And the graph should return 4 for count of "g.V(vid1).bothE()"
    And the graph should return 1 for count of "g.V(vid1).inE(\"createdBy\")"
    And the graph should return 0 for count of "g.V(vid1).outE(\"createdBy\")"
    And the graph should return 1 for count of "g.V(vid1).bothE(\"createdBy\").has(\"year\",2009).has(\"acl\", \"public\")"
    And the graph should return 1 for count of "g.V(vid2).bothE()"
    And the graph should return 6 for count of "g.V(vid3).bothE()"
    And the graph should return 0 for count of "g.V(vid3).inE(\"createdBy\")"
    And the graph should return 3 for count of "g.V(vid3).outE(\"createdBy\")"
    And the graph should return 3 for count of "g.V(vid3).bothE(\"createdBy\").has(\"year\",2009).has(\"acl\", \"public\")"
    And the graph should return 5 for count of "g.V(vid4).bothE()"
    And the graph should return 2 for count of "g.V(vid4).inE(\"createdBy\")"
    And the graph should return 0 for count of "g.V(vid4).outE(\"createdBy\")"
    And the graph should return 2 for count of "g.V(vid4).bothE(\"createdBy\").has(\"year\",2009).has(\"acl\", \"public\")"
    And the graph should return 2 for count of "g.V(vid5).bothE()"
    And the graph should return 0 for count of "g.V(vid5).inE(\"createdBy\")"
    And the graph should return 1 for count of "g.V(vid5).outE(\"createdBy\")"
    And the graph should return 1 for count of "g.V(vid5).bothE(\"createdBy\").has(\"year\",2009).has(\"acl\", \"public\")"
    And the graph should return 2 for count of "g.V(vid6).bothE()"
    And the graph should return 1 for count of "g.V(vid6).inE(\"createdBy\")"
    And the graph should return 0 for count of "g.V(vid6).outE(\"createdBy\")"
    And the graph should return 1 for count of "g.V(vid6).bothE(\"createdBy\").has(\"year\",2009).has(\"acl\", \"public\")"

  Scenario: g_withSideEffectXb_bX_VXaX_addEXknowsX_toXbX_propertyXweight_0_5X
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
    And using the parameter v1 defined as "v[marko]"
    And using the parameter v6 defined as "v[peter]"
    And the traversal of
      """
      g.withSideEffect("b", v6).V(v1).addE("knows").to("b").property("weight", 0.5D)
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
    And using the parameter v1 defined as "v[marko]"
    And using the parameter v6 defined as "v[peter]"
    And using the parameter xx1 defined as "d[0.1].d"
    And the traversal of
      """
      g.addE("knows").from(v1).to(v6).property("weight", xx1)
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 7 for count of "g.E()"
    And the graph should return 3 for count of "g.V(v1).outE(\"knows\")"
    And the graph should return 1 for count of "g.V(v1).out(\"knows\").has(\"name\",\"peter\")"

  Scenario: g_addEXknowsvarX_fromXaX_toXbX_propertyXweight_0_1X
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
    And using the parameter v1 defined as "v[marko]"
    And using the parameter v6 defined as "v[peter]"
    And using the parameter xx1 defined as "knows"
    And using the parameter xx2 defined as "d[0.1].d"
    And the traversal of
      """
      g.addE(xx1).from(v1).to(v6).property("weight", xx2)
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
    And using the parameter v1 defined as "v[marko]"
    And using the parameter v6 defined as "v[peter]"
    And using the parameter xx1 defined as "d[0.1].d"
    And the traversal of
      """
      g.V(v1).addE("knows").to(v6).property("weight", xx1)
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 7 for count of "g.E()"
    And the graph should return 3 for count of "g.V(v1).outE(\"knows\")"
    And the graph should return 1 for count of "g.V(v1).out(\"knows\").has(\"name\",\"peter\")"

  @AllowNullPropertyValues
  Scenario: g_addEXknowsXpropertyXweight_nullXfromXV_hasXname_markoXX_toXV_hasXname_vadasXX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", "marko").property("age", 29).
        addV("person").property("name", "vadas").property("age", 27)
      """
    And the traversal of
      """
      g.addE("knows").property("weight", null).from(V().has("name","marko")).to(V().has("name","vadas"))
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 1 for count of "g.E().has(\"knows\",\"weight\",null)"

  @AllowNullPropertyValues
  Scenario: g_addEXknowsvarXpropertyXweight_nullXfromXV_hasXname_markoXX_toXV_hasXname_vadasXX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", "marko").property("age", 29).
        addV("person").property("name", "vadas").property("age", 27)
      """
    And using the parameter xx1 defined as "knows"
    And the traversal of
      """
      g.addE(xx1).property("weight", null).from(V().has("name","marko")).to(V().has("name","vadas"))
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 1 for count of "g.E().has(\"knows\",\"weight\",null)"

  Scenario: g_unionXaddEXknowsvarXpropertyXweight_nullXfromXV_hasXname_markoXX_toXV_hasXname_vadasXXX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", "marko").property("age", 29).
        addV("person").property("name", "vadas").property("age", 27)
      """
    And using the parameter xx1 defined as "knows"
    And the traversal of
      """
      g.union(addE(xx1).property("weight", 1).from(V().has("name","marko")).to(V().has("name","vadas")))
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 1 for count of "g.E().has(\"knows\",\"weight\", 1)"