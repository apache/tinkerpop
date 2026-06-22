# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

@StepClassSideEffect @StepAddProperty
Feature: Step - property()

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_VXvid2X_propertyXalias_VXvid1X_valuesXnameXX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name","marko").property("age",29).
        addV("person").property("name","vadas").property("age",27).
        addV("software").property("name","lop").property("lang","java").
        addV("person").property("name","josh").property("age",32).
        addV("person").property("name","peter").property("age",35).
        addV("software").property("name","ripple").property("lang","java")
      """
    And using the parameter vid1 defined as "v[marko].id"
    And using the parameter vid2 defined as "v[josh].id"
    And the traversal of
      """
      g.V(vid2).property("alias", __.V(vid1).values("name"))
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 1 for count of "g.V().has(\"alias\", \"marko\")"

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_VXvid1X_propertyXcreatorCount_VXvid1X_inXcreatedX_countX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name","marko").property("age",29).as("marko").
        addV("person").property("name","vadas").property("age",27).
        addV("software").property("name","lop").property("lang","java").as("lop").
        addV("person").property("name","josh").property("age",32).as("josh").
        addV("person").property("name","peter").property("age",35).as("peter").
        addV("software").property("name","ripple").property("lang","java").as("ripple").
        addE("knows").from("marko").to(__.V().has("name","vadas")).
        addE("knows").from("marko").to("josh").
        addE("created").from("marko").to("lop").
        addE("created").from("josh").to("lop").
        addE("created").from("josh").to("ripple").
        addE("created").from("peter").to("lop")
      """
    And using the parameter vid1 defined as "v[lop].id"
    And the traversal of
      """
      g.V(vid1).property("creatorCount", __.V(vid1).in("created").count())
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 1 for count of "g.V().has(\"creatorCount\", 3L)"

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_VXvid1X_propertyXknownCount_outXknowsX_countX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name","marko").property("age",29).as("marko").
        addV("person").property("name","vadas").property("age",27).as("vadas").
        addV("software").property("name","lop").property("lang","java").
        addV("person").property("name","josh").property("age",32).as("josh").
        addV("person").property("name","peter").property("age",35).
        addV("software").property("name","ripple").property("lang","java").
        addE("knows").from("marko").to("vadas").
        addE("knows").from("marko").to("josh")
      """
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).property("knownCount", __.out("knows").count())
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 1 for count of "g.V().has(\"knownCount\", 2L)"

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_VXvid1X_propertyXcreator_inXcreatedX_valuesXnameXX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name","marko").property("age",29).as("marko").
        addV("person").property("name","vadas").property("age",27).
        addV("software").property("name","lop").property("lang","java").as("lop").
        addV("person").property("name","josh").property("age",32).as("josh").
        addV("person").property("name","peter").property("age",35).as("peter").
        addV("software").property("name","ripple").property("lang","java").as("ripple").
        addE("knows").from("marko").to(__.V().has("name","vadas")).
        addE("knows").from("marko").to("josh").
        addE("created").from("marko").to("lop").
        addE("created").from("josh").to("lop").
        addE("created").from("josh").to("ripple").
        addE("created").from("peter").to("lop")
      """
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).property("creator", __.in("created").values("name"))
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 0 for count of "g.V().has(\"creator\")"

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_VXvid2X_propertyXVXvid1X_projectXfriendCount_softwareCreatedXX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name","marko").property("age",29).as("marko").
        addV("person").property("name","vadas").property("age",27).as("vadas").
        addV("software").property("name","lop").property("lang","java").as("lop").
        addV("person").property("name","josh").property("age",32).as("josh").
        addV("person").property("name","peter").property("age",35).as("peter").
        addV("software").property("name","ripple").property("lang","java").as("ripple").
        addE("knows").from("marko").to("vadas").
        addE("knows").from("marko").to("josh").
        addE("created").from("marko").to("lop").
        addE("created").from("josh").to("lop").
        addE("created").from("josh").to("ripple").
        addE("created").from("peter").to("lop")
      """
    And using the parameter vid1 defined as "v[marko].id"
    And using the parameter vid2 defined as "v[josh].id"
    And the traversal of
      """
      g.V(vid2).property(__.V(vid1).project("friendCount","softwareCreated").by(__.out("knows").count()).by(__.out("created").values("name")))
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 1 for count of "g.V().has(\"friendCount\", 2L)"
    And the graph should return 1 for count of "g.V().has(\"softwareCreated\", \"lop\")"

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_VXvid2X_propertyXVXvid1X_projectXoriginalName_originalLabelX_byXnameX_byXlabelXX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name","marko").property("age",29).as("marko").
        addV("person").property("name","vadas").property("age",27).
        addV("software").property("name","lop").property("lang","java").as("lop").
        addV("person").property("name","josh").property("age",32).
        addV("person").property("name","peter").property("age",35).
        addV("software").property("name","ripple").property("lang","java")
      """
    And using the parameter vid1 defined as "v[marko].id"
    And using the parameter vid2 defined as "v[lop].id"
    And the traversal of
      """
      g.V(vid2).property(__.V(vid1).project("originalName","originalLabel").by(__.values("name")).by(__.label()))
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 1 for count of "g.V(vid2).has(\"originalName\", \"marko\")"
    And the graph should return 1 for count of "g.V(vid2).has(\"originalLabel\", \"person\")"

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_VXvid1X_propertyXlist_friends_outXknowsX_valuesXnameXX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name","marko").property("age",29).as("marko").
        addV("person").property("name","vadas").property("age",27).as("vadas").
        addV("software").property("name","lop").property("lang","java").
        addV("person").property("name","josh").property("age",32).as("josh").
        addV("person").property("name","peter").property("age",35).
        addV("software").property("name","ripple").property("lang","java").
        addE("knows").from("marko").to("vadas").
        addE("knows").from("marko").to("josh")
      """
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).property(Cardinality.list, "friends", __.out("knows").values("name"))
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 2 for count of "g.V(vid1).properties(\"friends\")"

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_VXvid1X_propertyXset_langs_outXcreatedX_valuesXlangXX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name","marko").property("age",29).
        addV("person").property("name","vadas").property("age",27).
        addV("software").property("name","lop").property("lang","java").as("lop").
        addV("person").property("name","josh").property("age",32).as("josh").
        addV("person").property("name","peter").property("age",35).
        addV("software").property("name","ripple").property("lang","java").as("ripple").
        addE("created").from("josh").to("lop").
        addE("created").from("josh").to("ripple")
      """
    And using the parameter vid1 defined as "v[josh].id"
    And the traversal of
      """
      g.V(vid1).property(Cardinality.set, "langs", __.out("created").values("lang"))
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 1 for count of "g.V(vid1).properties(\"langs\")"

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_VXvid1X_propertyXsingle_friend_outXknowsX_valuesXnameXX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name","marko").property("age",29).as("marko").
        addV("person").property("name","vadas").property("age",27).as("vadas").
        addV("software").property("name","lop").property("lang","java").
        addV("person").property("name","josh").property("age",32).as("josh").
        addV("person").property("name","peter").property("age",35).
        addV("software").property("name","ripple").property("lang","java").
        addE("knows").from("marko").to("vadas").
        addE("knows").from("marko").to("josh")
      """
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).property(Cardinality.single, "friend", __.out("knows").values("name"))
      """
    When iterated to list
    Then the traversal will raise an error

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_VXvid1X_propertyXVXvid1X_groupX_byXidX_byXvaluesXnameXXX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name","marko").property("age",29)
      """
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).property(__.V(vid1).group().by(__.id()).by(__.values("name")))
      """
    When iterated to list
    Then the traversal will raise an error
