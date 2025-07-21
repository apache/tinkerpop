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

@StepClassMap @StepToLower
Feature: Step - toLower()

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXfeature_test_nullX_toLower
    Given the empty graph
    And the traversal of
      """
      g.inject("FEATURE", "tESt", null).toLower()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | feature |
      | test |
      | null |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXfeature_test_nullX_toLowerXlocalX
    Given the empty graph
    And the traversal of
      """
      g.inject(["FEATURE","tESt",null]).toLower(Scope.local)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[feature,test,null] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXListXa_bXX_toLower
    Given the empty graph
    And the traversal of
      """
      g.inject(["a","b"]).toLower()
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "The toLower() step can only take string as argument"

  Scenario: g_V_valuesXnameX_toLower
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", "MARKO").property("age", 29).as("marko").
        addV("person").property("name", "VADAS").property("age", 27).as("vadas").
        addV("software").property("name", "LOP").property("lang", "java").as("lop").
        addV("person").property("name","JOSH").property("age", 32).as("josh").
        addV("software").property("name", "RIPPLE").property("lang", "java").as("ripple").
        addV("person").property("name", "PETER").property("age", 35).as('peter').
        addE("knows").from("marko").to("vadas").property("weight", 0.5d).
        addE("knows").from("marko").to("josh").property("weight", 1.0d).
        addE("created").from("marko").to("lop").property("weight", 0.4d).
        addE("created").from("josh").to("ripple").property("weight", 1.0d).
        addE("created").from("josh").to("lop").property("weight", 0.4d).
        addE("created").from("peter").to("lop").property("weight", 0.2d)
      """
    And the traversal of
      """
      g.V().values("name").toLower()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko |
      | vadas |
      | lop |
      | josh |
      | ripple |
      | peter |

  Scenario: g_V_valuesXnameX_toLowerXlocalX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", "MARKO").property("age", 29).as("marko").
        addV("person").property("name", "VADAS").property("age", 27).as("vadas").
        addV("software").property("name", "LOP").property("lang", "java").as("lop").
        addV("person").property("name","JOSH").property("age", 32).as("josh").
        addV("software").property("name", "RIPPLE").property("lang", "java").as("ripple").
        addV("person").property("name", "PETER").property("age", 35).as('peter').
        addE("knows").from("marko").to("vadas").property("weight", 0.5d).
        addE("knows").from("marko").to("josh").property("weight", 1.0d).
        addE("created").from("marko").to("lop").property("weight", 0.4d).
        addE("created").from("josh").to("ripple").property("weight", 1.0d).
        addE("created").from("josh").to("lop").property("weight", 0.4d).
        addE("created").from("peter").to("lop").property("weight", 0.2d)
      """
    And the traversal of
      """
      g.V().values("name").toLower(Scope.local)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko |
      | vadas |
      | lop |
      | josh |
      | ripple |
      | peter |

  Scenario: g_V_valuesXnameX_order_fold_toLowerXlocalX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", "MARKO").property("age", 29).as("marko").
        addV("person").property("name", "VADAS").property("age", 27).as("vadas").
        addV("software").property("name", "LOP").property("lang", "java").as("lop").
        addV("person").property("name","JOSH").property("age", 32).as("josh").
        addV("software").property("name", "RIPPLE").property("lang", "java").as("ripple").
        addV("person").property("name", "PETER").property("age", 35).as('peter').
        addE("knows").from("marko").to("vadas").property("weight", 0.5d).
        addE("knows").from("marko").to("josh").property("weight", 1.0d).
        addE("created").from("marko").to("lop").property("weight", 0.4d).
        addE("created").from("josh").to("ripple").property("weight", 1.0d).
        addE("created").from("josh").to("lop").property("weight", 0.4d).
        addE("created").from("peter").to("lop").property("weight", 0.2d)
      """
    And the traversal of
      """
      g.V().values("name").order().fold().toLower(Scope.local)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[josh,lop,marko,peter,ripple,vadas] |
