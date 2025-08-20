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

@StepClassMap @StepRTrim
Feature: Step - rTrim()

  @GraphComputerVerificationInjectionNotSupported
  # This verifies both ASCII control space and ideographic space character \u3000 are property trimmed.
  Scenario: g_injectX__feature___test__nullX_rTrim
    Given the empty graph
    And the traversal of
      """
      g.inject("feature  ", "one test ", null, "", " ", "　abc", "abc　", "　abc　", "　　").rTrim()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | feature |
      | one test |
      | null |
      | str[] |
      | str[] |
      | str[　abc] |
      | str[abc] |
      | str[　abc] |
      | str[] |

  @GraphComputerVerificationInjectionNotSupported
  # This verifies both ASCII control space and ideographic space character \u3000 are property trimmed.
  Scenario: g_injectX__feature___test__nullX_rTrimXlocalX
    Given the empty graph
    And the traversal of
      """
      g.inject(["  feature  ", " one test ", null, "", " ", "　abc", "abc　", "　abc　", "　　"]).rTrim(Scope.local)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[str[  feature],str[ one test],null,str[],str[],str[　abc],str[abc],str[　abc],str[]] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX__feature__X_rTrim
    Given the empty graph
    And the traversal of
      """
      g.inject("  feature  ").rTrim()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | str[  feature] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXListXa_bXX_rTrim
    Given the empty graph
    And the traversal of
    """
    g.inject(["a","b"]).rTrim()
    """
    When iterated to list
    Then the traversal will raise an error with message containing text of "The rTrim() step can only take string as argument"

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXListX1_2XX_rTrimXlocalX
    Given the empty graph
    And the traversal of
    """
    g.inject([1,2]).rTrim(Scope.local)
    """
    When iterated to list
    Then the traversal will raise an error with message containing text of "The rTrim(local) step can only take string or list of strings"

  Scenario: g_V_valuesXnameX_rTrim
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", " marko ").property("age", 29).as("marko").
        addV("person").property("name", "  vadas  ").property("age", 27).as("vadas").
        addV("software").property("name", "  lop").property("lang", "java").as("lop").
        addV("person").property("name","josh  ").property("age", 32).as("josh").
        addV("software").property("name", "   ripple   ").property("lang", "java").as("ripple").
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
      g.V().values("name").rTrim()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | str[ marko] |
      | str[  vadas] |
      | str[  lop] |
      | str[josh] |
      | str[   ripple] |
      | str[peter] |

  Scenario: g_V_valuesXnameX_order_fold_rTrimXlocalX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", " marko ").property("age", 29).as("marko").
        addV("person").property("name", "  vadas  ").property("age", 27).as("vadas").
        addV("software").property("name", "  lop").property("lang", "java").as("lop").
        addV("person").property("name","josh  ").property("age", 32).as("josh").
        addV("software").property("name", "   ripple   ").property("lang", "java").as("ripple").
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
      g.V().values("name").order().fold().rTrim(Scope.local)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[str[   ripple],str[  lop],str[  vadas],str[ marko],str[josh],str[peter]] |