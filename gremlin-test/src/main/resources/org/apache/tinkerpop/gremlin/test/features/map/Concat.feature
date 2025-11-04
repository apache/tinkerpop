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

@StepClassMap @StepConcat
Feature: Step - concat()

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXa_bX_concat
    Given the empty graph
    And the traversal of
      """
      g.inject("a", "b").concat()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | a |
      | b |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXa_bX_concat_XcX
    Given the empty graph
    And the traversal of
      """
      g.inject("a", "b").concat("c")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | ac |
      | bc |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXa_bX_concat_Xc_dX
    Given the empty graph
    And the traversal of
      """
      g.inject("a", "b").concat("c", "d")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | acd |
      | bcd |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXa_bX_concat_Xinject_c_dX
    Given the empty graph
    And the traversal of
      """
      g.inject("a", "b").concat(__.inject("c"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | aa |
      | bb |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXaX_concat_Xinject_List_b_cX
    Given the empty graph
    And the traversal of
      """
      g.inject("a").concat(__.inject(["b","c"]))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | aa |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXListXa_bXcX_concat_XdX
    Given the empty graph
    And the traversal of
      """
      g.inject(["a","b"],"c").concat("d")
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "String concat() can only take string as argument"

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXnullX_concat_XinjectX
    Given the empty graph
    And the traversal of
      """
      g.inject(null).concat()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | null |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXnull_aX_concat_Xnull_bX
    Given the empty graph
    And the traversal of
      """
      g.inject(null, "a").concat(null, "b")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | b |
      | ab |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXhello_hiX_concatXV_values_order_byXnameX_valuesXnameXX
    Given the modern graph
    And the traversal of
      """
      g.inject("hello", "hi").concat(__.V().order().by("name").values("name"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | hellojosh |
      | hijosh |

  Scenario: g_V_hasLabel_value_concat_X_X_concat_XpersonX
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("person").values("name").concat(" ").concat("person")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko person |
      | vadas person |
      | josh person |
      | peter person |

  Scenario: g_hasLabelXpersonX_valuesXnameX_asXaX_constantXMrX_concatXselectXaX
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("person").values("name").as("a").constant("Mr.").concat(__.select("a"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | Mr.marko |
      | Mr.vadas |
      | Mr.josh |
      | Mr.peter |

  @GraphComputerVerificationStarGraphExceeded
  Scenario: g_hasLabelXsoftwareX_asXaX_valuesXnameX_concatXunsesX_concatXselectXaXvaluesXlangX
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("software").as("a").values("name").concat(" uses ").concat(__.select("a").values("lang"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | lop uses java |
      | ripple uses java |

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_VX1X_outE_asXaX_VX1X_valuesXnamesX_concatXselectXaX_labelX_concatXselectXaX_inV_valuesXnameXX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).outE().as("a").V(vid1).values("name").concat(select("a").label()).concat(select("a").inV().values("name"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | markocreatedlop |
      | markoknowsvadas |
      | markoknowsjosh |

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_VX1X_outE_asXaX_VX1X_valuesXnamesX_concatXselectXaX_label_selectXaX_inV_valuesXnameXX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).outE().as("a").V(vid1).values("name").concat(select("a").label(), select("a").inV().values("name"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | markocreatedlop |
      | markoknowsvadas |
      | markoknowsjosh |

  Scenario: g_addVXconstantXprefix_X_concatXVX1X_labelX_label
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
      g.addV(constant("prefix_").concat(__.V(vid1).label())).label()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | prefix_person |
