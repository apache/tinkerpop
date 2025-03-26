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

@StepClassMap @StepConjoin
Feature: Step - conjoin()

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXnullX_conjoinX1X
    Given the empty graph
    And the traversal of
      """
      g.inject(null).conjoin("1")
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "Incoming traverser for conjoin step can't be null"

  Scenario: g_V_valuesXnameX_conjoinX1X
    Given the modern graph
    And the traversal of
      """
      g.V().values("name").conjoin("1")
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "conjoin step can only take an array or an Iterable type for incoming traversers, encountered"

  Scenario: g_V_valuesXnonexistantX_fold_conjoinX_X
    Given the modern graph
    And the traversal of
      """
      g.V().values("nonexistant").fold().conjoin(";")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | str[] |

  Scenario: g_V_valuesXnameX_order_fold_conjoinX_X
    Given the modern graph
    And the traversal of
      """
      g.V().values("name").order().fold().conjoin("_")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | josh_lop_marko_peter_ripple_vadas |

  Scenario: g_V_valuesXageX_order_fold_conjoinXsemicolonX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").order().fold().conjoin(";")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | 27;29;32;35 |

  Scenario: g_V_valuesXageX_order_fold_conjoinXslashX
    Given the modern graph
    And using the parameter xx1 defined as "/"
    And the traversal of
      """
      g.V().values("age").order().fold().conjoin(xx1)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | 27/29/32/35 |

  @GraphComputerVerificationReferenceOnly
  Scenario: g_V_out_path_byXvaluesXnameX_toUpperX_conjoinXMARKOX
    Given the modern graph
    And the traversal of
      """
      g.V().out().path().by(values("name").toUpper()).conjoin("MARKO")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | MARKOMARKOLOP |
      | MARKOMARKOVADAS |
      | MARKOMARKOJOSH |
      | JOSHMARKORIPPLE |
      | JOSHMARKOLOP |
      | PETERMARKOLOP |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXmarkoX_conjoinX_X
    Given the modern graph
    And using the parameter xx1 defined as "l[marko]"
    And the traversal of
      """
      g.inject(xx1).conjoin("-")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko |

  @MultiProperties @MetaProperties
  Scenario: g_V_valueMapXlocationX_selectXvaluesX_unfold_orderXlocalX_conjoinX1X
    Given the crew graph
    And the traversal of
      """
      g.V().valueMap("location").select(values).unfold().order(Scope.local).conjoin("1")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | brussels1san diego1santa cruz1santa fe |
      | centreville1dulles1purcellville |
      | baltimore1bremen1oakland1seattle |
      | aachen1kaiserslautern1spremberg |

  @GraphComputerVerificationReferenceOnly
  Scenario: g_V_out_out_path_byXnameX_conjoinXX
    Given the modern graph
    And the traversal of
      """
      g.V().out().out().path().by("name").conjoin("")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | markojoshripple |
      | markojoshlop |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXa_null_bX_conjoinXxyzX
    Given the empty graph
    And using the parameter xx1 defined as "l[a,null,b]"
    And the traversal of
      """
      g.inject(xx1).conjoin("xyz")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | str[axyzb] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX3_threeX_conjoinX_X
    Given the empty graph
    And using the parameter xx1 defined as "l[d[3].i,three]"
    And the traversal of
      """
      g.inject(xx1).conjoin(";")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | 3;three |
