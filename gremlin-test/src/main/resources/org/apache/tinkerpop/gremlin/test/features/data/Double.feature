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

@StepClassData @DataDouble
Feature: Data - DOUBLE

  Scenario: g_V_valuesXdoubleX_isXtypeOfXGType_DOUBLEXX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("data").property("double", 1.5d)
      """
    And the traversal of
      """
      g.V().values("double").is(P.typeOf(GType.DOUBLE))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[1.5].d |

  Scenario: g_E_valuesXweightX_isXtypeOfXGType_DOUBLEXX
    Given the modern graph
    And the traversal of
      """
      g.E().values("weight").is(P.typeOf(GType.DOUBLE))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[0.5].d |
      | d[1.0].d |
      | d[0.4].d |
      | d[1.0].d |
      | d[0.2].d |
      | d[0.4].d |

  Scenario: g_V_valuesXdoubleX_isXtypeOfXGType_DOUBLEXX_mathXceilX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("data").property("double", 2.7d)
      """
    And the traversal of
      """
      g.V().values("double").is(P.typeOf(GType.DOUBLE)).math('ceil _')
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[3].d |

  Scenario: g_V_valuesXdoubleX_isXtypeOfXGType_DOUBLEXX_isXgtX1_0XX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("data").property("double", 0.8d).
        addV("data").property("double", 1.2d)
      """
    And the traversal of
      """
      g.V().values("double").is(P.typeOf(GType.DOUBLE)).is(P.gt(1.0d))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[1.2].d |

  Scenario: g_V_valuesXdoubleX_isXtypeOfXGType_DOUBLEXX_sumX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("data").property("double", 1.5d).
        addV("data").property("double", 2.5d).
        addV("data").property("double", 3.5d)
      """
    And the traversal of
      """
      g.V().values("double").is(P.typeOf(GType.DOUBLE)).sum()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[7.5].d |

  Scenario: g_V_valuesXdoubleX_isXtypeOfXGType_DOUBLEXX_minX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("data").property("double", 0.1d).
        addV("data").property("double", 0.5d).
        addV("data").property("double", 0.9d)
      """
    And the traversal of
      """
      g.V().values("double").is(P.typeOf(GType.DOUBLE)).min()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[0.1].d |

  Scenario: g_V_valuesXdoubleX_isXtypeOfXGType_DOUBLEXX_maxX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("data").property("double", 2.1d).
        addV("data").property("double", 3.7d).
        addV("data").property("double", 1.9d)
      """
    And the traversal of
      """
      g.V().values("double").is(P.typeOf(GType.DOUBLE)).max()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[3.7].d |

  Scenario: g_V_valuesXdoubleX_isXtypeOfXGType_DOUBLEXX_meanX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("data").property("double", 2.1d).
        addV("data").property("double", 4.1d).
        addV("data").property("double", 6.1d)
      """
    And the traversal of
      """
      g.V().values("double").is(P.typeOf(GType.DOUBLE)).mean()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[4.1].d |

  Scenario: g_V_valuesXdoubleX_isXtypeOfXGType_DOUBLEXX_order_byXascX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("data").property("double", 3.2d).
        addV("data").property("double", 1.8d).
        addV("data").property("double", 2.5d)
      """
    And the traversal of
      """
      g.V().values("double").is(P.typeOf(GType.DOUBLE)).order().by(asc)
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | d[1.8].d |
      | d[2.5].d |
      | d[3.2].d |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX5_5dX_isXtypeOfXGType_DOUBLEXX_groupCount
    Given the empty graph
    And the traversal of
      """
      g.inject(5.5d).is(P.typeOf(GType.DOUBLE)).groupCount()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"d[5.5].d":"d[1].l"}] |

  Scenario: g_V_valuesXageX_isXtypeOfXGType_DOUBLEXX
    Given the modern graph
    And the traversal of
      """
      g.V().values("age").is(P.typeOf(GType.DOUBLE))
      """
    When iterated to list
    Then the result should be empty