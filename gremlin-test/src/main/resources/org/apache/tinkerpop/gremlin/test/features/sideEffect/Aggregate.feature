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

@StepClassSideEffect @StepAggregate
Feature: Step - aggregate()

  Scenario: g_V_valueXnameX_aggregateXxX_capXxX
    Given the modern graph
    And the traversal of
      """
      g.V().values("name").aggregate("x").cap("x")
      """
    When iterated next
    Then the result should be unordered
      | result |
      | marko |
      | josh |
      | peter |
      | lop |
      | vadas |
      | ripple |

  Scenario: g_V_valueXnameX_aggregateXglobal_xX_capXxX
    Given the modern graph
    And the traversal of
      """
      g.V().values("name").aggregate(Scope.global,"x").cap("x")
      """
    When iterated next
    Then the result should be unordered
      | result |
      | marko |
      | josh |
      | peter |
      | lop |
      | vadas |
      | ripple |

  Scenario: g_V_aggregateXxX_byXnameX_capXxX
    Given the modern graph
    And the traversal of
      """
      g.V().aggregate("x").by("name").cap("x")
      """
    When iterated next
    Then the result should be unordered
      | result |
      | marko |
      | josh |
      | peter |
      | lop |
      | vadas |
      | ripple |

  Scenario: g_V_out_aggregateXaX_path
    Given the modern graph
    And the traversal of
      """
      g.V().out().aggregate("a").path()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | p[v[marko],v[lop]] |
      | p[v[marko],v[vadas]] |
      | p[v[marko],v[josh]] |
      | p[v[josh],v[ripple]] |
      | p[v[josh],v[lop]] |
      | p[v[peter],v[lop]] |

  Scenario: g_V_hasLabelXpersonX_aggregateXxX_byXageX_capXxX_asXyX_selectXyX
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("person").aggregate("x").by("age").cap("x").as("y").select("y")
      """
    When iterated next
    Then the result should be unordered
      | result |
      | d[29].i |
      | d[27].i |
      | d[32].i |
      | d[35].i |

  Scenario: g_V_aggregateXxX_byXageX_capXxX
    Given the modern graph
    And the traversal of
      """
      g.V().aggregate("x").by("age").cap("x")
      """
    When iterated next
    Then the result should be unordered
      | result |
      | d[29].i |
      | d[27].i |
      | d[32].i |
      | d[35].i |

  Scenario: g_V_aggregateXlocal_xX_byXageX_capXxX
    Given the modern graph
    And the traversal of
      """
      g.V().aggregate(Scope.local, "x").by("age").cap("x")
      """
    When iterated next
    Then the result should be unordered
      | result |
      | d[29].i |
      | d[27].i |
      | d[32].i |
      | d[35].i |

  @WithProductiveByStrategy
  Scenario: g_withStrategiesXProductiveByStrategyX_V_aggregateXlocal_xX_byXageX_capXxX
    Given the modern graph
    And the traversal of
      """
      g.withStrategies(ProductiveByStrategy).V().aggregate(Scope.local, "x").by("age").cap("x")
      """
    When iterated next
    Then the result should be unordered
      | result |
      | d[29].i |
      | d[27].i |
      | d[32].i |
      | d[35].i |
      | null    |
      | null    |

  Scenario: g_V_aggregateXlocal_a_nameX_out_capXaX
    Given the modern graph
    And the traversal of
    """
    g.V().aggregate(Scope.local,"a").by("name").out().cap("a")
    """
    When iterated next
    Then the result should be unordered
    | result |
    | marko |
    | vadas |
    | lop |
    | josh |
    | ripple |
    | peter  |

  Scenario: g_VX1X_aggregateXlocal_aX_byXnameX_out_aggregateXlocal_aX_byXnameX_name_capXaX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
    """
    g.V(vid1).aggregate(Scope.local,"a").by("name").out().aggregate(Scope.local,"a").by("name").values("name").cap("a")
    """
    When iterated next
    Then the result should be unordered
    | result |
    | marko |
    | vadas |
    | lop |
    | josh |

  @GremlinLangScriptOnly
  Scenario: g_withSideEffectXa_setX_V_both_name_aggregateXlocal_aX_capXaX
    Given the modern graph
    And using the parameter xx1 defined as "s[]"
    And the traversal of
      """
      g.withSideEffect("a", xx1).V().both().values("name").aggregate(Scope.local,"a").cap("a")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | s[marko,vadas,lop,josh,ripple,peter] |

  @GremlinLangScriptOnly
  Scenario: g_withSideEffectXa_set_inlineX_V_both_name_aggregateXlocal_aX_capXaX
    Given the modern graph
    And the traversal of
      """
      g.withSideEffect("a", {"alice"}).V().both().values("name").aggregate(local,"a").cap("a")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | s[alice,marko,vadas,lop,josh,ripple,peter] |

  Scenario: g_V_aggregateXlocal_aX_byXoutEXcreatedX_countX_out_out_aggregateXlocal_aX_byXinEXcreatedX_weight_sumX
    Given the modern graph
    And the traversal of
    """
    g.V().aggregate(Scope.local,"a").
    by(__.outE("created").count()).
    out().out().aggregate(Scope.local,"a").
    by(__.inE("created").values("weight").sum()).
    cap("a")
    """
    When iterated next
    Then the result should be unordered
    | result |
    | d[1].l |
    | d[1].l |
    | d[0].l |
    | d[0].l |
    | d[0].l |
    | d[2].l |
    | d[1.0].d |
    | d[1.0].d |

  Scenario: g_V_aggregateXxX_byXvaluesXageX_isXgtX29XXX_capXxX
    Given the modern graph
    And the traversal of
    """
    g.V().aggregate("x").by(__.values("age").is(P.gt(29))).cap("x")
    """
    When iterated next
    Then the result should be unordered
    | result |
    | d[32].i |
    | d[35].i |

  @WithProductiveByStrategy
  Scenario: g_withStrategiesXProductiveByStrategyX_V_aggregateXxX_byXvaluesXageX_isXgtX29XXX_capXxX
    Given the modern graph
    And the traversal of
    """
    g.withStrategies(ProductiveByStrategy).V().aggregate("x").by(__.values("age").is(P.gt(29))).cap("x")
    """
    When iterated next
    Then the result should be unordered
    | result |
    | d[32].i |
    | d[35].i |
    | null |
    | null |
    | null |
    | null |

  @GraphComputerVerificationStarGraphExceeded
  Scenario: g_V_aggregateXxX_byXout_order_byXnameXX_capXxX
    Given the modern graph
    And the traversal of
    """
    g.V().aggregate("x").by(__.out().order().by("name")).cap("x")
    """
    When iterated next
    Then the result should be unordered
    | result |
    | v[josh] |
    | v[lop] |
    | v[lop] |

  @GraphComputerVerificationReferenceOnly @WithProductiveByStrategy
  Scenario: g_withStrategiesXProductiveByStrategyX_V_aggregateXxX_byXout_order_byXnameXX_capXxX
    Given the modern graph
    And the traversal of
    """
    g.withStrategies(ProductiveByStrategy).V().aggregate("x").by(__.out().order().by("name")).cap("x")
    """
    When iterated next
    Then the result should be unordered
    | result |
    | v[josh] |
    | v[lop] |
    | v[lop] |
    | null |
    | null |
    | null |

  Scenario: g_V_aggregateXaX_hasXperson_age_gteX30XXX_capXaX_unfold_valuesXnameX
    Given the modern graph
    And the traversal of
    """
    g.V().aggregate("a").has("person","age", P.gte(30)).cap("a").unfold().values("name")
    """
    When iterated to list
    Then the result should be unordered
    | result |
    | marko |
    | josh |
    | peter |
    | lop |
    | vadas |
    | ripple |

  Scenario: g_withSideEffectXa_1_sumX_V_aggregateXaX_byXageX_capXaX
    Given the modern graph
    And the traversal of
    """
    g.withSideEffect("a", 1, Operator.sum).V().aggregate("a").by("age").cap("a")
    """
    When iterated to list
    Then the result should be unordered
    | result   |
    | d[124].i |

  Scenario: g_withSideEffectXa_1_sumX_V_aggregateXlocal_aX_byXageX_capXaX
    Given the modern graph
    And the traversal of
    """
    g.withSideEffect("a", 1, Operator.sum).V().aggregate(Scope.local, "a").by("age").cap("a")
    """
    When iterated to list
    Then the result should be unordered
      | result   |
      | d[124].i |

  @GraphComputerVerificationStrategyNotSupported
  Scenario: g_withSideEffectXa_123_minusX_V_aggregateXaX_byXageX_capXaX
    Given the modern graph
    And the traversal of
    """
    g.withSideEffect("a", 123, Operator.minus).V().aggregate("a").by("age").cap("a")
    """
    When iterated to list
    Then the result should be unordered
    | result |
    | d[0].i |

  @GraphComputerVerificationStrategyNotSupported
  Scenario: g_withSideEffectXa_123_minusX_V_aggregateXlocal_aX_byXageX_capXaX
    Given the modern graph
    And the traversal of
    """
    g.withSideEffect("a", 123, Operator.minus).V().aggregate(Scope.local, "a").by("age").cap("a")
    """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[0].i |

  Scenario: g_withSideEffectXa_2_multX_V_aggregateXaX_byXageX_capXaX
    Given the modern graph
    And the traversal of
    """
    g.withSideEffect("a", 2, Operator.mult).V().aggregate("a").by("age").cap("a")
    """
    When iterated to list
    Then the result should be unordered
    | result |
    | d[1753920].i |

  Scenario: g_withSideEffectXa_2_multX_V_aggregateXlocal_aX_byXageX_capXaX
    Given the modern graph
    And the traversal of
    """
    g.withSideEffect("a", 2, Operator.mult).V().aggregate(Scope.local, "a").by("age").cap("a")
    """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[1753920].i |

  @GraphComputerVerificationStrategyNotSupported
  Scenario: g_withSideEffectXa_876960_divX_V_aggregateXaX_byXageX_capXaX
    Given the modern graph
    And the traversal of
    """
    g.withSideEffect("a", 876960, Operator.div).V().aggregate("a").by("age").cap("a")
    """
    When iterated to list
    Then the result should be unordered
    | result |
    | d[1].i |

  @GraphComputerVerificationStrategyNotSupported
  Scenario: g_withSideEffectXa_876960_divX_V_aggregateXlocal_aX_byXageX_capXaX
    Given the modern graph
    And the traversal of
    """
    g.withSideEffect("a", 876960, Operator.div).V().aggregate(Scope.local, "a").by("age").cap("a")
    """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[1].i |

  Scenario: g_withSideEffectXa_1_minX_V_aggregateXaX_byXageX_capXaX
    Given the modern graph
    And the traversal of
    """
    g.withSideEffect("a", 1, Operator.min).V().aggregate("a").by("age").cap("a")
    """
    When iterated to list
    Then the result should be unordered
    | result |
    | d[1].i |

  Scenario: g_withSideEffectXa_1_minX_V_aggregateXlocal_aX_byXageX_capXaX
    Given the modern graph
    And the traversal of
    """
    g.withSideEffect("a", 1, Operator.min).V().aggregate(Scope.local, "a").by("age").cap("a")
    """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[1].i |

  Scenario: g_withSideEffectXa_100_minX_V_aggregateXaX_byXageX_capXaX
    Given the modern graph
    And the traversal of
    """
    g.withSideEffect("a", 100, Operator.min).V().aggregate("a").by("age").cap("a")
    """
    When iterated to list
    Then the result should be unordered
    | result |
    | d[27].i |

  Scenario: g_withSideEffectXa_100_minX_V_aggregateXlocal_aX_byXageX_capXaX
    Given the modern graph
    And the traversal of
    """
    g.withSideEffect("a", 100, Operator.min).V().aggregate(Scope.local, "a").by("age").cap("a")
    """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[27].i |

  Scenario: g_withSideEffectXa_1_maxX_V_aggregateXaX_byXageX_capXaX
    Given the modern graph
    And the traversal of
    """
    g.withSideEffect("a", 1, Operator.max).V().aggregate("a").by("age").cap("a")
    """
    When iterated to list
    Then the result should be unordered
    | result |
    | d[35].i |

  Scenario: g_withSideEffectXa_1_maxX_V_aggregateXlocal_aX_byXageX_capXaX
    Given the modern graph
    And the traversal of
    """
    g.withSideEffect("a", 1, Operator.max).V().aggregate(Scope.local, "a").by("age").cap("a")
    """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[35].i |

  Scenario: g_withSideEffectXa_100_maxX_V_aggregateXaX_byXageX_capXaX
    Given the modern graph
    And the traversal of
    """
    g.withSideEffect("a", 100, Operator.max).V().aggregate("a").by("age").cap("a")
    """
    When iterated to list
    Then the result should be unordered
    | result |
    | d[100].i |

  Scenario: g_withSideEffectXa_100_maxX_V_aggregateXlocal_aX_byXageX_capXaX
    Given the modern graph
    And the traversal of
    """
    g.withSideEffect("a", 100, Operator.max).V().aggregate(Scope.local, "a").by("age").cap("a")
    """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[100].i |

  Scenario: g_withSideEffectXa_true_andX_V_constantXfalseX_aggregateXaX_capXaX
    Given the modern graph
    And the traversal of
    """
    g.withSideEffect("a", true, Operator.and).V().constant(false).aggregate("a").cap("a")
    """
    When iterated to list
    Then the result should be unordered
      | result |
      | false |

  Scenario: g_withSideEffectXa_true_andX_V_constantXfalseX_aggregateXlocal_aX_capXaX
    Given the modern graph
    And the traversal of
    """
    g.withSideEffect("a", true, Operator.and).V().constant(false).aggregate(Scope.local, "a").cap("a")
    """
    When iterated to list
    Then the result should be unordered
      | result |
      | false |

  Scenario: g_withSideEffectXa_true_orX_V_constantXfalseX_aggregateXaX_capXaX
    Given the modern graph
    And the traversal of
    """
    g.withSideEffect("a", true, Operator.or).V().constant(false).aggregate("a").cap("a")
    """
    When iterated to list
    Then the result should be unordered
      | result |
      | true |

  Scenario: g_withSideEffectXa_true_orX_V_constantXfalseX_aggregateXlocal_aX_capXaX
    Given the modern graph
    And the traversal of
    """
    g.withSideEffect("a", true, Operator.or).V().constant(false).aggregate(Scope.local, "a").cap("a")
    """
    When iterated to list
    Then the result should be unordered
      | result |
      | true |

  Scenario: g_withSideEffectXa_xx1_addAllX_V_aggregateXaX_byXageX_capXaX
    Given the modern graph
    And using the parameter xx1 defined as "l[d[1].i,d[2].i,d[3].i]"
    And the traversal of
    """
    g.withSideEffect("a", xx1, Operator.addAll).V().aggregate("a").by("age").cap("a")
    """
    When iterated next
    Then the result should be unordered
    | result |
    | d[1].i |
    | d[2].i |
    | d[3].i |
    | d[29].i |
    | d[27].i |
    | d[32].i |
    | d[35].i |

  Scenario: g_withSideEffectXa_xx1_addAllX_V_aggregateXlocal_aX_byXageX_capXaX
    Given the modern graph
    And using the parameter xx1 defined as "l[d[1].i,d[2].i,d[3].i]"
    And the traversal of
    """
    g.withSideEffect("a", xx1, Operator.addAll).V().aggregate(Scope.local, "a").by("age").cap("a")
    """
    When iterated next
    Then the result should be unordered
      | result |
      | d[1].i |
      | d[2].i |
      | d[3].i |
      | d[29].i |
      | d[27].i |
      | d[32].i |
      | d[35].i |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_withSideEffectXa_xx1_assignX_V_aggregateXaX_byXageX_capXaX
    Given the modern graph
    And using the parameter xx1 defined as "l[d[1].i,d[2].i,d[3].i]"
    And the traversal of
    """
    g.withSideEffect("a", xx1, Operator.assign).V().aggregate("a").by("age").cap("a")
    """
    When iterated next
    Then the result should be unordered
    | result |
    | d[29].i |
    | d[27].i |
    | d[32].i |
    | d[35].i |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_withSideEffectXa_xx1_assignX_V_order_byXageX_aggregateXlocal_aX_byXageX_capXaX
    Given the modern graph
    And using the parameter xx1 defined as "l[d[1].i,d[2].i,d[3].i]"
    And the traversal of
    # add order().by("age") to deterministically assign a vertex with the largest age value at the end
    """
    g.withSideEffect("a", xx1, Operator.assign).V().order().by("age").aggregate(Scope.local, "a").by("age").cap("a")
    """
    When iterated next
    Then the result should be unordered
      | result |
      | d[35].i |
