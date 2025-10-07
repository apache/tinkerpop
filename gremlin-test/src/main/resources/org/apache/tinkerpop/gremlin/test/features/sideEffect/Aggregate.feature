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

  Scenario: g_V_localXaggregateXxX_byXageXX_capXxX
    Given the modern graph
    And the traversal of
      """
      g.V().local(aggregate("x").by("age")).cap("x")
      """
    When iterated next
    Then the result should be unordered
      | result |
      | d[29].i |
      | d[27].i |
      | d[32].i |
      | d[35].i |

  @WithProductiveByStrategy
  Scenario: g_withStrategiesXProductiveByStrategyX_V_localXaggregateXxX_byXageXX_capXxX
    Given the modern graph
    And the traversal of
      """
      g.withStrategies(ProductiveByStrategy).V().local(aggregate("x").by("age")).cap("x")
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

  Scenario: g_V_localX_aggregateXa_byXnameXX_out_capXaX
    Given the modern graph
    And the traversal of
    """
    g.V().local(aggregate("a").by("name")).out().cap("a")
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

  Scenario: g_VX1X_localXaggregateXaX_byXnameXX_out_localXaggregateXaX_byXnameXX_name_capXaX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
    """
    g.V(vid1).local(aggregate("a").by("name")).out().local(aggregate("a").by("name")).values("name").cap("a")
    """
    When iterated next
    Then the result should be unordered
      | result |
      | marko |
      | vadas |
      | lop |
      | josh |

  Scenario: g_withSideEffectXa_setX_V_both_name_localXaggregateX_aXX_capXaX
    Given the modern graph
    And using the side effect a defined as "s[]"
    And the traversal of
      """
      g.V().both().values("name").local(aggregate("a")).cap("a")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | s[marko,vadas,lop,josh,ripple,peter] |

  Scenario: g_withSideEffectXa_set_inlineX_V_both_name_localXaggregateXaXX_capXaX
    Given the modern graph
    And the traversal of
      """
      g.withSideEffect("a", {"alice"}).V().both().values("name").local(aggregate("a")).cap("a")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | s[alice,marko,vadas,lop,josh,ripple,peter] |

  Scenario: g_V_localXaggregateXaX_byXoutEXcreatedX_countXX_out_out_localXaggregateXaX_byXinEXcreatedX_weight_sumXX_capXaX
    Given the modern graph
    And the traversal of
    """
    g.V().local(aggregate("a").by(__.outE("created").count())).
      out().out().
      local(aggregate("a").by(__.inE("created").values("weight").sum())).
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

  Scenario: g_withSideEffectXa_1_sumX_V_localXaggregateX_aX_byXageXX_capXaX
    Given the modern graph
    And the traversal of
    """
    g.withSideEffect("a", 1, Operator.sum).V().local(aggregate("a").by("age")).cap("a")
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
  Scenario: g_withSideEffectXa_123_minusX_V_localXaggregateX_aX_byXageXX_capXaX
    Given the modern graph
    And the traversal of
    """
    g.withSideEffect("a", 123, Operator.minus).V().local(aggregate("a").by("age")).cap("a")
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

  Scenario: g_withSideEffectXa_2_multX_V_localXaggregateX_aX_byXageXX_capXaX
    Given the modern graph
    And the traversal of
    """
    g.withSideEffect("a", 2, Operator.mult).V().local(aggregate("a").by("age")).cap("a")
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
  Scenario: g_withSideEffectXa_876960_divX_V_localXaggregateX_aX_byXageXX_capXaX
    Given the modern graph
    And the traversal of
    """
    g.withSideEffect("a", 876960, Operator.div).V().local(aggregate("a").by("age")).cap("a")
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

  Scenario: g_withSideEffectXa_1_minX_V_localXaggregateX_aX_byXageXX_capXaX
    Given the modern graph
    And the traversal of
    """
    g.withSideEffect("a", 1, Operator.min).V().local(aggregate("a").by("age")).cap("a")
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

  Scenario: g_withSideEffectXa_100_minX_V_localXaggregateX_aX_byXageXX_capXaX
    Given the modern graph
    And the traversal of
    """
    g.withSideEffect("a", 100, Operator.min).V().local(aggregate("a").by("age")).cap("a")
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

  Scenario: g_withSideEffectXa_1_maxX_V_localXaggregateX_aX_byXageXX_capXaX
    Given the modern graph
    And the traversal of
    """
    g.withSideEffect("a", 1, Operator.max).V().local(aggregate("a").by("age")).cap("a")
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

  Scenario: g_withSideEffectXa_100_maxX_V_localXaggregateX_aX_byXageX_capXaX
    Given the modern graph
    And the traversal of
    """
    g.withSideEffect("a", 100, Operator.max).V().local(aggregate("a").by("age")).cap("a")
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

  Scenario: g_withSideEffectXa_true_andX_V_constantXfalseX_localXaggregateX_aXX_capXaX
    Given the modern graph
    And the traversal of
    """
    g.withSideEffect("a", true, Operator.and).V().constant(false).local(aggregate("a")).cap("a")
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

  Scenario: g_withSideEffectXa_true_orX_V_constantXfalseX_localXaggregateX_aXX_capXaX
    Given the modern graph
    And the traversal of
    """
    g.withSideEffect("a", true, Operator.or).V().constant(false).local(aggregate("a")).cap("a")
    """
    When iterated to list
    Then the result should be unordered
      | result |
      | true |

  Scenario: g_withSideEffectXa_1_2_3_addAllX_V_aggregateXaX_byXageX_capXaX
    Given the modern graph
    And the traversal of
    """
    g.withSideEffect("a", [1i,2i,3i], Operator.addAll).V().aggregate("a").by("age").cap("a")
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

  Scenario: g_withSideEffectXa_1_2_3_addAllX_V_localXaggregateX_aX_byXageXX_capXaX
    Given the modern graph
    And the traversal of
    """
    g.withSideEffect("a", [1i,2i,3i], Operator.addAll).V().local(aggregate("a").by("age")).cap("a")
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
  Scenario: g_withSideEffectXa_1_2_3_assignX_V_aggregateXaX_byXageX_capXaX
    Given the modern graph
    And the traversal of
    """
    g.withSideEffect("a", [1i,2i,3i], Operator.assign).V().aggregate("a").by("age").cap("a")
    """
    When iterated next
    Then the result should be unordered
      | result |
      | d[29].i |
      | d[27].i |
      | d[32].i |
      | d[35].i |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_withSideEffectXa_1_2_3_assignX_V_order_byXageX_localXaggregateX_aX_byXageXX_capXaX
    Given the modern graph
    And the traversal of
    # add order().by("age") to deterministically assign a vertex with the largest age value at the end
    """
    g.withSideEffect("a", [1i,2i,3i], Operator.assign).V().order().by("age").local(aggregate("a").by("age")).cap("a")
    """
    When iterated next
    Then the result should be unordered
      | result |
      | d[35].i |

  Scenario: g_V_localXaggregateXa_nameXX_out_capXaX
    Given the modern graph
    And the traversal of
      """
      g.V().local(aggregate("a").by("name")).out().cap("a")
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

  Scenario: g_withSideEffectXa_setX_V_both_name_localXaggregateXaXX_capXaX
    Given the modern graph
    And using the side effect a defined as "s[]"
    And the traversal of
      """
      g.V().both().values("name").local(aggregate("a")).cap("a")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | s[marko,vadas,lop,josh,ripple,peter] |

  Scenario: g_V_localXaggregateXaXX_outE_inV_localXaggregateXaXX_capXaX_unfold_dedup
    Given the modern graph
    And the traversal of
      """
      g.V().local(aggregate("a")).outE().inV().local(aggregate("a")).cap("a").unfold().dedup()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |
      | v[vadas] |
      | v[lop] |
      | v[josh] |
      | v[ripple] |
      | v[peter] |

  Scenario: g_V_hasLabelXpersonX_localXaggregateXaXX_outXcreatedX_localXaggregateXaXX_capXaX
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("person").local(aggregate("a")).out("created").local(aggregate("a")).cap("a")
      """
    When iterated next
    Then the result should be unordered
      | result |
      | v[marko] |
      | v[lop] |
      | v[lop] |
      | v[lop] |
      | v[vadas] |
      | v[ripple] |
      | v[josh]  |
      | v[peter] |

  Scenario: g_V_localXaggregateXaXX_repeatXout_localXaggregateXaXXX_timesX2X_capXaX_unfold_groupCount
    Given the modern graph
    And the traversal of
      """
      g.V().local(aggregate("a")).repeat(__.out().local(aggregate("a"))).times(2).cap("a").unfold().values("name").groupCount()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"marko":"d[1].l","lop":"d[5].l","vadas":"d[2].l","josh":"d[2].l","ripple":"d[3].l","peter":"d[1].l"}] |

  Scenario: g_V_hasXname_markoX_localXaggregateXaXX_outXknowsX_localXaggregateXaXX_outXcreatedX_localXaggregateXaXX_capXaX
    Given the modern graph
    And the traversal of
      """
      g.V().has("name", "marko").local(aggregate("a")).out("knows").local(aggregate("a")).out("created").local(aggregate("a")).cap("a")
      """
    When iterated next
    Then the result should be unordered
      | result |
      | v[marko] |
      | v[vadas] |
      | v[josh] |
      | v[lop] |
      | v[ripple] |

  Scenario: g_V_hasLabelXsoftwareX_localXaggregateXaXX_inXcreatedX_localXaggregateXaXX_outXknowsX_localXaggregateXaXX_capXaX
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("software").local(aggregate("a")).in("created").local(aggregate("a")).out("knows").local(aggregate("a")).cap("a")
      """
    When iterated next
    Then the result should be unordered
      | result |
      | v[lop] |
      | v[marko] |
      | v[josh] |
      | v[josh] |
      | v[josh] |
      | v[peter] |
      | v[vadas] |
      | v[ripple] |

  Scenario: g_V_localXaggregateXaXX_outE_hasXweight_lgtX0_5XX_inV_localXaggregateXaXX_capXaX_unfold_path
    Given the modern graph
    And the traversal of
      """
      g.V().local(aggregate("a")).outE().has("weight", P.gt(0.5)).inV().local(aggregate("a")).cap("a").unfold().path()
      """
    When iterated to list
    Then the result should have a count of 8

  Scenario: g_V_localXaggregateXaXX_bothE_sampleX1X_otherV_localXaggregateXaXX_capXaX_unfold_groupCount_byXlabelX
    Given the modern graph
    And the traversal of
      """
      g.V().local(aggregate("a")).bothE().sample(1).otherV().local(aggregate("a")).cap("a").unfold().groupCount().by(T.label)
      """
    When iterated to list
    Then the result should have a count of 1

  Scenario: g_V_hasLabelXpersonX_localXaggregateXaXX_outE_inV_simplePath_localXaggregateXaXX_capXaX_unfold_hasLabelXsoftwareX_count
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("person").local(aggregate("a")).outE().inV().simplePath().local(aggregate("a")).cap("a").unfold().hasLabel("software").count()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[4].l |

  Scenario: g_V_localXaggregateXaXX_unionXout_inX_localXaggregateXaXX_capXaX_unfold_dedup_valuesXnameX
    Given the modern graph
    And the traversal of
      """
      g.V().local(aggregate("a")).union(__.out(), __.in()).local(aggregate("a")).cap("a").unfold().dedup().values("name")
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

  Scenario: g_V_hasXname_joshX_localXaggregateXaXX_outE_hasXweight_ltX1_0XX_inV_localXaggregateXaXX_outE_inV_localXaggregateXaXX_capXaX
    Given the modern graph
    And the traversal of
      """
      g.V().has("name", "josh").local(aggregate("a")).outE().has("weight", P.lt(1.0)).inV().local(aggregate("a")).outE().inV().local(aggregate("a")).cap("a")
      """
    When iterated next
    Then the result should be unordered
      | result |
      | v[josh] |
      | v[lop] |

  Scenario: g_V_hasLabelXpersonX_localXaggregateXaXX_outE_order_byXweightX_limitX1X_inV_localXaggregateXaXX_capXaX
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("person").local(aggregate("a")).outE().order().by("weight").limit(1).inV().local(aggregate("a")).cap("a")
      """
    When iterated next
    Then the result should be unordered
      | result |
      | v[marko] |
      | v[vadas] |
      | v[josh] |
      | v[lop] |
      | v[peter] |
