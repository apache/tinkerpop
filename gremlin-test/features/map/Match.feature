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

@StepClassMap @StepMatch
Feature: Step - match()

  Scenario: g_V_valueMap_matchXa_selectXnameX_bX
    Given the modern graph
    And the traversal of
      """
      g.V().valueMap().match(__.as("a").select("name").as("b"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a":{"name":["marko"],"age":[29]},"b":["marko"]}] |
      | m[{"a":{"name":["vadas"],"age":[27]},"b":["vadas"]}] |
      | m[{"a":{"name":["lop"],"lang":["java"]},"b":["lop"]}] |
      | m[{"a":{"name":["josh"],"age":[32]},"b":["josh"]}] |
      | m[{"a":{"name":["ripple"],"lang":["java"]},"b":["ripple"]}] |
      | m[{"a":{"name":["peter"],"age":[35]},"b":["peter"]}] |

  Scenario: g_V_matchXa_out_bX
    Given the modern graph
    And the traversal of
      """
      g.V().match(__.as("a").out().as("b"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a":"v[marko]","b":"v[lop]"}] |
      | m[{"a":"v[marko]","b":"v[vadas]"}] |
      | m[{"a":"v[marko]","b":"v[josh]"}] |
      | m[{"a":"v[josh]","b":"v[ripple]"}] |
      | m[{"a":"v[josh]","b":"v[lop]"}] |
      | m[{"a":"v[peter]","b":"v[lop]"}] |

  Scenario: g_V_matchXa_out_bX_selectXb_idX
    Given the modern graph
    And the traversal of
      """
      g.V().match(__.as("a").out().as("b")).select("b").by(T.id)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[lop].id |
      | v[vadas].id |
      | v[josh].id |
      | v[ripple].id |
      | v[lop].id |
      | v[lop].id |

  Scenario: g_V_matchXa_knows_b__b_created_cX
    Given the modern graph
    And the traversal of
      """
      g.V().match(__.as("a").out("knows").as("b"),
                  __.as("b").out("created").as("c"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a":"v[marko]","b":"v[josh]", "c":"v[ripple]"}] |
      | m[{"a":"v[marko]","b":"v[josh]", "c":"v[lop]"}] |

  Scenario: g_V_matchXb_created_c__a_knows_bX
    Given the modern graph
    And the traversal of
      """
      g.V().match(__.as("b").out("created").as("c"),
                  __.as("a").out("knows").as("b"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a":"v[marko]","b":"v[josh]", "c":"v[ripple]"}] |
      | m[{"a":"v[marko]","b":"v[josh]", "c":"v[lop]"}] |

  Scenario: g_V_matchXa_created_b__b_0created_cX_whereXa_neq_cX_selectXa_cX
    Given the modern graph
    And the traversal of
      """
      g.V().match(__.as("a").out("created").as("b"),
                  __.as("b").in("created").as("c")).where("a", P.neq("c")).select("a", "c")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a":"v[marko]","c":"v[josh]"}] |
      | m[{"a":"v[marko]","c":"v[peter]"}] |
      | m[{"a":"v[josh]","c":"v[marko]"}] |
      | m[{"a":"v[josh]","c":"v[peter]"}] |
      | m[{"a":"v[peter]","c":"v[marko]"}] |
      | m[{"a":"v[peter]","c":"v[josh]"}] |

  Scenario: g_V_matchXd_0knows_a__d_hasXname_vadasX__a_knows_b__b_created_cX
    Given the modern graph
    And the traversal of
      """
      g.V().match(__.as("d").in("knows").as("a"),
                  __.as("d").has("name", "vadas"),
                  __.as("a").out("knows").as("b"),
                  __.as("b").out("created").as("c"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a":"v[marko]","b":"v[josh]","c":"v[ripple]","d":"v[vadas]"}] |
      | m[{"a":"v[marko]","b":"v[josh]","c":"v[lop]","d":"v[vadas]"}] |

  @GraphComputerVerificationStarGraphExceeded
  Scenario: g_V_matchXa_created_lop_b__b_0created_29_c__c_whereXrepeatXoutX_timesX2XXX
    Given the modern graph
    And the traversal of
      """
      g.V().match(__.as("a").out("created").has("name", "lop").as("b"),
                  __.as("b").in("created").has("age", 29).as("c"),
                  __.as("c").where(__.repeat(__.out()).times(2)))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a":"v[marko]","b":"v[lop]","c":"v[marko]"}] |
      | m[{"a":"v[josh]","b":"v[lop]","c":"v[marko]"}] |
      | m[{"a":"v[peter]","b":"v[lop]","c":"v[marko]"}] |

  Scenario: g_V_asXaX_out_asXbX_matchXa_out_count_c__b_in_count_cX
    Given the modern graph
    And the traversal of
      """
      g.V().as("a").out().as("b").match(__.as("a").out().count().as("c"), __.as("b").in().count().as("c"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a":"v[marko]","b":"v[lop]","c":"d[3].l"}] |

  Scenario: g_V_matchXa__a_out_b__notXa_created_bXX
    Given the modern graph
    And the traversal of
      """
      g.V().match(__.as("a").out().as("b"),__.not(__.as("a").out("created").as("b")))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a":"v[marko]","b":"v[vadas]"}] |
      | m[{"a":"v[marko]","b":"v[josh]"}] |

  @GraphComputerVerificationStarGraphExceeded
  Scenario: g_V_matchXa_created_lop_b__b_0created_29_cX_whereXc_repeatXoutX_timesX2XX_selectXa_b_cX
    Given the modern graph
    And the traversal of
      """
      g.V().match(__.as("a").out("created").has("name", "lop").as("b"),
                  __.as("b").in("created").has("age", 29).as("c")).where(__.as("c").repeat(__.out()).times(2)).select("a", "b", "c")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a":"v[marko]","b":"v[lop]","c":"v[marko]"}] |
      | m[{"a":"v[josh]","b":"v[lop]","c":"v[marko]"}] |
      | m[{"a":"v[peter]","b":"v[lop]","c":"v[marko]"}] |

  Scenario: g_V_out_out_matchXa_0created_b__b_0knows_cX_selectXcX_outXcreatedX_name
    Given the modern graph
    And the traversal of
      """
      g.V().out().out().match(__.as("a").in("created").as("b"),
                              __.as("b").in("knows").as("c")).
                        select("c").out("created").values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | lop    |
      | lop    |

  Scenario: g_V_matchXa_knows_b__b_created_c__a_created_cX_dedupXa_b_cX_selectXaX_byXnameX
    Given the modern graph
    And the traversal of
      """
      g.V().match(__.as("a").out("knows").as("b"),
                  __.as("b").out("created").as("c"),
                  __.as("a").out("created").as("c")).dedup("a", "b", "c").select("a").by("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko |

  Scenario: g_V_matchXa_created_b__a_repeatXoutX_timesX2XX_selectXa_bX
    Given the modern graph
    And the traversal of
      """
      g.V().match(__.as("a").out("created").as("b"),
                  __.as("a").repeat(__.out()).times(2).as("b")).select("a", "b")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a":"v[marko]","b":"v[lop]"}] |

  Scenario: g_V_notXmatchXa_age_b__a_name_cX_whereXb_eqXcXX_selectXaXX_name
    Given the modern graph
    And the traversal of
      """
      g.V().not(__.match(__.as("a").values("age").as("b"), __.as("a").values("name").as("c")).where("b", P.eq("c")).select("a")).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko |
      | vadas |
      | lop   |
      | josh  |
      | ripple |
      | peter  |

  Scenario: g_V_matchXa_knows_b__andXa_created_c__b_created_c__andXb_created_count_d__a_knows_count_dXXX
    Given the modern graph
    And the traversal of
      """
      g.V().match(__.as("a").out("knows").as("b"),
                  __.and(__.as("a").out("created").as("c"),
                         __.as("b").out("created").as("c"),
                         __.and(__.as("b").out("created").count().as("d"),
                                __.as("a").out("knows").count().as("d"))))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a":"v[marko]","b":"v[josh]","c":"v[lop]","d":"d[2].l"}] |

  Scenario: g_V_matchXa_whereXa_neqXcXX__a_created_b__orXa_knows_vadas__a_0knows_and_a_hasXlabel_personXX__b_0created_c__b_0created_count_isXgtX1XXX_selectXa_b_cX_byXidX
    Given the modern graph
    And the traversal of
      """
      g.V().match(__.where("a", P.neq("c")),
                  __.as("a").out("created").as("b"),
                  __.or(__.as("a").out("knows").has("name", "vadas"),
                        __.as("a").in("knows").and().as("a").has(T.label, "person")),
                  __.as("b").in("created").as("c"),
                  __.as("b").in("created").count().is(P.gt(1))).select("a", "b", "c").by(T.id)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a":"v[marko].id","b":"v[lop].id","c":"v[josh].id"}] |
      | m[{"a":"v[marko].id","b":"v[lop].id","c":"v[peter].id"}] |
      | m[{"a":"v[josh].id","b":"v[lop].id","c":"v[marko].id"}] |
      | m[{"a":"v[josh].id","b":"v[lop].id","c":"v[peter].id"}] |

  Scenario: g_V_matchXa__a_both_b__b_both_cX_dedupXa_bX
    Given the modern graph
    And the traversal of
      """
      g.V().match(__.as("a").both().as("b"),
                  __.as("b").both().as("c")).dedup("a", "b")
      """
    When iterated to list
    Then the result should be of
      | result |
      | m[{"a":"v[marko]","b":"v[lop]","c":"v[marko]"}] |
      | m[{"a":"v[marko]","b":"v[lop]","c":"v[josh]"}] |
      | m[{"a":"v[marko]","b":"v[lop]","c":"v[peter]"}] |
      | m[{"a":"v[marko]","b":"v[vadas]","c":"v[marko]"}] |
      | m[{"a":"v[marko]","b":"v[josh]","c":"v[ripple]"}] |
      | m[{"a":"v[marko]","b":"v[josh]","c":"v[lop]"}] |
      | m[{"a":"v[marko]","b":"v[josh]","c":"v[marko]"}] |
      | m[{"a":"v[vadas]","b":"v[marko]","c":"v[lop]"}] |
      | m[{"a":"v[vadas]","b":"v[marko]","c":"v[vadas]"}] |
      | m[{"a":"v[vadas]","b":"v[marko]","c":"v[josh]"}] |
      | m[{"a":"v[lop]","b":"v[marko]","c":"v[lop]"}] |
      | m[{"a":"v[lop]","b":"v[marko]","c":"v[vadas]"}] |
      | m[{"a":"v[lop]","b":"v[marko]","c":"v[josh]"}] |
      | m[{"a":"v[lop]","b":"v[josh]","c":"v[ripple]"}] |
      | m[{"a":"v[lop]","b":"v[josh]","c":"v[lop]"}] |
      | m[{"a":"v[lop]","b":"v[josh]","c":"v[marko]"}] |
      | m[{"a":"v[lop]","b":"v[peter]","c":"v[lop]"}] |
      | m[{"a":"v[josh]","b":"v[ripple]","c":"v[josh]"}] |
      | m[{"a":"v[josh]","b":"v[lop]","c":"v[marko]"}] |
      | m[{"a":"v[josh]","b":"v[lop]","c":"v[josh]"}] |
      | m[{"a":"v[josh]","b":"v[lop]","c":"v[peter]"}] |
      | m[{"a":"v[josh]","b":"v[marko]","c":"v[lop]"}] |
      | m[{"a":"v[josh]","b":"v[marko]","c":"v[vadas]"}] |
      | m[{"a":"v[josh]","b":"v[marko]","c":"v[josh]"}] |
      | m[{"a":"v[ripple]","b":"v[josh]","c":"v[ripple]"}] |
      | m[{"a":"v[ripple]","b":"v[josh]","c":"v[lop]"}] |
      | m[{"a":"v[ripple]","b":"v[josh]","c":"v[marko]"}] |
      | m[{"a":"v[peter]","b":"v[lop]","c":"v[marko]"}] |
      | m[{"a":"v[peter]","b":"v[lop]","c":"v[josh]"}] |
      | m[{"a":"v[peter]","b":"v[lop]","c":"v[peter]"}] |

  Scenario: g_V_matchXa_knows_b__b_created_lop__b_matchXb_created_d__d_0created_cX_selectXcX_cX_selectXa_b_cX
    Given the modern graph
    And the traversal of
      """
      g.V().match(__.as("a").out("knows").as("b"),
                  __.as("b").out("created").has("name", "lop"),
                  __.as("b").match(__.as("b").out("created").as("d"),
                                   __.as("d").in("created").as("c")).select("c").as("c")).select("a", "b", "c")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a":"v[marko]","b":"v[josh]","c":"v[josh]"}] |
      | m[{"a":"v[marko]","b":"v[josh]","c":"v[marko]"}] |
      | m[{"a":"v[marko]","b":"v[josh]","c":"v[josh]"}] |
      | m[{"a":"v[marko]","b":"v[josh]","c":"v[peter]"}] |

  Scenario: g_V_matchXa_knows_b__a_created_cX
    Given the modern graph
    And the traversal of
      """
      g.V().match(__.as("a").out("knows").as("b"),
                  __.as("a").out("created").as("c"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a":"v[marko]","b":"v[vadas]","c":"v[lop]"}] |
      | m[{"a":"v[marko]","b":"v[josh]","c":"v[lop]"}] |

  @GraphComputerVerificationReferenceOnly
  Scenario: g_V_matchXwhereXandXa_created_b__b_0created_count_isXeqX3XXXX__a_both_b__whereXb_inXX
    Given the modern graph
    And the traversal of
      """
      g.V().match(__.where(__.and(__.as("a").out("created").as("b"),
                                  __.as("b").in("created").count().is(P.eq(3)))),
                  __.as("a").both().as("b"),
                  __.where(__.as("b").in()))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a":"v[marko]","b":"v[lop]"}] |
      | m[{"a":"v[josh]","b":"v[lop]"}] |
      | m[{"a":"v[peter]","b":"v[lop]"}] |

  Scenario: g_V_matchXa_outEXcreatedX_order_byXweight_descX_limitX1X_inV_b__b_hasXlang_javaXX_selectXa_bX_byXnameX
    Given the modern graph
    And the traversal of
      """
      g.V().match(__.as("a").outE("created").order().by("weight", Order.desc).limit(1).inV().as("b"),
                  __.as("b").has("lang", "java")).select("a", "b").by("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a":"marko","b":"lop"}] |
      | m[{"a":"josh","b":"ripple"}] |
      | m[{"a":"peter","b":"lop"}] |

  @GraphComputerVerificationReferenceOnly
  Scenario: g_V_matchXa_both_b__b_both_cX_dedupXa_bX_byXlabelX
    Given the modern graph
    And the traversal of
      """
      g.V().match(__.as("a").both().as("b"),
                  __.as("b").both().as("c")).dedup("a", "b").by(T.label)
      """
    When iterated to list
    Then the result should be of
      | result |
      | m[{"a":"v[marko]","b":"v[lop]","c":"v[marko]"}] |
      | m[{"a":"v[marko]","b":"v[lop]","c":"v[josh]"}] |
      | m[{"a":"v[marko]","b":"v[lop]","c":"v[peter]"}] |
      | m[{"a":"v[marko]","b":"v[vadas]","c":"v[marko]"}] |
      | m[{"a":"v[marko]","b":"v[josh]","c":"v[ripple]"}] |
      | m[{"a":"v[marko]","b":"v[josh]","c":"v[lop]"}] |
      | m[{"a":"v[marko]","b":"v[josh]","c":"v[marko]"}] |
      | m[{"a":"v[vadas]","b":"v[marko]","c":"v[lop]"}] |
      | m[{"a":"v[vadas]","b":"v[marko]","c":"v[vadas]"}] |
      | m[{"a":"v[vadas]","b":"v[marko]","c":"v[josh]"}] |
      | m[{"a":"v[lop]","b":"v[marko]","c":"v[lop]"}] |
      | m[{"a":"v[lop]","b":"v[marko]","c":"v[vadas]"}] |
      | m[{"a":"v[lop]","b":"v[marko]","c":"v[josh]"}] |
      | m[{"a":"v[lop]","b":"v[josh]","c":"v[ripple]"}] |
      | m[{"a":"v[lop]","b":"v[josh]","c":"v[lop]"}] |
      | m[{"a":"v[lop]","b":"v[josh]","c":"v[marko]"}] |
      | m[{"a":"v[lop]","b":"v[peter]","c":"v[lop]"}] |
      | m[{"a":"v[josh]","b":"v[ripple]","c":"v[josh]"}] |
      | m[{"a":"v[josh]","b":"v[lop]","c":"v[marko]"}] |
      | m[{"a":"v[josh]","b":"v[lop]","c":"v[josh]"}] |
      | m[{"a":"v[josh]","b":"v[lop]","c":"v[peter]"}] |
      | m[{"a":"v[josh]","b":"v[marko]","c":"v[lop]"}] |
      | m[{"a":"v[josh]","b":"v[marko]","c":"v[vadas]"}] |
      | m[{"a":"v[josh]","b":"v[marko]","c":"v[josh]"}] |
      | m[{"a":"v[ripple]","b":"v[josh]","c":"v[ripple]"}] |
      | m[{"a":"v[ripple]","b":"v[josh]","c":"v[lop]"}] |
      | m[{"a":"v[ripple]","b":"v[josh]","c":"v[marko]"}] |
      | m[{"a":"v[peter]","b":"v[lop]","c":"v[marko]"}] |
      | m[{"a":"v[peter]","b":"v[lop]","c":"v[josh]"}] |
      | m[{"a":"v[peter]","b":"v[lop]","c":"v[peter]"}] |

  Scenario: g_V_matchXa_created_b__b_0created_aX
    Given the modern graph
    And the traversal of
      """
      g.V().match(__.as("a").out("created").as("b"),
                  __.as("b").in("created").as("a"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a":"v[marko]","b":"v[lop]"}] |
      | m[{"a":"v[josh]","b":"v[ripple]"}] |
      | m[{"a":"v[josh]","b":"v[lop]"}] |
      | m[{"a":"v[peter]","b":"v[lop]"}] |

  Scenario: g_V_asXaX_out_asXbX_matchXa_out_count_c__orXa_knows_b__b_in_count_c__and__c_isXgtX2XXXX
    Given the modern graph
    And the traversal of
      """
      g.V().as("a").out().as("b").match(__.as("a").out().count().as("c"),
                                        __.or(__.as("a").out("knows").as("b"),__.as("b").in().count().as("c").and().as("c").is(P.gt(2))))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a":"v[marko]","b":"v[lop]","c":"d[3].l"}] |
      | m[{"a":"v[marko]","b":"v[vadas]","c":"d[3].l"}] |
      | m[{"a":"v[marko]","b":"v[josh]","c":"d[3].l"}] |

  Scenario: g_V_matchXa_knows_count_bX_selectXbX
    Given the modern graph
    And the traversal of
      """
      g.V().match(__.as("a").out("knows").count().as("b")).select("b")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[2].l |
      | d[0].l |
      | d[0].l |
      | d[0].l |
      | d[0].l |
      | d[0].l |

  Scenario: g_V_matchXa_0sungBy_b__a_0writtenBy_c__b_writtenBy_d__c_sungBy_d__d_hasXname_GarciaXX
    Given the grateful graph
    And the traversal of
      """
      g.V().match(__.as("a").in("sungBy").as("b"),
                  __.as("a").in("writtenBy").as("c"),
                  __.as("b").out("writtenBy").as("d"),
                  __.as("c").out("sungBy").as("d"),
                  __.as("d").has("name", "Garcia"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a":"v[Garcia]","b":"v[CREAM PUFF WAR]","c":"v[CREAM PUFF WAR]","d":"v[Garcia]"}] |
      | m[{"a":"v[Garcia]","b":"v[CREAM PUFF WAR]","c":"v[CRYPTICAL ENVELOPMENT]","d":"v[Garcia]"}] |
      | m[{"a":"v[Garcia]","b":"v[CRYPTICAL ENVELOPMENT]","c":"v[CREAM PUFF WAR]","d":"v[Garcia]"}] |
      | m[{"a":"v[Garcia]","b":"v[CRYPTICAL ENVELOPMENT]","c":"v[CRYPTICAL ENVELOPMENT]","d":"v[Garcia]"}] |
      | m[{"a":"v[Grateful_Dead]","b":"v[CANT COME DOWN]","c":"v[DOWN SO LONG]","d":"v[Garcia]"}] |
      | m[{"a":"v[Grateful_Dead]","b":"v[THE ONLY TIME IS NOW]","c":"v[DOWN SO LONG]","d":"v[Garcia]"}] |

  Scenario: g_V_matchXa_hasXsong_name_sunshineX__a_mapX0followedBy_weight_meanX_b__a_0followedBy_c__c_filterXweight_whereXgteXbXXX_outV_dX_selectXdX_byXnameX
    Given the grateful graph
    And the traversal of
      """
      g.V().match(__.as("a").has("song", "name", "HERE COMES SUNSHINE"),
                  __.as("a").map(__.inE("followedBy").values("weight").mean()).as("b"),
                  __.as("a").inE("followedBy").as("c"),
                  __.as("c").filter(__.values("weight").where(P.gte("b"))).outV().as("d")).
            select("d").by("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | THE MUSIC NEVER STOPPED |
      | PROMISED LAND           |
      | PLAYING IN THE BAND     |
      | CASEY JONES             |
      | BIG RIVER               |
      | EL PASO                 |
      | LIBERTY                 |
      | LOOKS LIKE RAIN         |

  Scenario: g_V_matchXa_0sungBy_b__a_0sungBy_c__b_writtenBy_d__c_writtenBy_e__d_hasXname_George_HarisonX__e_hasXname_Bob_MarleyXX
    Given the grateful graph
    And the traversal of
      """
      g.V().match(__.as("a").in("sungBy").as("b"),
                  __.as("a").in("sungBy").as("c"),
                  __.as("b").out("writtenBy").as("d"),
                  __.as("c").out("writtenBy").as("e"),
                  __.as("d").has("name", "George_Harrison"),
                  __.as("e").has("name", "Bob_Marley"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a":"v[Garcia]","b":"v[I WANT TO TELL YOU]","c":"v[STIR IT UP]","d":"v[George_Harrison]","e":"v[Bob_Marley]"}] |

  Scenario: g_V_matchXa_hasXname_GarciaX__a_0writtenBy_b__a_0sungBy_bX
    Given the grateful graph
    And the traversal of
      """
      g.V().match(__.as("a").has("name", "Garcia"),
                  __.as("a").in("writtenBy").as("b"),
                  __.as("a").in("sungBy").as("b"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a":"v[Garcia]","b":"v[CREAM PUFF WAR]"}] |
      | m[{"a":"v[Garcia]","b":"v[CRYPTICAL ENVELOPMENT]"}] |

  Scenario: g_V_hasLabelXsongsX_matchXa_name_b__a_performances_cX_selectXb_cX_count
    Given the grateful graph
    And the traversal of
      """
       g.V().hasLabel("song").match(
                    __.as("a").values("name").as("b"),
                    __.as("a").values("performances").as("c")).select("b", "c").count()
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | d[584].l |

  Scenario: g_V_matchXa_followedBy_count_isXgtX10XX_b__a_0followedBy_count_isXgtX10XX_bX_count
    Given the grateful graph
    And the traversal of
      """
      g.V().match(__.as("a").out("followedBy").count().is(P.gt(10)).as("b"),
                  __.as("a").in("followedBy").count().is(P.gt(10)).as("b")).count()
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | d[6].l |

  Scenario: g_V_matchXa_0sungBy_b__a_0writtenBy_c__b_writtenBy_dX_whereXc_sungBy_dX_whereXd_hasXname_GarciaXX
    Given the grateful graph
    And the traversal of
      """
      g.V().match(__.as("a").in("sungBy").as("b"),
                  __.as("a").in("writtenBy").as("c"),
                  __.as("b").out("writtenBy").as("d")).
            where(__.as("c").out("sungBy").as("d")).
            where(__.as("d").has("name", "Garcia"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a":"v[Garcia]","b":"v[CREAM PUFF WAR]","c":"v[CREAM PUFF WAR]","d":"v[Garcia]"}] |
      | m[{"a":"v[Garcia]","b":"v[CREAM PUFF WAR]","c":"v[CRYPTICAL ENVELOPMENT]","d":"v[Garcia]"}] |
      | m[{"a":"v[Garcia]","b":"v[CRYPTICAL ENVELOPMENT]","c":"v[CREAM PUFF WAR]","d":"v[Garcia]"}] |
      | m[{"a":"v[Garcia]","b":"v[CRYPTICAL ENVELOPMENT]","c":"v[CRYPTICAL ENVELOPMENT]","d":"v[Garcia]"}] |
      | m[{"a":"v[Grateful_Dead]","b":"v[CANT COME DOWN]","c":"v[DOWN SO LONG]","d":"v[Garcia]"}] |
      | m[{"a":"v[Grateful_Dead]","b":"v[THE ONLY TIME IS NOW]","c":"v[DOWN SO LONG]","d":"v[Garcia]"}] |

  Scenario: g_V_matchXa_hasXname_GarciaX__a_0writtenBy_b__b_followedBy_c__c_writtenBy_d__whereXd_neqXaXXX
    Given the grateful graph
    And the traversal of
      """
      g.V().match(__.as("a").has("name", "Garcia"),
                  __.as("a").in("writtenBy").as("b"),
                  __.as("b").out("followedBy").as("c"),
                  __.as("c").out("writtenBy").as("d"),
                  __.where("d", P.neq("a")))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a":"v[Garcia]","b":"v[CRYPTICAL ENVELOPMENT]","c":"v[DRUMS]","d":"v[Grateful_Dead]"}] |
      | m[{"a":"v[Garcia]","b":"v[CRYPTICAL ENVELOPMENT]","c":"v[THE OTHER ONE]","d":"v[Weir]"}] |
      | m[{"a":"v[Garcia]","b":"v[CRYPTICAL ENVELOPMENT]","c":"v[WHARF RAT]","d":"v[Hunter]"}] |

  Scenario: g_V_matchXa_outXknowsX_name_bX_identity
    Given the modern graph
    And the traversal of
      """
      g.V().match(__.as("a").out("knows").values("name").as("b")).identity()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a":"v[marko]","b":"vadas"}] |
      | m[{"a":"v[marko]","b":"josh"}] |