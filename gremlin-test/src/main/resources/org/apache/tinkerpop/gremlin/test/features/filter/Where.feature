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

@StepClassFilter @StepWhere
Feature: Step - where()

  Scenario: g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_selectXa_bX_whereXa_eqXbXX
    Given the modern graph
    And the traversal of
      """
      g.V().has("age").as("a").out().in().has("age").as("b").select("a", "b").where("a", P.eq("b"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a":"v[marko]","b":"v[marko]"}] |
      | m[{"a":"v[marko]","b":"v[marko]"}] |
      | m[{"a":"v[marko]","b":"v[marko]"}] |
      | m[{"a":"v[josh]","b":"v[josh]"}] |
      | m[{"a":"v[josh]","b":"v[josh]"}] |
      | m[{"a":"v[peter]","b":"v[peter]"}] |

  Scenario: g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_selectXa_bX_whereXa_neqXbXX
    Given the modern graph
    And the traversal of
      """
      g.V().has("age").as("a").out().in().has("age").as("b").select("a", "b").where("a", P.neq("b"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a":"v[marko]","b":"v[josh]"}] |
      | m[{"a":"v[marko]","b":"v[peter]"}] |
      | m[{"a":"v[josh]","b":"v[marko]"}] |
      | m[{"a":"v[josh]","b":"v[peter]"}] |
      | m[{"a":"v[peter]","b":"v[marko]"}] |
      | m[{"a":"v[peter]","b":"v[josh]"}] |

  Scenario: g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_selectXa_bX_whereXb_hasXname_markoXX
    Given the modern graph
    And the traversal of
      """
      g.V().has("age").as("a").out().in().has("age").as("b").select("a", "b").where(__.as("b").has("name", "marko"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a":"v[marko]","b":"v[marko]"}] |
      | m[{"a":"v[marko]","b":"v[marko]"}] |
      | m[{"a":"v[marko]","b":"v[marko]"}] |
      | m[{"a":"v[josh]","b":"v[marko]"}] |
      | m[{"a":"v[peter]","b":"v[marko]"}] |

  Scenario: g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_selectXa_bX_whereXa_outXknowsX_bX
    Given the modern graph
    And the traversal of
      """
      g.V().has("age").as("a").out().in().has("age").as("b").select("a", "b").where(__.as("a").out("knows").as("b"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a":"v[marko]","b":"v[josh]"}] |

  Scenario: g_V_asXaX_outXcreatedX_whereXasXaX_name_isXjoshXX_inXcreatedX_name
    Given the modern graph
    And the traversal of
      """
      g.V().as("a").out("created").where(__.as("a").values("name").is("josh")).in("created").values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | josh |
      | josh |
      | marko |
      | peter |

  Scenario: g_withSideEffectXa_josh_peterX_VX1X_outXcreatedX_inXcreatedX_name_whereXwithinXaXX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.withSideEffect("a", ["josh","peter"]).V(vid1).out("created").in("created").values("name").where(P.within("a"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | josh |
      | peter |

  Scenario: g_VX1X_asXaX_outXcreatedX_inXcreatedX_asXbX_whereXa_neqXbXX_name
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).as("a").out("created").in("created").as("b").where("a", P.neq("b")).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | josh |
      | peter |

  @GraphComputerVerificationStarGraphExceeded
  Scenario: g_VX1X_asXaX_outXcreatedX_inXcreatedX_asXbX_whereXasXbX_outXcreatedX_hasXname_rippleXX_valuesXage_nameX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).as("a").out("created").in("created").as("b").where(__.as("b").out("created").has("name", "ripple")).values("age", "name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | josh |
      | d[32].i |

  Scenario: g_VX1X_asXaX_outXcreatedX_inXcreatedX_whereXeqXaXX_name
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).as("a").out("created").in("created").where(P.eq("a")).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko |

  Scenario: g_VX1X_asXaX_outXcreatedX_inXcreatedX_whereXneqXaXX_name
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).as("a").out("created").in("created").where(P.neq("a")).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | peter |
      | josh  |

  Scenario: g_VX1X_out_aggregateXxX_out_whereXnotXwithinXaXXX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).out().aggregate("x").out().where(P.not(P.within("x")))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[ripple] |

  Scenario: g_withSideEffectXa_g_VX2XX_VX1X_out_whereXneqXaXX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And using the parameter v2 defined as "v[vadas]"
    And the traversal of
      """
      g.withSideEffect("a", v2).V(vid1).out().where(P.neq("a"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[josh] |
      | v[lop] |

  Scenario: g_VX1X_repeatXbothEXcreatedX_whereXwithoutXeXX_aggregateXeX_otherVX_emit_path
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).repeat(__.bothE("created").where(P.without("e")).aggregate("e").otherV()).emit().path()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | p[v[marko],e[marko-created->lop],v[lop]] |
      | p[v[marko],e[marko-created->lop],v[lop],e[josh-created->lop],v[josh]] |
      | p[v[marko],e[marko-created->lop],v[lop],e[peter-created->lop],v[peter]] |
      | p[v[marko],e[marko-created->lop],v[lop],e[josh-created->lop],v[josh],e[josh-created->ripple],v[ripple]] |

  Scenario: g_V_whereXnotXoutXcreatedXXX_name
    Given the modern graph
    And the traversal of
      """
      g.V().where(__.not(__.out("created"))).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | vadas |
      | lop |
      | ripple |

  @GraphComputerVerificationReferenceOnly
  Scenario: g_V_asXaX_out_asXbX_whereXandXasXaX_outXknowsX_asXbX__orXasXbX_outXcreatedX_hasXname_rippleX__asXbX_inXknowsX_count_isXnotXeqX0XXXXX_selectXa_bX
    Given the modern graph
    And the traversal of
      """
      g.V().as("a").out().as("b").where(__.and(__.as("a").out("knows").as("b"), __.or(__.as("b").out("created").has("name", "ripple"), __.as("b").in("knows").count().is(P.not(P.eq(0)))))).select("a", "b")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a": "v[marko]", "b": "v[vadas]"}] |
      | m[{"a": "v[marko]", "b": "v[josh]"}] |

  Scenario: g_V_whereXoutXcreatedX_and_outXknowsX_or_inXknowsXX_valuesXnameX
    Given the modern graph
    And the traversal of
      """
      g.V().where(__.out("created").and().out("knows").or().in("knows")).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko |
      | vadas |
      | josh |

  @GraphComputerVerificationReferenceOnly
  Scenario: g_V_asXaX_outXcreatedX_asXbX_whereXandXasXbX_in__notXasXaX_outXcreatedX_hasXname_rippleXXX_selectXa_bX
    Given the modern graph
    And the traversal of
      """
      g.V().as("a").out("created").as("b").where(__.and(__.as("b").in(), __.not(__.as("a").out("created").has("name", "ripple")))).select("a", "b")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a": "v[marko]", "b": "v[lop]"}] |
      | m[{"a": "v[peter]", "b": "v[lop]"}] |

  Scenario: g_V_asXaX_outXcreatedX_asXbX_inXcreatedX_asXcX_bothXknowsX_bothXknowsX_asXdX_whereXc__notXeqXaX_orXeqXdXXXX_selectXa_b_c_dX
    Given the modern graph
    And the traversal of
      """
      g.V().as("a").out("created").as("b").in("created").as("c").both("knows").both("knows").as("d").where("c", P.not(P.eq("a").or(P.eq("d")))).select("a", "b", "c", "d")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a": "v[marko]", "b": "v[lop]", "c": "v[josh]", "d": "v[vadas]"}] |
      | m[{"a": "v[peter]", "b": "v[lop]", "c": "v[josh]", "d": "v[vadas]"}] |

  @GraphComputerVerificationReferenceOnly
  Scenario: g_V_asXaX_out_asXbX_whereXin_count_isXeqX3XX_or_whereXoutXcreatedX_and_hasXlabel_personXXX_selectXa_bX
    Given the modern graph
    And the traversal of
      """
      g.V().as("a").out().as("b").where(__.as("b").in().count().is(P.eq(3)).or().where(__.as("b").out("created").and().as("b").has(T.label, "person"))).select("a", "b")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a": "v[marko]", "b": "v[lop]"}] |
      | m[{"a": "v[marko]", "b": "v[josh]"}] |
      | m[{"a": "v[josh]", "b": "v[lop]"}] |
      | m[{"a": "v[peter]", "b": "v[lop]"}] |

  @GraphComputerVerificationReferenceOnly
  Scenario: g_V_asXaX_outXcreatedX_inXcreatedX_asXbX_whereXa_gtXbXX_byXageX_selectXa_bX_byXnameX
    Given the modern graph
    And the traversal of
      """
      g.V().as("a").out("created").in("created").as("b").where("a", P.gt("b")).by("age").select("a", "b").by("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a": "josh", "b": "marko"}] |
      | m[{"a": "peter", "b": "marko"}] |
      | m[{"a": "peter", "b": "josh"}] |

  @GraphComputerVerificationReferenceOnly
  Scenario: g_V_asXaX_outEXcreatedX_asXbX_inV_asXcX_whereXa_gtXbX_orXeqXbXXX_byXageX_byXweightX_byXweightX_selectXa_cX_byXnameX
    Given the modern graph
    And the traversal of
      """
      g.V().as("a").outE("created").as("b").inV().as("c").where("a", P.gt("b").or(P.eq("b"))).by("age").by("weight").by("weight").select("a", "c").by("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a": "marko", "c": "lop"}] |
      | m[{"a": "josh", "c": "ripple"}] |
      | m[{"a": "josh", "c": "lop"}] |
      | m[{"a": "peter", "c": "lop"}] |

  @GraphComputerVerificationReferenceOnly
  Scenario: g_V_asXaX_outEXcreatedX_asXbX_inV_asXcX_inXcreatedX_asXdX_whereXa_ltXbX_orXgtXcXX_andXneqXdXXX_byXageX_byXweightX_byXinXcreatedX_valuesXageX_minX_selectXa_c_dX
    Given the modern graph
    And the traversal of
      """
      g.V().as("a").outE("created").as("b").inV().as("c").in("created").as("d").where("a", P.lt("b").or(P.gt("c")).and(P.neq("d"))).by("age").by("weight").by(__.in("created").values("age").min()).select("a", "c", "d").by("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a": "josh", "c": "lop", "d": "marko"}] |
      | m[{"a": "josh", "c": "lop", "d": "peter"}] |
      | m[{"a": "peter", "c": "lop", "d": "marko"}] |
      | m[{"a": "peter", "c": "lop", "d": "josh"}] |

  @GraphComputerVerificationReferenceOnly
  Scenario: g_VX1X_asXaX_out_hasXageX_whereXgtXaXX_byXageX_name
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).as("a").out().has("age").where(P.gt("a")).by("age").values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | josh |

  @GraphComputerVerificationReferenceOnly
  Scenario: g_VX3X_asXaX_in_out_asXbX_whereXa_eqXbXX_byXageX_name
    Given the modern graph
    And using the parameter vid3 defined as "v[lop].id"
    And the traversal of
      """
      g.V(vid3).as("a").in().out().as("b").where("a", P.eq("b")).by("age").values("name")
      """
    When iterated to list
    Then the result should be empty

  # comparison of null "age" values in where()
  @GraphComputerVerificationReferenceOnly @WithProductiveByStrategy
  Scenario: g_withStrategiesXProductiveByStrategyX_VX3X_asXaX_in_out_asXbX_whereXa_eqXbXX_byXageX_name
    Given the modern graph
    And using the parameter vid3 defined as "v[lop].id"
    And the traversal of
      """
      g.withStrategies(ProductiveByStrategy).V(vid3).as("a").in().out().as("b").where("a", P.eq("b")).by("age").values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | lop |
      | lop |
      | lop |
      | ripple |

  Scenario: g_V_asXnX_whereXorXhasLabelXsoftwareX_hasLabelXpersonXXX_selectXnX_byXnameX
    Given the modern graph
    And the traversal of
      """
      g.V().as("n").where(
        __.or(__.hasLabel("software"), __.hasLabel("person"))
      ).select("n").by("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko |
      | vadas |
      | josh |
      | peter |
      | lop |
      | ripple |

  @GraphComputerVerificationStarGraphExceeded
  Scenario: g_V_asXnX_whereXorXselectXnX_hasLabelXsoftwareX_selectXnX_hasLabelXpersonXXX_selectXnX_byXnameX
    Given the modern graph
    And the traversal of
      """
      g.V().as("n").
        where(__.or(__.select("n").hasLabel("software"), __.select("n").hasLabel("person"))).
        select("n").by("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko |
      | vadas |
      | josh |
      | peter |
      | lop |
      | ripple |

  Scenario: g_V_hasLabelXpersonX_asXxX_whereXinEXknowsX_count_isXgteX1XXX_selectXxX
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("person").as("x").where(__.inE("knows").count().is(P.gte(1))).select("x")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[vadas] |
      | v[josh] |