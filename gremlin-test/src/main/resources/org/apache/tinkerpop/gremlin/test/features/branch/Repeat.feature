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

@StepClassBranch @StepRepeat
Feature: Step - repeat()

  Scenario: g_V_repeatXoutX_timesX2X_emit_path
    Given the modern graph
    And the traversal of
      """
      g.V().repeat(__.out()).times(2).emit().path()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | p[v[marko],v[lop]] |
      | p[v[marko],v[vadas]] |
      | p[v[marko],v[josh]] |
      | p[v[marko],v[josh],v[ripple]] |
      | p[v[marko],v[josh],v[lop]] |
      | p[v[josh],v[ripple]] |
      | p[v[josh],v[lop]] |
      | p[v[peter],v[lop]] |

  Scenario: g_V_repeatXoutX_timesX2X_repeatXinX_timesX2X_name
    Given the modern graph
    And the traversal of
      """
      g.V().repeat(__.out()).times(2).repeat(__.in()).times(2).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko |
      | marko |

  @GraphComputerVerificationReferenceOnly
  Scenario: g_V_repeatXoutE_inVX_timesX2X_path_by_name_by_label
    Given the modern graph
    And the traversal of
      """
      g.V().repeat(__.outE().inV()).times(2).path().by("name").by(T.label)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | p[marko,knows,josh,created,lop] |
      | p[marko,knows,josh,created,ripple] |

  Scenario: g_V_repeatXoutX_timesX2X
    Given the modern graph
    And the traversal of
      """
      g.V().repeat(__.out()).times(2) 
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[ripple] |
      | v[lop] |

  Scenario: g_V_repeatXoutX_timesX2X_emit
    Given the modern graph
    And the traversal of
      """
      g.V().repeat(__.out()).times(2).emit()
      """
    When iterated to list
    Then the result should be of
      | result |
      | v[ripple] |
      | v[lop] |
      | v[josh] |
      | v[vadas] |
    And the result should have a count of 8

  Scenario: g_VX1X_timesX2X_repeatXoutX_name
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).times(2).repeat(__.out()).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | ripple |
      | lop |

  Scenario: g_V_emit_timesX2X_repeatXoutX_path
    Given the modern graph
    And the traversal of
      """
      g.V().emit().times(2).repeat(__.out()).path()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | p[v[marko]] |
      | p[v[marko],v[lop]] |
      | p[v[marko],v[vadas]] |
      | p[v[marko],v[josh]] |
      | p[v[marko],v[josh],v[ripple]] |
      | p[v[marko],v[josh],v[lop]] |
      | p[v[vadas]] |
      | p[v[lop]] |
      | p[v[josh]] |
      | p[v[josh],v[ripple]] |
      | p[v[josh],v[lop]] |
      | p[v[ripple]] |
      | p[v[peter]] |
      | p[v[peter],v[lop]] |

  Scenario: g_V_emit_repeatXoutX_timesX2X_path
    Given the modern graph
    And the traversal of
      """
      g.V().emit().repeat(__.out()).times(2).path()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | p[v[marko]] |
      | p[v[marko],v[lop]] |
      | p[v[marko],v[vadas]] |
      | p[v[marko],v[josh]] |
      | p[v[marko],v[josh],v[ripple]] |
      | p[v[marko],v[josh],v[lop]] |
      | p[v[vadas]] |
      | p[v[lop]] |
      | p[v[josh]] |
      | p[v[josh],v[ripple]] |
      | p[v[josh],v[lop]] |
      | p[v[ripple]] |
      | p[v[peter]] |
      | p[v[peter],v[lop]] |

  Scenario: g_VX1X_emitXhasXlabel_personXX_repeatXoutX_name
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).emit(__.has(T.label, "person")).repeat(__.out()).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko |
      | vadas |
      | josh  |

  Scenario: g_V_repeatXgroupCountXmX_byXnameX_outX_timesX2X_capXmX
    Given the modern graph
    And the traversal of
      """
      g.V().repeat(__.groupCount("m").by("name").out()).times(2).cap("m")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"ripple":"d[2].l","peter":"d[1].l","vadas":"d[2].l","josh":"d[2].l","lop":"d[4].l","marko":"d[1].l"}] |

  Scenario: g_VX1X_repeatXgroupCountXmX_byXloopsX_outX_timesX3X_capXmX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).repeat(__.groupCount("m").by(__.loops()).out()).times(3).cap("m")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"d[0].i":"d[1].l","d[1].i":"d[3].l","d[2].i":"d[2].l"}] |

  Scenario: g_V_repeatXbothX_timesX10X_asXaX_out_asXbX_selectXa_bX
    Given the modern graph
    And the traversal of
      """
      g.V().repeat(__.both()).times(10).as("a").out().as("b").select("a", "b").count()
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | d[43958].l |

  Scenario: g_VX1X_repeatXoutX_untilXoutE_count_isX0XX_name
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).repeat(__.out()).until(__.outE().count().is(0)).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | lop |
      | vadas |
      | ripple  |
      | lop  |

  @GraphComputerVerificationReferenceOnly
  Scenario: g_V_hasXname_markoX_repeatXoutE_inV_simplePathX_untilXhasXname_rippleXX_path_byXnameX_byXlabelX
    Given the modern graph
    And the traversal of
      """
      g.V().has("name", "marko").repeat(__.outE().inV().simplePath()).until(__.has("name", "ripple")).path().by("name").by(T.label)
      """
    When iterated next
    Then the result should be unordered
      | result |
      | marko |
      | knows |
      | josh  |
      | created |
      | ripple  |

  @GraphComputerVerificationReferenceOnly
  Scenario: g_V_hasXloop_name_loopX_repeatXinX_timesX5X_path_by_name
    Given the sink graph
    And the traversal of
      """
      g.V().has("loops","name","loop").repeat(__.in()).times(5).path().by("name")
      """
    When iterated next
    Then the result should be unordered
      | result |
      | loop |
      | loop |
      | loop  |
      | loop |
      | loop  |
      | loop  |

  @GraphComputerVerificationReferenceOnly
  Scenario: g_V_repeatXout_repeatXout_order_byXname_descXX_timesX1XX_timesX1X_limitX1X_path_byXnameX
      Given the modern graph
      And the traversal of
          """
          g.V().repeat(__.out().repeat(__.out().order().by("name",desc)).times(1)).times(1).limit(1).path().by("name")
          """
      When iterated next
      Then the result should be unordered
          | result |
          | marko |
          | josh |
          | ripple |

  @GraphComputerVerificationReferenceOnly
  Scenario: g_V_repeatXoutXknowsXX_untilXrepeatXoutXcreatedXX_emitXhasXname_lopXXX_path_byXnameX
    Given the modern graph
    And the traversal of
      """
      g.V().repeat(__.out("knows")).until(__.repeat(__.out("created")).emit(__.has("name", "lop"))).path().by("name")
      """
    When iterated next
    Then the result should be unordered
      | result |
      | marko |
      | josh |

  Scenario: g_V_repeatXrepeatXout_createdXX_untilXhasXname_rippleXXXemit_lang
    Given the modern graph
    And the traversal of
      """
      g.V().repeat(__.repeat(__.out("created")).until(__.has("name", "ripple"))).emit().values("lang")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | java |

  Scenario: g_V_untilXconstantXtrueXX_repeatXrepeatXout_createdXX_untilXhasXname_rippleXXXemit_lang
    Given the modern graph
    And the traversal of
      """
      g.V().until(__.constant(true)).repeat(__.repeat(__.out("created")).until(__.has("name", "ripple"))).emit().values("lang")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | java |
      | java |

  Scenario: g_V_emit_repeatXa_outXknows_filterXloops_isX0XX_lang
    Given the modern graph
    And the traversal of
      """
      g.V().emit().repeat("a", __.out("knows").filter(__.loops("a").is(0))).values("lang")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | java |
      | java |

  @GraphComputerVerificationReferenceOnly
  Scenario: g_VX3X_repeatXbothX_createdXX_untilXloops_is_40XXemit_repeatXin_knowsXX_emit_loopsXisX1Xdedup_values
      Given the modern graph
      And using the parameter vid3 defined as "v[lop].id"
      And the traversal of
        """
        g.V(vid3).repeat(__.both("created")).until(__.loops().is(40)).emit(__.repeat(__.in("knows")).emit(__.loops().is(1))).dedup().values("name")
        """
      When iterated to list
        Then the result should be unordered
          | result |
          | josh |
          | lop |
          | ripple |

  @MultiProperties @MetaProperties
  Scenario: g_VX1X_repeatXrepeatXunionXout_uses_out_traversesXX_whereXloops_isX0X_timesX1X_timeX2X_name
      Given the crew graph
      And using the parameter vid1 defined as "v[marko].id"
      And the traversal of
        """
        g.V(vid1).repeat(__.repeat(__.union(__.out("uses"), __.out("traverses")).where(__.loops().is(0))).times(1)).times(2).values("name")
        """
      When iterated to list
        Then the result should be unordered
          | result |
          | tinkergraph |

  Scenario: g_V_repeatXa_outXknows_repeatXb_outXcreatedX_filterXloops_isX0XX_emit_lang
    Given the modern graph
    And the traversal of
      """
      g.V().repeat("a", __.out("knows").repeat("b", __.out("created").filter(__.loops("a").is(0))).emit()).emit().values("lang")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | java |
      | java |

  @GraphComputerVerificationReferenceOnly
  Scenario: g_VX6X_repeatXa_bothXcreatedX_simplePathX_emitXrepeatXb_bothXknowsXX_untilXloopsXbX_asXb_whereXloopsXaX_asXbX_hasXname_vadasXX_dedup_name
    Given the modern graph
    And using the parameter vid6 defined as "v[peter].id"
    And the traversal of
      """
      g.V(vid6).repeat("a", __.both("created").simplePath()).emit(__.repeat("b", __.both("knows")).until(__.loops("b").as("b").where(__.loops("a").as("b"))).has("name", "vadas")).dedup().values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | josh |

  Scenario: g_V_emit
    Given the modern graph
    And the traversal of
      """
      g.V().emit()
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "The repeat()-traversal was not defined"

  Scenario: g_V_untilXidentityX
    Given the modern graph
    And the traversal of
      """
      g.V().until(__.identity())
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "The repeat()-traversal was not defined"

  Scenario: g_V_timesX5X
    Given the modern graph
    And the traversal of
      """
      g.V().times(5)
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "The repeat()-traversal was not defined"

  Scenario: g_V_hasXperson_name_markoX_repeatXoutXcreatedXX_timesX1X_name
    Given the modern graph
    And the traversal of
      """
      g.V().has("person","name","marko").repeat(__.out("created")).times(1).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | lop |

  Scenario: g_V_hasXperson_name_markoX_repeatXoutXcreatedXX_timesX0X_name
    Given the modern graph
    And the traversal of
      """
      g.V().has("person","name","marko").repeat(out("created")).times(0).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | lop |

  Scenario: g_V_hasXperson_name_markoX_timesX1X_repeatXoutXcreatedXX_name
    Given the modern graph
    And the traversal of
      """
      g.V().has("person","name","marko").times(1).repeat(out("created")).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | lop |

  Scenario: g_V_hasXperson_name_markoX_timesX0X_repeatXoutXcreatedXX_name
    Given the modern graph
    And the traversal of
      """
      g.V().has("person","name","marko").times(0).repeat(out("created")).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko |

  # Proper handling of empty results if repeat doesn't output traversers
  Scenario: g_V_repeatXboth_hasXnot_productiveXX_timesX3X_constantX1X
    Given the modern graph
    And the traversal of
      """
      g.V().repeat(__.both().has("not", "productive")).times(3).constant(1)
      """
    When iterated to list
    Then the result should be empty

  # Proper handling of empty results if no traverser goes into repeat
  Scenario: g_V_hasXnot_productiveX_repeatXbothX_timesX3X_constantX1X
    Given the modern graph
    And the traversal of
      """
      g.V().has("not", "productive").repeat(__.both()).times(3).constant(1)
      """
    When iterated to list
    Then the result should be empty

  # Each iteration of repeat traversal that ends in a barrier leads to BFS style processing
  @InsertionOrderingRequired
  Scenario: g_VX1_2_3X_repeatXboth_barrierX_emit_timesX2X_path
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And using the parameter vid2 defined as "v[vadas].id"
    And using the parameter vid3 defined as "v[lop].id"
    And the traversal of
      """
      g.V(vid1, vid2, vid3).repeat(__.both().barrier()).emit().times(2).path()
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | p[v[marko],v[lop]] |
      | p[v[marko],v[vadas]] |
      | p[v[marko],v[josh]] |
      | p[v[vadas],v[marko]] |
      | p[v[lop],v[marko]] |
      | p[v[lop],v[josh]] |
      | p[v[lop],v[peter]] |
      | p[v[marko],v[lop],v[marko]] |
      | p[v[marko],v[lop],v[josh]] |
      | p[v[marko],v[lop],v[peter]] |
      | p[v[marko],v[vadas],v[marko]] |
      | p[v[marko],v[josh],v[ripple]] |
      | p[v[marko],v[josh],v[lop]] |
      | p[v[marko],v[josh],v[marko]] |
      | p[v[vadas],v[marko],v[lop]] |
      | p[v[vadas],v[marko],v[vadas]] |
      | p[v[vadas],v[marko],v[josh]] |
      | p[v[lop],v[marko],v[lop]] |
      | p[v[lop],v[marko],v[vadas]] |
      | p[v[lop],v[marko],v[josh]] |
      | p[v[lop],v[josh],v[ripple]] |
      | p[v[lop],v[josh],v[lop]] |
      | p[v[lop],v[josh],v[marko]] |
      | p[v[lop],v[peter],v[lop]] |

  # Global children should be ordered by last loop first
  Scenario: g_V_order_byXname_descX_repeatXboth_simplePath_order_byXname_descXX_timesX2X_path
    Given the modern graph
    And the traversal of
      """
      g.V().order().by("name", Order.desc).repeat(__.both().simplePath().order().by("name", Order.desc)).times(2).path()
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | p[v[lop],v[marko],v[vadas]] |
      | p[v[josh],v[marko],v[vadas]] |
      | p[v[marko],v[josh],v[ripple]] |
      | p[v[lop],v[josh],v[ripple]] |
      | p[v[marko],v[lop],v[peter]] |
      | p[v[josh],v[lop],v[peter]] |
      | p[v[peter],v[lop],v[marko]] |
      | p[v[josh],v[lop],v[marko]] |
      | p[v[ripple],v[josh],v[marko]] |
      | p[v[lop],v[josh],v[marko]] |
      | p[v[vadas],v[marko],v[lop]] |
      | p[v[josh],v[marko],v[lop]] |
      | p[v[ripple],v[josh],v[lop]] |
      | p[v[marko],v[josh],v[lop]] |
      | p[v[vadas],v[marko],v[josh]] |
      | p[v[lop],v[marko],v[josh]] |
      | p[v[peter],v[lop],v[josh]] |
      | p[v[marko],v[lop],v[josh]] |

  # Nested repeat should maintain globalness
  Scenario: g_V_repeatXboth_repeatXorder_byXnameXX_timesX1XX_timesX1X
    Given the modern graph
    And the traversal of
      """
      g.V().repeat(__.both().repeat(__.order().by("name")).times(1)).times(1)
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | v[josh] |
      | v[josh] |
      | v[josh] |
      | v[lop] |
      | v[lop] |
      | v[lop] |
      | v[marko] |
      | v[marko] |
      | v[marko] |
      | v[peter] |
      | v[ripple] |
      | v[vadas] |

  # Nested local inside repeat should prevent global children from parent repeat
  @GraphComputerVerificationStarGraphExceeded
  Scenario: g_V_order_byXname_descX_repeatXlocalXout_order_byXnameXXX_timesX1X
    Given the modern graph
    And the traversal of
      """
      g.V().order().by("name", Order.desc).repeat(__.local(__.out().order().by("name"))).times(1)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[lop] |
      | v[josh] |
      | v[lop] |
      | v[vadas] |
      | v[lop] |
      | v[ripple] |

  # Local child traversal should be applied per loop
  @GraphComputerVerificationStarGraphExceeded
  Scenario: g_V_order_byXnameX_repeatXlocalXboth_simplePath_order_byXnameXXX_timesX2X_path
    Given the modern graph
    And the traversal of
      """
      g.V().order().by("name").repeat(__.local(__.both().simplePath().order().by("name"))).times(2).path()
      """
    When iterated to list
    Then the result should be ordered
    | result |
    | p[v[josh],v[lop],v[marko]] |
    | p[v[josh],v[lop],v[peter]] |
    | p[v[josh],v[marko],v[lop]] |
    | p[v[josh],v[marko],v[vadas]] |
    | p[v[lop],v[josh],v[marko]] |
    | p[v[lop],v[josh],v[ripple]] |
    | p[v[lop],v[marko],v[josh]] |
    | p[v[lop],v[marko],v[vadas]] |
    | p[v[marko],v[josh],v[lop]] |
    | p[v[marko],v[josh],v[ripple]] |
    | p[v[marko],v[lop],v[josh]] |
    | p[v[marko],v[lop],v[peter]] |
    | p[v[peter],v[lop],v[josh]] |
    | p[v[peter],v[lop],v[marko]] |
    | p[v[ripple],v[josh],v[lop]] |
    | p[v[ripple],v[josh],v[marko]] |
    | p[v[vadas],v[marko],v[josh]] |
    | p[v[vadas],v[marko],v[lop]] |

  # Branching step with global children should remain global in repeat traversal
  # Results are unordered due to server bulking of traversers which leads to incorrect result
  Scenario: g_V_repeatXunionXoutXknowsX_order_byXnameX_inXcreatedX_order_byXnameXXX_timesX1X
    Given the modern graph
    And the traversal of
      """
      g.V().repeat(__.union(__.out("knows").order().by("name"), __.in("created").order().by("name"))).times(1)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[josh] |
      | v[vadas] |
      | v[josh] |
      | v[josh] |
      | v[marko] |
      | v[peter] |

  # addV inside repeat should create the proper number of vertices
  Scenario: g_V_repeatXaddV_propertyXgenerated_trueXX_timesX2X
    Given the empty graph
    And the graph initializer of
      """
      g.addV().property("notGenerated", "true").addV().property("notGenerated", "true")
      """
    And the traversal of
      """
      g.V().repeat(__.addV().property("generated", "true")).times(2)
      """
    When iterated to list
    Then the result should have a count of 2
    And the graph should return 2 for count of "g.V().has(\"notGenerated\")"
    And the graph should return 4 for count of "g.V().has(\"generated\")"

  # global dedup should remove all traversers if looped more than once with all possible vertices
  Scenario: g_V_repeatXdedup_bothX_timesX2X
    Given the modern graph
    And the traversal of
      """
      g.V().repeat(__.dedup().both()).times(2)
      """
    When iterated to list
    Then the result should be empty

  # global, eager aggregate should contain all results even in the first select
  Scenario: g_V_repeatXaggregateXxXX_timesX2X_selectXxX_limitX1X_unfold
    Given the modern graph
    And the traversal of
      """
      g.V().repeat(__.aggregate("x")).times(2).select("x").limit(1).unfold()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |
      | v[marko] |
      | v[josh] |
      | v[josh] |
      | v[peter] |
      | v[peter] |
      | v[ripple] |
      | v[ripple] |
      | v[lop] |
      | v[lop] |
      | v[vadas] |
      | v[vadas] |

  Scenario: g_V_valuesXstrX_repeatXsplitXabcX_conjoinX_timesX2X
    Given the empty graph
    And the graph initializer of
      """
      g.addV().property("str", "ababcczababcc").addV().property("str", "abcyabc")
      """
    And the traversal of
      """
      g.V().values("str").repeat(__.split("abc").conjoin("")).times(2)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | z |
      | y |

  Scenario: g_withSackX0X_V_repeatXsackXsumX_byXageX_whereXsack_isXltX59XXXX_timesX2X
    Given the modern graph
    And the traversal of
      """
      g.withSack(0L).V().repeat(__.sack(Operator.sum).by("age").where(__.sack().is(P.lt(59)))).times(2)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |
      | v[vadas] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_V_repeatXinjectXyXX_timesX2X
    Given the modern graph
    And the traversal of
      """
      g.V().repeat(inject('y')).times(2)
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "The parent of inject()-step can not be repeat()-step"
    
  Scenario: g_V_repeatXunionXconstantXyX_limitX1X_identityXX_timesX3X
    Given the modern graph
    And the traversal of
      """
      g.V().repeat(union(constant('y').limit(1),identity())).times(2)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | y |
      | y |
      | v[marko] |
      | v[vadas] |
      | v[lop] |
      | v[josh] |
      | v[ripple] |
      | v[peter] |

  # Test tail works in repeat with a single loop
  Scenario: g_VX3X_repeatXout_order_byXperformancesX_tailX2XX_timesX1X_valuesXnameX
    Given the grateful graph
    And using the parameter vid3 defined as "v[NOT FADE AWAY].id"
    And the traversal of
      """
      g.V(vid3).repeat(__.out().order().by("performances").tail(2)).times(1).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | ME AND MY UNCLE |
      | DRUMS |

  # Test tail runs per iteration in repeat with multiple iterations
  Scenario: g_VX3X_repeatXout_order_byXperformancesX_tailX2XX_timesX2X_valuesXnameX
    Given the grateful graph
    And using the parameter vid3 defined as "v[NOT FADE AWAY].id"
    And the traversal of
      """
      g.V(vid3).repeat(__.out().order().by("performances").tail(2)).times(2).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | THE OTHER ONE |
      | SUGAR MAGNOLIA |

  # Test object-local tail works in repeat with a single loop
  Scenario: g_VX2X_repeatXout_localXorder_byXperformancesX_tailX1XXX_timesX1X_valuesXnameX
    Given the grateful graph
    And using the parameter vid2 defined as "v[IM A MAN].id"
    And the traversal of
      """
      g.V(vid2).repeat(__.out().local(__.order().by("performances").tail(1))).times(1).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | JAM |
      | JACK STRAW |

  # Test object-local tail runs per iteration in repeat with multiple iterations
  Scenario: g_VX250X_repeatXout_localXorder_byXperformancesX_tailX1XXX_timesX2X_valuesXnameX
    Given the grateful graph
    And using the parameter vid250 defined as "v[SIMPLE TWIST OF FATE].id"
    And the traversal of
      """
      g.V(vid250).repeat(__.out().local(__.order().by("performances").tail(1))).times(2).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | STUCK INSIDE OF MOBILE |
      | STUCK INSIDE OF MOBILE |
      | WICKED MESSENGER |
      | TANGLED UP IN BLUE |
      | SHELTER FROM THE STORM |
      | RAINY DAY WOMAN |
      | CUMBERLAND BLUES |
      | WHEN PUSH COMES TO SHOVE |
      | JOHN BROWN |
      | SIMPLE TWIST OF FATE |
      | BABY BLUE |

  # Test tail inside repeat can be followed by other range-based steps
  Scenario: g_VX3X_repeatXout_order_byXperformancesX_tailX3X_limitX1XX_timesX2X_valuesXnameX
    Given the grateful graph
    And using the parameter vid3 defined as "v[NOT FADE AWAY].id"
    And the traversal of
      """
      g.V(vid3).repeat(__.out().order().by("performances").tail(3).limit(1)).times(2).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | THE OTHER ONE |

  # Test tail inside repeat can be preceded by other range-based steps
  Scenario: g_VX3X_repeatXout_order_byXperformances_descX_limitX5X_tailX1XX_timesX2X_valuesXnameX
    Given the grateful graph
    And using the parameter vid3 defined as "v[NOT FADE AWAY].id"
    And the traversal of
      """
      g.V(vid3).repeat(__.out().order().by("performances", Order.desc).limit(5).tail(1)).times(2).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | CHINA CAT SUNFLOWER |

  # Test order on edge weight with tail in repeat leads to ordered walk
  Scenario: g_VX3X_repeatXoutE_order_byXweightX_tailX2X_inVX_timesX2X_valuesXnameX
    Given the grateful graph
    And using the parameter vid3 defined as "v[NOT FADE AWAY].id"
    And the traversal of
      """
      g.V(vid3).repeat(__.outE().order().by("weight").tail(2).inV()).times(2).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | SUGAR MAGNOLIA |
      | AROUND AND AROUND |

  # Test order on edge weight with limit in repeat leads to limited, ordered walk
  Scenario: g_VX3X_repeatXoutE_order_byXweight_descX_limitX2X_inVX_timesX2X_valuesXnameX
    Given the grateful graph
    And using the parameter vid3 defined as "v[NOT FADE AWAY].id"
    And the traversal of
      """
      g.V(vid3).repeat(__.outE().order().by("weight", Order.desc).limit(2).inV()).times(2).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | SUGAR MAGNOLIA |
      | AROUND AND AROUND |
