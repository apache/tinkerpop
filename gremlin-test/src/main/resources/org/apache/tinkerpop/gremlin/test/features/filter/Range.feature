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

@StepClassFilter @StepRange
Feature: Step - range()

  Scenario: g_VX1X_out_limitX2X
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).out().limit(2)
      """
    When iterated to list
    Then the result should be of
      | result |
      | v[josh] |
      | v[vadas] |
      | v[lop] |
    And the result should have a count of 2

  Scenario: g_VX1X_out_limitX2varX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And using the parameter xx1 defined as "d[2].i"
    And the traversal of
      """
      g.V(vid1).out().limit(xx1)
      """
    When iterated to list
    Then the result should be of
      | result |
      | v[josh] |
      | v[vadas] |
      | v[lop] |
    And the result should have a count of 2

  Scenario: g_V_localXoutE_limitX1X_inVX_limitX3X
    Given the modern graph
    And the traversal of
      """
      g.V().local(__.outE().limit(1)).inV().limit(3)
      """
    When iterated to list
    Then the result should be of
      | result |
      | v[josh] |
      | v[vadas] |
      | v[lop] |
      | v[ripple] |
    And the result should have a count of 3

  Scenario: g_VX1X_outXknowsX_outEXcreatedX_rangeX0_1X_inV
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).out("knows").outE("created").range(0, 1).inV()
      """
    When iterated to list
    Then the result should be of
      | result |
      | v[lop] |
      | v[ripple] |
    And the result should have a count of 1

  Scenario: g_VX1X_outXknowsX_outXcreatedX_rangeX0_1X
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).out("knows").out("created").range(0, 1)
      """
    When iterated to list
    Then the result should be of
      | result |
      | v[lop] |
      | v[ripple] |
    And the result should have a count of 1

  Scenario: g_VX1X_outXcreatedX_inXcreatedX_rangeX1_3X
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).out("created").in("created").range(1, 3)
      """
    When iterated to list
    Then the result should be of
      | result |
      | v[marko] |
      | v[josh] |
      | v[peter] |
    And the result should have a count of 2

  Scenario: g_VX1X_outXcreatedX_inXcreatedX_rangeX1var_3varX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And using the parameter xx1 defined as "d[1].i"
    And using the parameter xx2 defined as "d[3].i"
    And the traversal of
      """
      g.V(vid1).out("created").in("created").range(xx1, xx2)
      """
    When iterated to list
    Then the result should be of
      | result |
      | v[marko] |
      | v[josh] |
      | v[peter] |
    And the result should have a count of 2

  Scenario: g_VX1X_outXcreatedX_inEXcreatedX_rangeX1_3X_outV
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).out("created").inE("created").range(1, 3).outV()
      """
    When iterated to list
    Then the result should be of
      | result |
      | v[marko] |
      | v[josh] |
      | v[peter] |
    And the result should have a count of 2

  Scenario: g_V_repeatXbothX_timesX3X_rangeX5_11X
    Given the modern graph
    And the traversal of
      """
      g.V().repeat(__.both()).times(3).range(5, 11)
      """
    When iterated to list
    Then the result should be of
      | result |
      | v[marko] |
      | v[josh] |
      | v[peter] |
      | v[lop] |
      | v[vadas] |
      | v[ripple] |
    And the result should have a count of 6

  Scenario: g_V_asXaX_in_asXbX_in_asXcX_selectXa_b_cX_byXnameX_limitXlocal_2X
    Given the modern graph
    And the traversal of
      """
      g.V().as("a").in().as("b").in().as("c").select("a","b","c").by("name").limit(Scope.local, 2)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a":"lop","b":"josh"}] |
      | m[{"a":"ripple","b":"josh"}] |

  Scenario: g_V_asXaX_in_asXbX_in_asXcX_selectXa_b_cX_byXnameX_limitXlocal_2varX
    Given the modern graph
    And using the parameter xx1 defined as "d[2].i"
    And the traversal of
      """
      g.V().as("a").in().as("b").in().as("c").select("a","b","c").by("name").limit(Scope.local, xx1)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a":"lop","b":"josh"}] |
      | m[{"a":"ripple","b":"josh"}] |

  Scenario: g_V_asXaX_in_asXbX_in_asXcX_selectXa_b_cX_byXnameX_limitXlocal_1X
    Given the modern graph
    And the traversal of
      """
      g.V().as("a").in().as("b").in().as("c").select("a","b","c").by("name").limit(Scope.local, 1)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a":"lop"}] |
      | m[{"a":"ripple"}] |

  Scenario: g_V_asXaX_out_asXbX_out_asXcX_selectXa_b_cX_byXnameX_rangeXlocal_1_3X
    Given the modern graph
    And the traversal of
      """
      g.V().as("a").out().as("b").out().as("c").select("a","b","c").by("name").range(Scope.local, 1, 3)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"b":"josh","c":"lop"}] |
      | m[{"b":"josh","c":"ripple"}] |

  Scenario: g_V_asXaX_out_asXbX_out_asXcX_selectXa_b_cX_byXnameX_rangeXlocal_1var_3varX
    Given the modern graph
    And using the parameter xx1 defined as "d[1].i"
    And using the parameter xx2 defined as "d[3].i"
    And the traversal of
      """
      g.V().as("a").out().as("b").out().as("c").select("a","b","c").by("name").range(Scope.local, xx1, xx2)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"b":"josh","c":"lop"}] |
      | m[{"b":"josh","c":"ripple"}] |

  Scenario: g_V_asXaX_out_asXbX_out_asXcX_selectXa_b_cX_byXnameX_rangeXlocal_1_2X
    Given the modern graph
    And the traversal of
      """
      g.V().as("a").out().as("b").out().as("c").select("a","b","c").by("name").range(Scope.local, 1, 2)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"b":"josh"}] |
      | m[{"b":"josh"}] |

  Scenario: g_V_hasLabelXpersonX_order_byXageX_skipX1X_valuesXnameX
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("person").order().by("age").skip(1).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko |
      | josh |
      | peter |

  Scenario: g_V_hasLabelXpersonX_order_byXageX_skipX1varX_valuesXnameX
    Given the modern graph
    And using the parameter xx1 defined as "d[1].i"
    And the traversal of
      """
      g.V().hasLabel("person").order().by("age").skip(xx1).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko |
      | josh |
      | peter |

  @GraphComputerVerificationReferenceOnly
  Scenario: g_V_foldX_rangeXlocal_6_7X
    Given the modern graph
    And the traversal of
      """
      g.V().fold().range(Scope.local, 6, 7)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[] |

  Scenario: g_V_outE_valuesXweightX_fold_orderXlocalX_skipXlocal_2X
    Given the modern graph
    And the traversal of
      """
      g.V().outE().values("weight").fold().order(Scope.local).skip(Scope.local, 2)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[d[0.4].d,d[0.5].d,d[1.0].d,d[1.0].d] |

  Scenario: g_V_outE_valuesXweightX_fold_orderXlocalX_skipXlocal_2varX
    Given the modern graph
    And using the parameter xx1 defined as "d[2].i"
    And the traversal of
      """
      g.V().outE().values("weight").fold().order(Scope.local).skip(Scope.local, xx1)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[d[0.4].d,d[0.5].d,d[1.0].d,d[1.0].d] |

  Scenario: g_V_hasLabelXpersonX_order_byXageX_valuesXnameX_skipX1X
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("person").order().by("age").values("name").skip(1)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko |
      | josh |
      | peter |

  Scenario: g_VX1X_valuesXageX_rangeXlocal_20_30X
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).values("age").range(Scope.local, 20, 30)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[29].i |

  Scenario: g_V_mapXin_hasIdX1XX_limitX2X_valuesXnameX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V().map(__.in().hasId(vid1)).limit(2).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko |
      | marko |

  Scenario: g_V_rangeX2_1X
    Given the modern graph
    And the traversal of
      """
      g.V().range(2, 1)
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "Not a legal range: [2, 1]"

  # iterated next variant of the previous test (which is iterated to list)
  Scenario: g_V_rangeX3_2X
    Given the modern graph
    And the traversal of
      """
      g.V().range(3, 2)
      """
    When iterated next
    Then the traversal will raise an error with message containing text of "Not a legal range: [3, 2]"

  # Test consistent collection output for range(local) - single element should return collection
  Scenario: g_injectXlistX1_2_3XX_rangeXlocal_1_2X
    Given the empty graph
    And the traversal of
      """
      g.inject([1, 2, 3]).range(Scope.local, 1, 2)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[d[2].i] |

  # Test consistent collection output for limit(local) - single element should return collection
  Scenario: g_injectXlistX1_2_3XX_limitXlocal_1X
    Given the empty graph
    And the traversal of
      """
      g.inject([1, 2, 3]).limit(Scope.local, 1)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[d[1].i] |

  # Test unfold() can be used to extract single elements from collections
  Scenario: g_injectXlistX1_2_3X_limitXlocal_1X_unfold
    Given the empty graph
    And the traversal of
      """
      g.inject([1, 2, 3]).limit(Scope.local, 1).unfold()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[1].i |

  # Test multiple collections with consistent output
  Scenario: g_injectX1_2_3_4_5X_limitXlocal_1X
    Given the empty graph
    And the traversal of
      """
      g.inject([1, 2], [3, 4, 5]).limit(Scope.local, 1)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[d[1].i] |
      | l[d[3].i] |

  # Test range(local) with multiple collections
  Scenario: g_injectX1_2_3_4_5_6X_rangeXlocal_1_2X
    Given the empty graph
    And the traversal of
      """
      g.inject([1, 2, 3], [4, 5, 6]).range(Scope.local, 1, 2)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[d[2].i] |
      | l[d[5].i] |

  Scenario: 111.1
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.withoutStrategies(RepeatUnrollStrategy).V(vid1).repeat(__.limit(1)).times(2).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko |

  Scenario: 111.2
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.withoutStrategies(RepeatUnrollStrategy).V(vid1).repeat(__.limit(1)).until(__.loops().is(2)).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko |

  Scenario: 111.3
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.withoutStrategies(RepeatUnrollStrategy).V(vid1).limit(1).limit(1).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko |

  Scenario: 222.1
    Given the modern graph
    And using the parameter vid5 defined as "v[ripple].id"
    And the traversal of
      """
      g.withoutStrategies(RepeatUnrollStrategy).V(vid5).repeat(__.limit(1).in()).times(2).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko |

  Scenario: 222.2
    Given the modern graph
    And using the parameter vid5 defined as "v[ripple].id"
    And the traversal of
      """
      g.withoutStrategies(RepeatUnrollStrategy).V(vid5).repeat(__.limit(1).in()).until(loops().is(2)).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko |

  Scenario: 222.3
    Given the modern graph
    And using the parameter vid5 defined as "v[ripple].id"
    And the traversal of
      """
      g.withoutStrategies(RepeatUnrollStrategy).V(vid5).limit(1).in().limit(1).in().values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko |

  Scenario: 333.1
    Given the modern graph
    And using the parameter vid5 defined as "v[ripple].id"
    And the traversal of
      """
      g.withoutStrategies(RepeatUnrollStrategy).V(vid5).repeat(__.limit(1).in()).times(1).repeat(__.limit(1).in()).times(1).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko |

  Scenario: 333.2
    Given the modern graph
    And using the parameter vid5 defined as "v[ripple].id"
    And the traversal of
      """
      g.withoutStrategies(RepeatUnrollStrategy).V(vid5).repeat(__.limit(1).in()).until(loops().is(1)).repeat(__.limit(1).in()).until(loops().is(1)).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko |

  Scenario: 444.1
    Given the modern graph
    And using the parameter vid5 defined as "v[ripple].id"
    And the traversal of
      """
      g.withoutStrategies(RepeatUnrollStrategy).V(vid5).repeat(__.limit(1).in().repeat(__.limit(1).in()).times(1)).times(1).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko |

  Scenario: 444.2
    Given the modern graph
    And using the parameter vid5 defined as "v[ripple].id"
    And the traversal of
      """
      g.withoutStrategies(RepeatUnrollStrategy).V(vid5).repeat(limit(1).in().aggregate('x')).times(2).cap('x')
      """
    When iterated next
    Then the result should be unordered
      | result |
      | v[josh] |
      | v[marko] |