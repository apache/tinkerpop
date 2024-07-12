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

@StepClassFilter @StepHasId
Feature: Step - hasId()

  Scenario: g_V_hasIdXemptyX_count
    Given the modern graph
    And using the parameter xx1 defined as "l[]"
    And the traversal of
    """
    g.V().hasId(xx1).count()
    """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[0].l |

  Scenario: g_V_hasIdXwithinXemptyXX_count
    Given the modern graph
    And the traversal of
    """
    g.V().hasId(P.within([])).count()
    """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[0].l |

  Scenario: g_V_hasIdXwithoutXemptyXX_count
    Given the modern graph
    And the traversal of
    """
    g.V().hasId(P.without([])).count()
    """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[6].l |

  Scenario: g_V_notXhasIdXwithinXemptyXXX_count
    Given the modern graph
    And the traversal of
    """
    g.V().not(__.hasId(P.within([]))).count()
    """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[6].l |

  Scenario: g_V_hasIdXnullX
    Given the modern graph
    And the traversal of
      """
      g.V().hasId(null)
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_hasIdXeqXnullXX
    Given the modern graph
    And the traversal of
      """
      g.V().hasId(P.eq(null))
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_hasIdX2_nullX
    Given the modern graph
    And using the parameter vid2 defined as "v[vadas].id"
    And the traversal of
      """
      g.V().hasId(vid2, null)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[vadas] |

  Scenario: g_V_hasIdXmarkovar_vadasvarX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And using the parameter vid2 defined as "v[vadas].id"
    And the traversal of
      """
      g.V().hasId(vid1, vid2)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |
      | v[vadas] |

  Scenario: g_V_hasIdXmarkovar_vadasvar_petervarX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And using the parameter vid2 defined as "l[v[vadas].id,v[peter].id]"
    And the traversal of
      """
      g.V().hasId(vid1, vid2)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |
      | v[vadas] |
      | v[peter] |

  Scenario: g_V_hasIdX2AsString_nullX
    Given the modern graph
    And using the parameter vid2 defined as "v[vadas].sid"
    And the traversal of
      """
      g.V().hasId(vid2, null)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[vadas] |

  Scenario: g_V_hasIdX1AsString_2AsString_nullX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].sid"
    And using the parameter vid2 defined as "v[vadas].sid"
    And the traversal of
      """
      g.V().hasId(vid1, vid2, null)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |
      | v[vadas] |

  Scenario: g_V_hasIdXnull_2X
    Given the modern graph
    And using the parameter vid2 defined as "v[vadas].id"
    And the traversal of
      """
      g.V().hasId(null, vid2)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[vadas] |

  Scenario: g_V_hasIdX1X_hasIdX2X
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And using the parameter vid2 defined as "v[vadas].id"
    And the traversal of
    """
    g.V().hasId(vid1).hasId(vid2)
    """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_in_hasIdXneqX1XX
    Given the modern graph
    And using the parameter xx1 defined as "v[marko].id"
    And the traversal of
    """
    g.V().in().hasId(P.neq(xx1))
    """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[josh] |
      | v[josh] |
      | v[peter] |

  Scenario: g_VX1X_out_hasIdX2X
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And using the parameter vid2 defined as "v[vadas].id"
    And the traversal of
      """
      g.V(vid1).out().hasId(vid2)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[vadas] |

  Scenario: g_VX1X_out_hasXid_2_3X
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And using the parameter vid2 defined as "v[vadas].id"
    And using the parameter vid3 defined as "v[lop].id"
    And the traversal of
      """
      g.V(vid1).out().hasId(vid2, vid3)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[vadas] |
      | v[lop] |

  Scenario: g_VX1X_out_hasXid_2AsString_3AsStringX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].sid"
    And using the parameter vid2 defined as "v[vadas].sid"
    And using the parameter vid3 defined as "v[lop].sid"
    And the traversal of
      """
      g.V(vid1).out().hasId(vid2, vid3)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[vadas] |
      | v[lop] |

  Scenario: g_VX1AsStringX_out_hasXid_2AsStringX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].sid"
    And using the parameter vid2 defined as "v[vadas].sid"
    And the traversal of
      """
      g.V(vid1).out().hasId(vid2)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[vadas] |

  Scenario: g_VX1X_out_hasXid_2_3X_inList
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And using the parameter xx1 defined as "l[v[vadas].id,v[lop].id]"
    And the traversal of
      """
      g.V(vid1).out().hasId(xx1)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[vadas] |
      | v[lop] |

  Scenario: g_V_hasXid_1_2X
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And using the parameter vid2 defined as "v[vadas].id"
    And the traversal of
      """
      g.V().hasId(vid1, vid2)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |
      | v[vadas] |

  Scenario: g_V_hasXid_1_2X_inList
    Given the modern graph
    And using the parameter xx1 defined as "l[v[marko].id,v[vadas].id]"
    And the traversal of
      """
      g.V().hasId(xx1)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |
      | v[vadas] |