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

Feature: Step - V(), E(), out(), in(), both(), inE(), outE(), bothE()

  Scenario: g_VXlistX1_2_3XX_name
    Given the modern graph
    And using the parameter vx is "l[v[marko],v[vadas],v[lop]]"
    And the traversal of
      """
      g.V(vx).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | marko |
      | vadas |
      | lop |

  Scenario: g_VXlistXv1_v2_v3XX_name
    Given the modern graph
    And using the parameter vx is "l[v[marko].id,v[vadas].id,v[lop].id]"
    And the traversal of
      """
      g.V(vx).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | marko |
      | vadas |
      | lop |

  Scenario: g_V
    Given the modern graph
    And the traversal of
      """
      g.V()
      """
    When iterated to list
    Then the result should be unordered
      | v[marko] |
      | v[vadas] |
      | v[lop] |
      | v[josh] |
      | v[ripple] |
      | v[peter] |

  Scenario: g_VX1X_out
    Given the modern graph
    And using the parameter v1 is "v[marko]"
    And the traversal of
      """
      g.V(v1).out()
      """
    When iterated to list
    Then the result should be unordered
      | v[vadas] |
      | v[lop] |
      | v[josh] |

  Scenario: g_VX2X_in
    Given the modern graph
    And using the parameter v2 is "v[vadas]"
    And the traversal of
      """
      g.V(v2).in()
      """
    When iterated to list
    Then the result should be unordered
      | v[marko] |

  Scenario: g_VX4X_both
    Given the modern graph
    And using the parameter v4 is "v[josh]"
    And the traversal of
      """
      g.V(v4).both()
      """
    When iterated to list
    Then the result should be unordered
      | v[marko] |
      | v[lop] |
      | v[ripple] |

  Scenario: g_E
    Given the modern graph
    And the traversal of
      """
      g.E()
      """
    When iterated to list
    Then the result should be unordered
      | e[marko-created->lop] |
      | e[marko-knows->josh] |
      | e[marko-knows->vadas] |
      | e[peter-created->lop] |
      | e[josh-created->lop] |
      | e[josh-created->ripple] |

  Scenario: g_EX11X
    Given the modern graph
    And using the parameter e11 is "e[josh-created->lop]"
    And the traversal of
    """
      g.E(e11)
      """
    When iterated to list
    Then the result should be unordered
      | e[josh-created->lop] |

  Scenario: g_VX1X_outE
    Given the modern graph
    And using the parameter v1 is "v[marko]"
    And the traversal of
    """
      g.V(v1).outE()
      """
    When iterated to list
    Then the result should be unordered
      | e[marko-created->lop] |
      | e[marko-knows->josh] |
      | e[marko-knows->vadas] |

  Scenario: g_VX2X_outE
    Given the modern graph
    And using the parameter v2 is "v[vadas]"
    And the traversal of
    """
      g.V(v2).inE()
      """
    When iterated to list
    Then the result should be unordered
      | e[marko-knows->vadas] |

  Scenario: g_VX4X_bothEXcreatedX
    Given the modern graph
    And using the parameter v4 is "v[josh]"
    And the traversal of
    """
      g.V(v4).bothE("created")
      """
    When iterated to list
    Then the result should be unordered
      | e[josh-created->lop] |
      | e[josh-created->ripple] |

  Scenario: g_VX4X_bothE
    Given the modern graph
    And using the parameter v4 is "v[josh]"
    And the traversal of
    """
      g.V(v4).bothE()
      """
    When iterated to list
    Then the result should be unordered
      | e[josh-created->lop] |
      | e[josh-created->ripple] |
      | e[marko-knows->josh] |

  Scenario: g_VX1X_outE_inV
    Given the modern graph
    And using the parameter v1 is "v[marko]"
    And the traversal of
      """
      g.V(v1).both()
      """
    When iterated to list
    Then the result should be unordered
      | v[vadas] |
      | v[josh] |
      | v[lop] |

  Scenario: g_VX2X_inE_outV
    Given the modern graph
    And using the parameter v2 is "v[vadas]"
    And the traversal of
      """
      g.V(v2).inE().outV()
      """
    When iterated to list
    Then the result should be unordered
      | v[marko] |

  Scenario: g_V_outE_hasXweight_1X_outV
    Given the modern graph
    And the traversal of
      """
      g.V().outE().has("weight",1.0).outV()
      """
    When iterated to list
    Then the result should be unordered
      | v[marko] |
      | v[josh] |

  Scenario: g_V_out_outE_inV_inE_inV_both_name
    Given the modern graph
    And the traversal of
      """
      g.V().out().outE().inV().inE().inV().both().values("name")
      """
    When iterated to list
    Then the result should be unordered
      | marko |
      | marko |
      | marko |
      | josh |
      | josh |
      | josh |
      | josh |
      | peter |
      | peter |
      | peter |

  Scenario: g_VX1X_outEXknowsX_bothV_name
    Given the modern graph
    And using the parameter v1 is "v[marko]"
    And the traversal of
      """
      g.V(v1).outE("knows").bothV().values("name")
      """
    When iterated to list
    Then the result should be unordered
      | marko |
      | marko |
      | josh |
      | vadas |

  Scenario: g_VX1X_outE_otherV
    Given the modern graph
    And using the parameter v1 is "v[marko]"
    And the traversal of
      """
      g.V(v1).outE().otherV()
      """
    When iterated to list
    Then the result should be unordered
      | v[vadas] |
      | v[josh] |
      | v[lop] |

  Scenario: g_VX4X_bothE_otherV
    Given the modern graph
    And using the parameter v4 is "v[josh]"
    And the traversal of
      """
      g.V(v4).bothE().otherV()
      """
    When iterated to list
    Then the result should be unordered
      | v[marko] |
      | v[ripple] |
      | v[lop] |

  Scenario: g_VX4X_bothE_hasXweight_lt_1X_otherV
    Given the modern graph
    And using the parameter v4 is "v[josh]"
    And the traversal of
      """
      g.V(v4).bothE().has("weight", P.lt(1.0)).otherV()
      """
    When iterated to list
    Then the result should be unordered
      | v[lop] |