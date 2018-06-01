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
    And using the parameter vx defined as "l[v[marko].id,v[vadas].id,v[lop].id]"
    And the traversal of
      """
      g.V(vx).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko |
      | vadas |
      | lop |

  Scenario: g_VXlistXv1_v2_v3XX_name
    Given the modern graph
    And using the parameter vx defined as "l[v[marko],v[vadas],v[lop]]"
    And the traversal of
      """
      g.V(vx).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
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
      | result |
      | v[marko] |
      | v[vadas] |
      | v[lop] |
      | v[josh] |
      | v[ripple] |
      | v[peter] |

  Scenario: g_VX1X_out
    Given the modern graph
    And using the parameter v1Id defined as "v[marko].id"
    And the traversal of
      """
      g.V(v1Id).out()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[vadas] |
      | v[lop] |
      | v[josh] |

  Scenario: g_VX2X_in
    Given the modern graph
    And using the parameter v2Id defined as "v[vadas].id"
    And the traversal of
      """
      g.V(v2Id).in()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |

  Scenario: g_VX4X_both
    Given the modern graph
    And using the parameter v4Id defined as "v[josh].id"
    And the traversal of
      """
      g.V(v4Id).both()
      """
    When iterated to list
    Then the result should be unordered
      | result |
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
      | result |
      | e[marko-created->lop] |
      | e[marko-knows->josh] |
      | e[marko-knows->vadas] |
      | e[peter-created->lop] |
      | e[josh-created->lop] |
      | e[josh-created->ripple] |

  Scenario: g_EX11X
    Given the modern graph
    And using the parameter e11Id defined as "e[josh-created->lop].id"
    And the traversal of
    """
      g.E(e11Id)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | e[josh-created->lop] |

  Scenario: g_EX11AsStringX
    Given the modern graph
    And using the parameter e11Id defined as "e[josh-created->lop].sid"
    And the traversal of
    """
      g.E(e11Id)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | e[josh-created->lop] |

  Scenario: g_VX1X_outE
    Given the modern graph
    And using the parameter v1Id defined as "v[marko].id"
    And the traversal of
    """
      g.V(v1Id).outE()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | e[marko-created->lop] |
      | e[marko-knows->josh] |
      | e[marko-knows->vadas] |

  Scenario: g_VX2X_outE
    Given the modern graph
    And using the parameter v2Id defined as "v[vadas].id"
    And the traversal of
    """
      g.V(v2Id).inE()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | e[marko-knows->vadas] |

  Scenario: g_VX4X_bothEXcreatedX
    Given the modern graph
    And using the parameter v4Id defined as "v[josh].id"
    And the traversal of
    """
      g.V(v4Id).bothE("created")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | e[josh-created->lop] |
      | e[josh-created->ripple] |

  Scenario: g_VX4X_bothE
    Given the modern graph
    And using the parameter v4Id defined as "v[josh].id"
    And the traversal of
    """
      g.V(v4Id).bothE()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | e[josh-created->lop] |
      | e[josh-created->ripple] |
      | e[marko-knows->josh] |

  Scenario: g_VX1X_outE_inV
    Given the modern graph
    And using the parameter v1Id defined as "v[marko].id"
    And the traversal of
      """
      g.V(v1Id).both()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[vadas] |
      | v[josh] |
      | v[lop] |

  Scenario: g_VX2X_inE_outV
    Given the modern graph
    And using the parameter v2Id defined as "v[vadas].id"
    And the traversal of
      """
      g.V(v2Id).inE().outV()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |

  Scenario: g_V_outE_hasXweight_1X_outV
    Given the modern graph
    And the traversal of
      """
      g.V().outE().has("weight",1.0).outV()
      """
    When iterated to list
    Then the result should be unordered
      | result |
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
      | result |
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
    And using the parameter v1Id defined as "v[marko].id"
    And the traversal of
      """
      g.V(v1Id).outE("knows").bothV().values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko |
      | marko |
      | josh |
      | vadas |

  Scenario: g_VX1X_outE_otherV
    Given the modern graph
    And using the parameter v1Id defined as "v[marko].id"
    And the traversal of
      """
      g.V(v1Id).outE().otherV()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[vadas] |
      | v[josh] |
      | v[lop] |

  Scenario: g_VX4X_bothE_otherV
    Given the modern graph
    And using the parameter v4Id defined as "v[josh].id"
    And the traversal of
      """
      g.V(v4Id).bothE().otherV()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |
      | v[ripple] |
      | v[lop] |

  Scenario: g_VX4X_bothE_hasXweight_lt_1X_otherV
    Given the modern graph
    And using the parameter v4Id defined as "v[josh].id"
    And the traversal of
      """
      g.V(v4Id).bothE().has("weight", P.lt(1.0)).otherV()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[lop] |

  Scenario: g_VX2X_inE
    Given the modern graph
    And using the parameter v2Id defined as "v[vadas].id"
    And the traversal of
    """
      g.V(v2Id).bothE()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | e[marko-knows->vadas] |

  Scenario: get_g_VX1X_outE_otherV
    Given the modern graph
    And using the parameter v1Id defined as "v[marko].id"
    And the traversal of
      """
      g.V(v1Id).outE().otherV()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[vadas] |
      | v[josh] |
      | v[lop] |

  Scenario: g_VX1X_outXknowsX
    Given the modern graph
    And using the parameter v1Id defined as "v[marko].id"
    And the traversal of
      """
      g.V(v1Id).out("knows")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[vadas] |
      | v[josh] |

  Scenario: g_VX1AsStringX_outXknowsX
    Given the modern graph
    And using the parameter v1Id defined as "v[marko].sid"
    And the traversal of
      """
      g.V(v1Id).out("knows")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[vadas] |
      | v[josh] |

  Scenario: g_VX1X_outXknows_createdX
    Given the modern graph
    And using the parameter v1Id defined as "v[marko].id"
    And the traversal of
      """
      g.V(v1Id).out("knows","created")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[vadas] |
      | v[josh] |
      | v[lop] |

  Scenario: g_VX1X_outEXknowsX_inV
    Given the modern graph
    And using the parameter v1Id defined as "v[marko].id"
    And the traversal of
      """
      g.V(v1Id).outE("knows").inV()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[vadas] |
      | v[josh] |

  Scenario: g_VX1X_outEXknows_createdX_inV
    Given the modern graph
    And using the parameter v1Id defined as "v[marko].id"
    And the traversal of
      """
      g.V(v1Id).outE("knows","created").inV()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[vadas] |
      | v[josh] |
      | v[lop] |

  Scenario: g_V_out_out
    Given the modern graph
    And the traversal of
      """
      g.V().out().out()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[ripple] |
      | v[lop] |

  Scenario: g_VX1X_out_out_out
    Given the modern graph
    And using the parameter v1Id defined as "v[marko].id"
    And the traversal of
      """
      g.V(v1Id).out().out().out()
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_VX1X_out_name
    Given the modern graph
    And using the parameter v1Id defined as "v[marko].id"
    And the traversal of
      """
      g.V(v1Id).out().values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | vadas |
      | josh |
      | lop |

  Scenario: g_VX1X_to_XOUT_knowsX
    Given the modern graph
    And using the parameter v1Id defined as "v[marko].id"
    And the traversal of
      """
      g.V(v1Id).to(Direction.OUT, "knows")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[vadas] |
      | v[josh] |

  # this test deviates from the setup of the java test, but the intent is the same. the java test drops lop and then
  # tries to query it as part of the group of ids. here, rather than drop, we simply use an id that doesn't exist
  # which is simulated by an edge identifier.
  Scenario: g_VX1_2_3_4X_name
    Given the modern graph
    And using the parameter v1Id defined as "v[marko].id"
    And using the parameter v2Id defined as "v[vadas].id"
    And using the parameter v3Id defined as "e[marko-knows->josh].id"
    And using the parameter v4Id defined as "v[josh].id"
    And the traversal of
      """
      g.V(v1Id, v2Id, v3Id, v4Id).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko |
      | vadas |
      | josh |

  Scenario: g_V_hasLabelXpersonX_V_hasLabelXsoftwareX_name
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("person").V().hasLabel("software").values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | lop |
      | lop |
      | lop |
      | lop |
      | ripple |
      | ripple |
      | ripple |
      | ripple |

  Scenario: g_V_hasLabelXloopsX_bothEXselfX
    Given the sink graph
    And the traversal of
    """
    g.V().hasLabel("loops").bothE("self")
    """
    When iterated to list
    Then the result should be unordered
      | result |
      | e[loop-self->loop] |
      | e[loop-self->loop] |

  Scenario: g_V_hasLabelXloopsX_bothXselfX
    Given the sink graph
    And the traversal of
    """
    g.V().hasLabel("loops").both("self")
    """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[loop] |
      | v[loop] |
