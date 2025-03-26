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

@StepClassMap @StepE
Feature: Step - E(), inV(), outV(), bothV(), otherV()

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
    And using the parameter eid11 defined as "e[josh-created->lop].id"
    And the traversal of
      """
      g.E(eid11)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | e[josh-created->lop] |

  Scenario: g_EX11AsStringX
    Given the modern graph
    And using the parameter eid11 defined as "e[josh-created->lop].sid"
    And the traversal of
      """
      g.E(eid11)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | e[josh-created->lop] |

  Scenario: g_EXe11X
    Given the modern graph
    And using the parameter e11 defined as "e[josh-created->lop]"
    And the traversal of
      """
      g.E(e11)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | e[josh-created->lop] |

  Scenario: g_EXe7_e11X
    Given the modern graph
    And using the parameter e7 defined as "e[marko-knows->vadas]"
    And using the parameter e11 defined as "e[josh-created->lop]"
    And the traversal of
      """
      g.E(e7,e11)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | e[marko-knows->vadas] |
      | e[josh-created->lop] |

  Scenario: g_EXlistXe7_e11XX
    Given the modern graph
    And using the parameter xx1 defined as "l[e[marko-knows->vadas],e[josh-created->lop]]"
    And the traversal of
      """
      g.E(xx1)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | e[marko-knows->vadas] |
      | e[josh-created->lop] |

  Scenario: g_EXnullX
    Given the modern graph
    And the traversal of
      """
      g.E(null)
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_EXlistXnullXX
    Given the modern graph
    And using the parameter xx1 defined as "l[null]"
    And the traversal of
      """
      g.E(xx1)
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_EX11_nullX
    Given the modern graph
    And using the parameter eid11 defined as "e[josh-created->lop].id"
    And the traversal of
      """
      g.E(eid11,null)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | e[josh-created->lop] |

  Scenario: g_V_EX11X
    Given the modern graph
    And using the parameter eid11 defined as "e[josh-created->lop].id"
    And the traversal of
      """
      g.V().E(eid11)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | e[josh-created->lop] |
      | e[josh-created->lop] |
      | e[josh-created->lop] |
      | e[josh-created->lop] |
      | e[josh-created->lop] |
      | e[josh-created->lop] |

  Scenario: g_EX11X_E
    Given the modern graph
    And using the parameter eid11 defined as "e[josh-created->lop].id"
    And the traversal of
      """
      g.E(eid11).E()
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
      
  Scenario: g_V_EXnullX
    Given the modern graph
    And the traversal of
      """
      g.V().E(null)
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_EXlistXnullXX
    Given the modern graph
    And using the parameter xx1 defined as "l[null]"
    And the traversal of
      """
      g.V().E(xx1)
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_injectX1X_EX11_nullX
    Given the modern graph
    And using the parameter eid11 defined as "e[josh-created->lop].id"
    And the traversal of
      """
      g.inject(1).E(eid11,null)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | e[josh-created->lop] |

 Scenario: g_injectX1X_coalesceXEX_hasLabelXtestsX_addEXtestsX_from_V_hasXnameX_XjoshXX_toXV_hasXnameX_XvadasXXX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", "josh").
        addV("person").property("name", "vadas")
      """
    And the traversal of
      """
      g.inject(1).coalesce(E().hasLabel("tests"), addE("tests").from(V().has("name","josh")).to(V().has("name","vadas")))
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 1 for count of "g.E().hasLabel(\"tests\")"

  Scenario: g_VX1X_outE_inV
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).outE().inV()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[vadas] |
      | v[josh] |
      | v[lop] |

  Scenario: g_VX2X_inE_outV
    Given the modern graph
    And using the parameter vid2 defined as "v[vadas].id"
    And the traversal of
      """
      g.V(vid2).inE().outV()
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

  Scenario: g_VX1X_outE_otherV
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).outE().otherV()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[vadas] |
      | v[josh] |
      | v[lop] |

  Scenario: g_VX4X_bothE_otherV
    Given the modern graph
    And using the parameter vid4 defined as "v[josh].id"
    And the traversal of
      """
      g.V(vid4).bothE().otherV()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |
      | v[ripple] |
      | v[lop] |

  Scenario: g_VX4X_bothE_hasXweight_lt_1X_otherV
    Given the modern graph
    And using the parameter vid4 defined as "v[josh].id"
    And the traversal of
      """
      g.V(vid4).bothE().has("weight", P.lt(1.0)).otherV()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[lop] |

  Scenario: get_g_VX1X_outE_otherV
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).outE().otherV()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[vadas] |
      | v[josh] |
      | v[lop] |

  Scenario: g_VX1X_outEXknowsX_inV
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).outE("knows").inV()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[vadas] |
      | v[josh] |

  Scenario: g_VX1X_outEXknows_createdX_inV
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).outE("knows","created").inV()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[vadas] |
      | v[josh] |
      | v[lop] |

  Scenario: g_VX1X_outEXknowsX_bothV
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).outE("knows").bothV()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |
      | v[marko] |
      | v[josh] |
      | v[vadas] |

  Scenario: g_VX1X_outEXknowsX_bothV_name
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).outE("knows").bothV().values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko |
      | marko |
      | josh |
      | vadas |

  Scenario: g_V_toEXout_knowsvarX_valuesXweightX
    Given the modern graph
    And using the parameter xx1 defined as "knows"
    And the traversal of
      """
      g.V().toE(Direction.OUT, xx1).values("weight")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[0.5].d |
      | d[1.0].d |