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

@StepClassIntegrated
Feature: Step - SubgraphStrategy

  # subgraphA = new SubgraphStrategy(vertices: __.has("name", P.within("josh", "lop", "ripple")))
  #
  # subgraphB = new SubgraphStrategy(edges: or(
  #                has("weight", 1.0).hasLabel("knows"), // 8
  #                has("weight", 0.4).hasLabel("created").outV().has("name", "marko"), // 9
  #                has("weight", 1.0).hasLabel("created"))) // 10
  #
  # edge 9 isn't present because marko is not in the vertex list
  # subgraphC = new SubgraphStrategy(vertices: has("name", P.within("josh", "lop", "ripple")),
  #                                  edges: or(has("weight", 0.4).hasLabel("created"), // 11
  #                                               has("weight", 1.0).hasLabel("created"))) // 10
  #
  # edge 9 isn't present because marko is not in the vertex list
  # subgraphD = new SubgraphStrategy(checkAdjacentVertices: false,
  #                                  vertices: has("name", P.within("josh", "lop", "ripple")),
  #                                  edges: or(has("weight", 0.4).hasLabel("created"), // 11
  #                                               has("weight", 1.0).hasLabel("created"))) // 10

  @WithSubgraphStrategy @GraphComputerVerificationStarGraphExceeded
  Scenario: g_withStrategiesXSubgraphStrategyXsubgraphAXX_V
    Given the modern graph
    And the traversal of
      """
      g.withStrategies(new SubgraphStrategy(vertices: __.has("name", P.within("josh", "lop", "ripple")))).V()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[lop] |
      | v[josh] |
      | v[ripple] |

  @WithSubgraphStrategy @GraphComputerVerificationStarGraphExceeded
  Scenario: g_withStrategiesXSubgraphStrategyXsubgraphAXX_E
    Given the modern graph
    And the traversal of
      """
      g.withStrategies(new SubgraphStrategy(vertices: __.has("name", P.within("josh", "lop", "ripple")))).E()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | e[josh-created->lop] |
      | e[josh-created->ripple] |

  @WithSubgraphStrategy @GraphComputerVerificationStarGraphExceeded
  Scenario: g_withStrategiesXSubgraphStrategyXsubgraphAXX_VX4X_outE
    Given the modern graph
    And using the parameter vid4 defined as "v[josh].id"
    And the traversal of
      """
      g.withStrategies(new SubgraphStrategy(vertices: __.has("name", P.within("josh", "lop", "ripple")))).V(vid4).outE()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | e[josh-created->lop] |
      | e[josh-created->ripple] |

  @WithSubgraphStrategy @GraphComputerVerificationStarGraphExceeded
  Scenario: g_withStrategiesXSubgraphStrategyXsubgraphAXX_VX4X_inE
    Given the modern graph
    And using the parameter vid4 defined as "v[josh].id"
    And the traversal of
      """
      g.withStrategies(new SubgraphStrategy(vertices: __.has("name", P.within("josh", "lop", "ripple")))).V(vid4).inE()
      """
    When iterated to list
    Then the result should be empty

  @WithSubgraphStrategy @GraphComputerVerificationStarGraphExceeded
  Scenario: g_withStrategiesXSubgraphStrategyXsubgraphAXX_VX4X_out
    Given the modern graph
    And using the parameter vid4 defined as "v[josh].id"
    And the traversal of
      """
      g.withStrategies(new SubgraphStrategy(vertices: __.has("name", P.within("josh", "lop", "ripple")))).V(vid4).out()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[lop] |
      | v[ripple] |

  @WithSubgraphStrategy @GraphComputerVerificationStarGraphExceeded
  Scenario: g_withStrategiesXSubgraphStrategyXsubgraphAXX_VX4X_in
    Given the modern graph
    And using the parameter vid4 defined as "v[josh].id"
    And the traversal of
      """
      g.withStrategies(new SubgraphStrategy(vertices: __.has("name", P.within("josh", "lop", "ripple")))).V(vid4).in()
      """
    When iterated to list
    Then the result should be empty

  @WithSubgraphStrategy @GraphComputerVerificationStarGraphExceeded
  Scenario: g_withStrategiesXSubgraphStrategyXsubgraphAXX_VX4X_both
    Given the modern graph
    And using the parameter vid4 defined as "v[josh].id"
    And the traversal of
      """
      g.withStrategies(new SubgraphStrategy(vertices: __.has("name", P.within("josh", "lop", "ripple")))).V(vid4).both()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[lop] |
      | v[ripple] |

  @WithSubgraphStrategy @GraphComputerVerificationStarGraphExceeded
  Scenario: g_withStrategiesXSubgraphStrategyXsubgraphAXX_VX4X_bothE
    Given the modern graph
    And using the parameter vid4 defined as "v[josh].id"
    And the traversal of
      """
      g.withStrategies(new SubgraphStrategy(vertices: __.has("name", P.within("josh", "lop", "ripple")))).V(vid4).bothE()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | e[josh-created->lop] |
      | e[josh-created->ripple] |

  @WithSubgraphStrategy @GraphComputerVerificationStarGraphExceeded
  Scenario: g_withStrategiesXSubgraphStrategyXsubgraphAXX_VX4X_localXbothE_limitX1XX
    Given the modern graph
    And using the parameter vid4 defined as "v[josh].id"
    And the traversal of
      """
      g.withStrategies(new SubgraphStrategy(vertices: __.has("name", P.within("josh", "lop", "ripple")))).V(vid4).local(__.bothE().limit(1))
      """
    When iterated to list
    Then the result should have a count of 1

  @WithSubgraphStrategy @GraphComputerVerificationStarGraphExceeded
  Scenario: g_withStrategiesXSubgraphStrategyXsubgraphAXX_EX11X_bothV
    Given the modern graph
    And using the parameter eid11 defined as "e[josh-created->lop].id"
    And the traversal of
      """
      g.withStrategies(new SubgraphStrategy(vertices: __.has("name", P.within("josh", "lop", "ripple")))).E(eid11).bothV()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[josh] |
      | v[lop] |

  @WithSubgraphStrategy @GraphComputerVerificationStarGraphExceeded
  Scenario: g_withStrategiesXSubgraphStrategyXsubgraphAXX_EX12X_bothV
    Given the modern graph
    And using the parameter eid12 defined as "e[peter-created->lop].id"
    And the traversal of
      """
      g.withStrategies(new SubgraphStrategy(vertices: __.has("name", P.within("josh", "lop", "ripple")))).E(eid12).bothV()
      """
    When iterated to list
    Then the result should be empty

  @WithSubgraphStrategy @GraphComputerVerificationStarGraphExceeded
  Scenario: g_withStrategiesXSubgraphStrategyXsubgraphBXX_V
    Given the modern graph
    And the traversal of
      """
      g.withStrategies(new SubgraphStrategy(edges: __.or(__.has("weight", 1.0).hasLabel("knows"),
                                                         __.has("weight", 0.4).hasLabel("created").outV().has("name", "marko"),
                                                         __.has("weight", 1.0).hasLabel("created")))).V()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |
      | v[vadas] |
      | v[lop] |
      | v[josh] |
      | v[ripple] |
      | v[peter]  |

  @WithSubgraphStrategy @GraphComputerVerificationStarGraphExceeded
  Scenario: g_withStrategiesXSubgraphStrategyXsubgraphBXX_E
    Given the modern graph
    And the traversal of
      """
      g.withStrategies(new SubgraphStrategy(edges: __.or(__.has("weight", 1.0).hasLabel("knows"),
                                                         __.has("weight", 0.4).hasLabel("created").outV().has("name", "marko"),
                                                         __.has("weight", 1.0).hasLabel("created")))).E()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | e[marko-knows->josh] |
      | e[marko-created->lop] |
      | e[josh-created->ripple] |

  @WithSubgraphStrategy @GraphComputerVerificationStarGraphExceeded
  Scenario: g_withStrategiesXSubgraphStrategyXsubgraphBXX_VX1X_outE
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.withStrategies(new SubgraphStrategy(edges: __.or(__.has("weight", 1.0).hasLabel("knows"),
                                                         __.has("weight", 0.4).hasLabel("created").outV().has("name", "marko"),
                                                         __.has("weight", 1.0).hasLabel("created")))).V(vid1).outE()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | e[marko-knows->josh] |
      | e[marko-created->lop] |

  @WithSubgraphStrategy @GraphComputerVerificationStarGraphExceeded
  Scenario: g_withStrategiesXSubgraphStrategyXsubgraphBXX_VX1X_out
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.withStrategies(new SubgraphStrategy(edges: __.or(__.has("weight", 1.0).hasLabel("knows"),
                                                         __.has("weight", 0.4).hasLabel("created").outV().has("name", "marko"),
                                                         __.has("weight", 1.0).hasLabel("created")))).V(vid1).out()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[josh] |
      | v[lop] |

  @WithSubgraphStrategy @GraphComputerVerificationStarGraphExceeded
  Scenario: g_withStrategiesXSubgraphStrategyXsubgraphBXX_VX1X_outXcreatedX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.withStrategies(new SubgraphStrategy(edges: __.or(__.has("weight", 1.0).hasLabel("knows"),
                                                         __.has("weight", 0.4).hasLabel("created").outV().has("name", "marko"),
                                                         __.has("weight", 1.0).hasLabel("created")))).V(vid1).out("knows")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[josh] |

  @WithSubgraphStrategy @GraphComputerVerificationStarGraphExceeded
  Scenario: g_withStrategiesXSubgraphStrategyXsubgraphBXX_VX4X_outXcreatedX
    Given the modern graph
    And using the parameter vid4 defined as "v[josh].id"
    And the traversal of
      """
      g.withStrategies(new SubgraphStrategy(edges: __.or(__.has("weight", 1.0).hasLabel("knows"),
                                                         __.has("weight", 0.4).hasLabel("created").outV().has("name", "marko"),
                                                         __.has("weight", 1.0).hasLabel("created")))).V(vid4).out("created")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[ripple] |

  @WithSubgraphStrategy @GraphComputerVerificationStarGraphExceeded
  Scenario: g_withStrategiesXSubgraphStrategyXsubgraphBXX_VX4X_outE
    Given the modern graph
    And using the parameter vid4 defined as "v[josh].id"
    And the traversal of
      """
      g.withStrategies(new SubgraphStrategy(edges: __.or(__.has("weight", 1.0).hasLabel("knows"),
                                                         __.has("weight", 0.4).hasLabel("created").outV().has("name", "marko"),
                                                         __.has("weight", 1.0).hasLabel("created")))).V(vid4).outE()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | e[josh-created->ripple] |

  @WithSubgraphStrategy @GraphComputerVerificationStarGraphExceeded
  Scenario: g_withStrategiesXSubgraphStrategyXsubgraphBXX_VX4X_out
    Given the modern graph
    And using the parameter vid4 defined as "v[josh].id"
    And the traversal of
      """
      g.withStrategies(new SubgraphStrategy(edges: __.or(__.has("weight", 1.0).hasLabel("knows"),
                                                         __.has("weight", 0.4).hasLabel("created").outV().has("name", "marko"),
                                                         __.has("weight", 1.0).hasLabel("created")))).V(vid4).out()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[ripple] |

  @WithSubgraphStrategy @GraphComputerVerificationStarGraphExceeded
  Scenario: g_withStrategiesXSubgraphStrategyXsubgraphBXX_VX4X_bothE
    Given the modern graph
    And using the parameter vid4 defined as "v[josh].id"
    And the traversal of
      """
      g.withStrategies(new SubgraphStrategy(edges: __.or(__.has("weight", 1.0).hasLabel("knows"),
                                                         __.has("weight", 0.4).hasLabel("created").outV().has("name", "marko"),
                                                         __.has("weight", 1.0).hasLabel("created")))).V(vid4).bothE()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | e[marko-knows->josh] |
      | e[josh-created->ripple] |

  @WithSubgraphStrategy @GraphComputerVerificationStarGraphExceeded
  Scenario: g_withStrategiesXSubgraphStrategyXsubgraphBXX_VX4X_both
    Given the modern graph
    And using the parameter vid4 defined as "v[josh].id"
    And the traversal of
      """
      g.withStrategies(new SubgraphStrategy(edges: __.or(__.has("weight", 1.0).hasLabel("knows"),
                                                         __.has("weight", 0.4).hasLabel("created").outV().has("name", "marko"),
                                                         __.has("weight", 1.0).hasLabel("created")))).V(vid4).both()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |
      | v[ripple] |

  @WithSubgraphStrategy @GraphComputerVerificationStarGraphExceeded
  Scenario: g_withStrategiesXSubgraphStrategyXsubgraphBXX_VX4X_outV_outE
    Given the modern graph
    And using the parameter eid8 defined as "e[marko-knows->josh].id"
    And the traversal of
      """
      g.withStrategies(new SubgraphStrategy(edges: __.or(__.has("weight", 1.0).hasLabel("knows"),
                                                         __.has("weight", 0.4).hasLabel("created").outV().has("name", "marko"),
                                                         __.has("weight", 1.0).hasLabel("created")))).E(eid8).outV().outE()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | e[marko-knows->josh] |
      | e[marko-created->lop] |

  @WithSubgraphStrategy @GraphComputerVerificationStarGraphExceeded
  Scenario: g_withStrategiesXSubgraphStrategyXvertices_inXknowsX_hasXname_markoXXX_V_name
    Given the modern graph
    And the traversal of
      """
      g.withStrategies(new SubgraphStrategy(vertices: __.in("knows").has("name", "marko"))).V().values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | vadas |
      | josh |

  @WithSubgraphStrategy @GraphComputerVerificationStarGraphExceeded
  Scenario: g_withStrategiesXSubgraphStrategyXvertices_in_hasXname_markoXXX_V_name
    Given the modern graph
    And the traversal of
      """
      g.withStrategies(new SubgraphStrategy(vertices: __.in().has("name", "marko"))).V().values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | vadas |
      | josh |
      | lop |

  @WithSubgraphStrategy @GraphComputerVerificationStarGraphExceeded
  Scenario: g_withStrategiesXSubgraphStrategyXvertices_inXknowsX_whereXoutXcreatedX_hasXname_lopXXXX_V_name
    Given the modern graph
    And the traversal of
      """
      g.withStrategies(new SubgraphStrategy(vertices: __.in("knows").where(__.out("created").has("name", "lop")))).V().values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | vadas |
      | josh |

  @WithSubgraphStrategy @GraphComputerVerificationStarGraphExceeded
  Scenario: g_withStrategiesXSubgraphStrategyXvertices_in_hasXname_markoX_outXcreatedX_hasXname_lopXXXX_V_name
    Given the modern graph
    And the traversal of
      """
      g.withStrategies(new SubgraphStrategy(vertices: __.in().where(__.has("name", "marko").out("created").has("name", "lop")))).V().values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | vadas |
      | josh |
      | lop |

  @WithSubgraphStrategy @GraphComputerVerificationStarGraphExceeded
  Scenario: g_withStrategiesXSubgraphStrategyXvertices_orXboth_hasXname_markoX_hasXname_markoXXXX_V_name
    Given the modern graph
    And the traversal of
      """
      g.withStrategies(new SubgraphStrategy(vertices: __.or(__.both().has("name", "marko"), __.has("name", "marko")))).V().where(__.bothE().count().is(P.neq(0))).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko |
      | vadas |
      | josh |
      | lop |

  @WithSubgraphStrategy @GraphComputerVerificationStarGraphExceeded
  Scenario: g_withStrategiesXSubgraphStrategyXsubgraphCXX_V
    Given the modern graph
    And the traversal of
      """
      g.withStrategies(new SubgraphStrategy(vertices: __.has("name", P.within("josh", "lop", "ripple")),
                                            edges: __.or(__.has("weight", 0.4).hasLabel("created"),
                                                         __.has("weight", 1.0).hasLabel("created")))).V()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[lop] |
      | v[josh] |
      | v[ripple] |

  @WithSubgraphStrategy @GraphComputerVerificationStarGraphExceeded
  Scenario: g_withStrategiesXSubgraphStrategyXsubgraphCXX_E
    Given the modern graph
    And the traversal of
      """
      g.withStrategies(new SubgraphStrategy(vertices: __.has("name", P.within("josh", "lop", "ripple")),
                                            edges: __.or(__.has("weight", 0.4).hasLabel("created"),
                                                         __.has("weight", 1.0).hasLabel("created")))).E()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | e[josh-created->lop] |
      | e[josh-created->ripple] |

  @WithSubgraphStrategy @GraphComputerVerificationStarGraphExceeded
  Scenario: g_withStrategiesXSubgraphStrategyXsubgraphCXX_VX4X_outE
    Given the modern graph
    And using the parameter vid4 defined as "v[josh].id"
    And the traversal of
      """
      g.withStrategies(new SubgraphStrategy(vertices: __.has("name", P.within("josh", "lop", "ripple")),
                                            edges: __.or(__.has("weight", 0.4).hasLabel("created"),
                                                         __.has("weight", 1.0).hasLabel("created")))).V(vid4).outE()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | e[josh-created->lop] |
      | e[josh-created->ripple] |

  @WithSubgraphStrategy @GraphComputerVerificationStarGraphExceeded
  Scenario: g_withStrategiesXSubgraphStrategyXsubgraphCXX_VX4X_inE
    Given the modern graph
    And using the parameter vid4 defined as "v[josh].id"
    And the traversal of
      """
      g.withStrategies(new SubgraphStrategy(vertices: __.has("name", P.within("josh", "lop", "ripple")),
                                            edges: __.or(__.has("weight", 0.4).hasLabel("created"),
                                                         __.has("weight", 1.0).hasLabel("created")))).V(vid4).inE()
      """
    When iterated to list
    Then the result should be empty

  @WithSubgraphStrategy @GraphComputerVerificationStarGraphExceeded
  Scenario: g_withStrategiesXSubgraphStrategyXsubgraphCXX_VX4X_out
    Given the modern graph
    And using the parameter vid4 defined as "v[josh].id"
    And the traversal of
      """
      g.withStrategies(new SubgraphStrategy(vertices: __.has("name", P.within("josh", "lop", "ripple")),
                                            edges: __.or(__.has("weight", 0.4).hasLabel("created"),
                                                         __.has("weight", 1.0).hasLabel("created")))).V(vid4).out()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[lop] |
      | v[ripple] |

  @WithSubgraphStrategy @GraphComputerVerificationStarGraphExceeded
  Scenario: g_withStrategiesXSubgraphStrategyXsubgraphCXX_VX4X_in
    Given the modern graph
    And using the parameter vid4 defined as "v[josh].id"
    And the traversal of
      """
      g.withStrategies(new SubgraphStrategy(vertices: __.has("name", P.within("josh", "lop", "ripple")),
                                            edges: __.or(__.has("weight", 0.4).hasLabel("created"),
                                                         __.has("weight", 1.0).hasLabel("created")))).V(vid4).in()
      """
    When iterated to list
    Then the result should be empty

  @WithSubgraphStrategy @GraphComputerVerificationStarGraphExceeded
  Scenario: g_withStrategiesXSubgraphStrategyXsubgraphCXX_VX4X_both
    Given the modern graph
    And using the parameter vid4 defined as "v[josh].id"
    And the traversal of
      """
      g.withStrategies(new SubgraphStrategy(vertices: __.has("name", P.within("josh", "lop", "ripple")),
                                            edges: __.or(__.has("weight", 0.4).hasLabel("created"),
                                                         __.has("weight", 1.0).hasLabel("created")))).V(vid4).both()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[lop] |
      | v[ripple] |

  @WithSubgraphStrategy @GraphComputerVerificationStarGraphExceeded
  Scenario: g_withStrategiesXSubgraphStrategyXsubgraphCXX_VX4X_bothE
    Given the modern graph
    And using the parameter vid4 defined as "v[josh].id"
    And the traversal of
      """
      g.withStrategies(new SubgraphStrategy(vertices: __.has("name", P.within("josh", "lop", "ripple")),
                                            edges: __.or(__.has("weight", 0.4).hasLabel("created"),
                                                         __.has("weight", 1.0).hasLabel("created")))).V(vid4).bothE()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | e[josh-created->lop] |
      | e[josh-created->ripple] |

  @WithSubgraphStrategy @GraphComputerVerificationStarGraphExceeded
  Scenario: g_withStrategiesXSubgraphStrategyXsubgraphCXX_VX4X_localXbothE_limitX1XX
    Given the modern graph
    And using the parameter vid4 defined as "v[josh].id"
    And the traversal of
      """
      g.withStrategies(new SubgraphStrategy(vertices: __.has("name", P.within("josh", "lop", "ripple")),
                                            edges: __.or(__.has("weight", 0.4).hasLabel("created"),
                                                         __.has("weight", 1.0).hasLabel("created")))).V(vid4).local(__.bothE().limit(1))
      """
    When iterated to list
    Then the result should have a count of 1

  @WithSubgraphStrategy @GraphComputerVerificationStarGraphExceeded
  Scenario: g_withStrategiesXSubgraphStrategyXsubgraphCXX_EX11X_bothV
    Given the modern graph
    And using the parameter eid11 defined as "e[josh-created->lop].id"
    And the traversal of
      """
      g.withStrategies(new SubgraphStrategy(vertices: __.has("name", P.within("josh", "lop", "ripple")),
                                            edges: __.or(__.has("weight", 0.4).hasLabel("created"),
                                                         __.has("weight", 1.0).hasLabel("created")))).E(eid11).bothV()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[josh] |
      | v[lop] |

  @WithSubgraphStrategy @GraphComputerVerificationStarGraphExceeded
  Scenario: g_withStrategiesXSubgraphStrategyXsubgraphCXX_EX12X_bothV
    Given the modern graph
    And using the parameter eid12 defined as "e[peter-created->lop].id"
    And the traversal of
      """
      g.withStrategies(new SubgraphStrategy(vertices: __.has("name", P.within("josh", "lop", "ripple")),
                                            edges: __.or(__.has("weight", 0.4).hasLabel("created"),
                                                         __.has("weight", 1.0).hasLabel("created")))).E(eid12).bothV()
      """
    When iterated to list
    Then the result should be empty

  @WithSubgraphStrategy @GraphComputerVerificationStarGraphExceeded
  Scenario: g_withStrategiesXSubgraphStrategyXsubgraphCXX_EX9X_bothV
    Given the modern graph
    And using the parameter eid9 defined as "e[marko-created->lop].id"
    And the traversal of
      """
      g.withStrategies(new SubgraphStrategy(vertices: __.has("name", P.within("josh", "lop", "ripple")),
                                            edges: __.or(__.has("weight", 0.4).hasLabel("created"),
                                                         __.has("weight", 1.0).hasLabel("created")))).E(eid9).bothV()
      """
    When iterated to list
    Then the result should be empty

  @WithSubgraphStrategy @GraphComputerVerificationStarGraphExceeded
  Scenario: g_withStrategiesXSubgraphStrategyXvertices_hasXname_withinXripple_josh_markoXXX_V_asXaX_out_in_asXbX_dedupXa_bX_name
    Given the modern graph
    And the traversal of
      """
      g.withStrategies(new SubgraphStrategy(vertices: __.has("name", P.within("ripple", "josh", "marko")))).V().as("a").
        out().in().as("b").dedup("a", "b").values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko |
      | josh |

  @WithSubgraphStrategy @GraphComputerVerificationStarGraphExceeded @MultiMetaProperties
  Scenario: g_withStrategiesXSubgraphStrategyXvertexProperties_hasXstartTime_gtX2005XXXX_V_propertiesXlocationX_value
    Given the crew graph
    And the traversal of
      """
      g.withStrategies(new SubgraphStrategy(vertexProperties: __.has("startTime", P.gt(2005)))).V().properties("location").value()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | purcellville |
      | baltimore |
      | oakland |
      | seattle |
      | aachen |

  @WithSubgraphStrategy @GraphComputerVerificationStarGraphExceeded @MultiMetaProperties
  Scenario: g_withStrategiesXSubgraphStrategyXvertexProperties_hasXstartTime_gtX2005XXXX_V_valuesXlocationX
    Given the crew graph
    And the traversal of
      """
      g.withStrategies(new SubgraphStrategy(vertexProperties: __.has("startTime", P.gt(2005)))).V().values("location")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | purcellville |
      | baltimore |
      | oakland |
      | seattle |
      | aachen |

  @WithSubgraphStrategy @GraphComputerVerificationStarGraphExceeded @MultiMetaProperties
  Scenario: g_withStrategiesXSubgraphStrategyXvertexProperties_hasXstartTime_gtX2005XXXX_V_asXaX_propertiesXlocationX_asXbX_selectXaX_outE_properties_selectXbX_value_dedup
    Given the crew graph
    And the traversal of
      """
      g.withStrategies(new SubgraphStrategy(vertexProperties: __.has("startTime", P.gt(2005)))).V().as("a").
        properties("location").as("b").select("a").outE().properties().select("b").value().dedup()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | purcellville |
      | baltimore |
      | oakland |
      | seattle |
      | aachen |

  @WithSubgraphStrategy @GraphComputerVerificationStarGraphExceeded @MultiMetaProperties
  Scenario: g_withStrategiesXSubgraphStrategyXvertexProperties_hasXstartTime_gtX2005XXXX_V_asXaX_valuesXlocationX_asXbX_selectXaX_outE_properties_selectXbX_dedup
    Given the crew graph
    And the traversal of
      """
      g.withStrategies(new SubgraphStrategy(vertexProperties: __.has("startTime", P.gt(2005)))).V().as("a").
        values("location").as("b").select("a").outE().properties().select("b").dedup()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | purcellville |
      | baltimore |
      | oakland |
      | seattle |
      | aachen |

  @WithSubgraphStrategy @GraphComputerVerificationStarGraphExceeded @MultiMetaProperties
  Scenario: g_withStrategiesXSubgraphStrategyXvertices_hasXname_neqXstephenXX_vertexProperties_hasXstartTime_gtX2005XXXX_V_propertiesXlocationX_value
    Given the crew graph
    And the traversal of
      """
      g.withStrategies(new SubgraphStrategy(vertexProperties: __.has("startTime", P.gt(2005)),
                                            vertices: __.has("name", P.neq("stephen")))).V().properties("location").value()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | baltimore |
      | oakland |
      | seattle |
      | aachen |

  @WithSubgraphStrategy @GraphComputerVerificationStarGraphExceeded @MultiMetaProperties
  Scenario: g_withStrategiesXSubgraphStrategyXvertices_hasXname_neqXstephenXX_vertexProperties_hasXstartTime_gtX2005XXXX_V_valuesXlocationX
    Given the crew graph
    And the traversal of
      """
      g.withStrategies(new SubgraphStrategy(vertexProperties: __.has("startTime", P.gt(2005)),
                                            vertices: __.has("name", P.neq("stephen")))).V().values("location")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | baltimore |
      | oakland |
      | seattle |
      | aachen |

  @WithSubgraphStrategy @GraphComputerVerificationStarGraphExceeded @MultiMetaProperties
  Scenario: g_withStrategiesXSubgraphStrategyXedges_hasLabelXusesX_hasXskill_5XXX_V_outE_valueMap_selectXvaluesX_unfold
    Given the crew graph
    And the traversal of
      """
      g.withStrategies(new SubgraphStrategy(edges: __.hasLabel("uses").has("skill", 5))).V().
        outE().valueMap().select(Column.values).unfold()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[5].i |
      | d[5].i |
      | d[5].i |

  @WithSubgraphStrategy @GraphComputerVerificationStarGraphExceeded
  Scenario: g_withStrategiesXSubgraphStrategyXsubgraphDXX_V
    Given the modern graph
    And the traversal of
      """
      g.withStrategies(new SubgraphStrategy(checkAdjacentVertices: false,
                                            vertices: __.has("name", P.within("josh", "lop", "ripple")),
                                            edges: __.or(__.has("weight", 0.4).hasLabel("created"),
                                                         __.has("weight", 1.0).hasLabel("created")))).V()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[lop] |
      | v[josh] |
      | v[ripple] |

  @WithSubgraphStrategy @GraphComputerVerificationStarGraphExceeded
  Scenario: g_withStrategiesXSubgraphStrategyXsubgraphDXX_E
    Given the modern graph
    And the traversal of
      """
      g.withStrategies(new SubgraphStrategy(checkAdjacentVertices: false,
                                            vertices: __.has("name", P.within("josh", "lop", "ripple")),
                                            edges: __.or(__.has("weight", 0.4).hasLabel("created"),
                                                         __.has("weight", 1.0).hasLabel("created")))).E()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | e[marko-created->lop] |
      | e[josh-created->lop] |
      | e[josh-created->ripple] |

  @WithSubgraphStrategy @GraphComputerVerificationStarGraphExceeded
  Scenario: g_withStrategiesXSubgraphStrategyXsubgraphDXX_VX4X_outE
    Given the modern graph
    And using the parameter vid4 defined as "v[josh].id"
    And the traversal of
      """
      g.withStrategies(new SubgraphStrategy(checkAdjacentVertices: false,
                                            vertices: __.has("name", P.within("josh", "lop", "ripple")),
                                            edges: __.or(__.has("weight", 0.4).hasLabel("created"),
                                                         __.has("weight", 1.0).hasLabel("created")))).V(vid4).outE()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | e[josh-created->lop] |
      | e[josh-created->ripple] |

  @WithSubgraphStrategy @GraphComputerVerificationStarGraphExceeded
  Scenario: g_withStrategiesXSubgraphStrategyXsubgraphDXX_VX4X_inE
    Given the modern graph
    And using the parameter vid4 defined as "v[josh].id"
    And the traversal of
      """
      g.withStrategies(new SubgraphStrategy(checkAdjacentVertices: false,
                                            vertices: __.has("name", P.within("josh", "lop", "ripple")),
                                            edges: __.or(__.has("weight", 0.4).hasLabel("created"),
                                                         __.has("weight", 1.0).hasLabel("created")))).V(vid4).inE()
      """
    When iterated to list
    Then the result should be empty

  @WithSubgraphStrategy @GraphComputerVerificationStarGraphExceeded
  Scenario: g_withStrategiesXSubgraphStrategyXsubgraphDXX_VX4X_out
    Given the modern graph
    And using the parameter vid4 defined as "v[josh].id"
    And the traversal of
      """
      g.withStrategies(new SubgraphStrategy(checkAdjacentVertices: false,
                                            vertices: __.has("name", P.within("josh", "lop", "ripple")),
                                            edges: __.or(__.has("weight", 0.4).hasLabel("created"),
                                                         __.has("weight", 1.0).hasLabel("created")))).V(vid4).out()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[lop] |
      | v[ripple] |

  @WithSubgraphStrategy @GraphComputerVerificationStarGraphExceeded
  Scenario: g_withStrategiesXSubgraphStrategyXsubgraphDXX_VX4X_in
    Given the modern graph
    And using the parameter vid4 defined as "v[josh].id"
    And the traversal of
      """
      g.withStrategies(new SubgraphStrategy(checkAdjacentVertices: false,
                                            vertices: __.has("name", P.within("josh", "lop", "ripple")),
                                            edges: __.or(__.has("weight", 0.4).hasLabel("created"),
                                                         __.has("weight", 1.0).hasLabel("created")))).V(vid4).in()
      """
    When iterated to list
    Then the result should be empty

  @WithSubgraphStrategy @GraphComputerVerificationStarGraphExceeded
  Scenario: g_withStrategiesXSubgraphStrategyXsubgraphDXX_VX4X_both
    Given the modern graph
    And using the parameter vid4 defined as "v[josh].id"
    And the traversal of
      """
      g.withStrategies(new SubgraphStrategy(checkAdjacentVertices: false,
                                            vertices: __.has("name", P.within("josh", "lop", "ripple")),
                                            edges: __.or(__.has("weight", 0.4).hasLabel("created"),
                                                         __.has("weight", 1.0).hasLabel("created")))).V(vid4).both()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[lop] |
      | v[ripple] |

  @WithSubgraphStrategy @GraphComputerVerificationStarGraphExceeded
  Scenario: g_withStrategiesXSubgraphStrategyXsubgraphDXX_VX4X_bothE
    Given the modern graph
    And using the parameter vid4 defined as "v[josh].id"
    And the traversal of
      """
      g.withStrategies(new SubgraphStrategy(checkAdjacentVertices: false,
                                            vertices: __.has("name", P.within("josh", "lop", "ripple")),
                                            edges: __.or(__.has("weight", 0.4).hasLabel("created"),
                                                         __.has("weight", 1.0).hasLabel("created")))).V(vid4).bothE()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | e[josh-created->lop] |
      | e[josh-created->ripple] |

  @WithSubgraphStrategy @GraphComputerVerificationStarGraphExceeded
  Scenario: g_withStrategiesXSubgraphStrategyXsubgraphDXX_VX4X_localXbothE_limitX1XX
    Given the modern graph
    And using the parameter vid4 defined as "v[josh].id"
    And the traversal of
      """
      g.withStrategies(new SubgraphStrategy(checkAdjacentVertices: false,
                                            vertices: __.has("name", P.within("josh", "lop", "ripple")),
                                            edges: __.or(__.has("weight", 0.4).hasLabel("created"),
                                                         __.has("weight", 1.0).hasLabel("created")))).V(vid4).local(__.bothE().limit(1))
      """
    When iterated to list
    Then the result should have a count of 1

  @WithSubgraphStrategy @GraphComputerVerificationStarGraphExceeded
  Scenario: g_withStrategiesXSubgraphStrategyXsubgraphDXX_EX11X_bothV
    Given the modern graph
    And using the parameter eid11 defined as "e[josh-created->lop].id"
    And the traversal of
      """
      g.withStrategies(new SubgraphStrategy(checkAdjacentVertices: false,
                                            vertices: __.has("name", P.within("josh", "lop", "ripple")),
                                            edges: __.or(__.has("weight", 0.4).hasLabel("created"),
                                                         __.has("weight", 1.0).hasLabel("created")))).E(eid11).bothV()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[josh] |
      | v[lop] |

  @WithSubgraphStrategy @GraphComputerVerificationStarGraphExceeded
  Scenario: g_withStrategiesXSubgraphStrategyXsubgraphDXX_EX12X_bothV
    Given the modern graph
    And using the parameter eid12 defined as "e[peter-created->lop].id"
    And the traversal of
      """
      g.withStrategies(new SubgraphStrategy(checkAdjacentVertices: false,
                                            vertices: __.has("name", P.within("josh", "lop", "ripple")),
                                            edges: __.or(__.has("weight", 0.4).hasLabel("created"),
                                                         __.has("weight", 1.0).hasLabel("created")))).E(eid12).bothV()
      """
    When iterated to list
    Then the result should be empty

  @WithSubgraphStrategy @GraphComputerVerificationStarGraphExceeded
  Scenario: g_withStrategiesXSubgraphStrategyXsubgraphDXX_EX9X_bothV
    Given the modern graph
    And using the parameter eid9 defined as "e[marko-created->lop].id"
    And the traversal of
      """
      g.withStrategies(new SubgraphStrategy(checkAdjacentVertices: false,
                                            vertices: __.has("name", P.within("josh", "lop", "ripple")),
                                            edges: __.or(__.has("weight", 0.4).hasLabel("created"),
                                                         __.has("weight", 1.0).hasLabel("created")))).E(eid9).bothV()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[lop] |