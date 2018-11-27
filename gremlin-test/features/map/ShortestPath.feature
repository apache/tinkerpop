# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License,Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS,WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND,either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

Feature: Step - shortestPath()

  Scenario: g_V_shortestPath
    Given the modern graph
    And the traversal of
      """
      g.withComputer().V().shortestPath()
      """
    When iterated to list
    Then the result should be unordered
      | result                                 |
      | p[v[josh],v[lop],v[peter]]             |
      | p[v[josh],v[lop]]                      |
      | p[v[josh],v[marko],v[vadas]]           |
      | p[v[josh],v[marko]]                    |
      | p[v[josh],v[ripple]]                   |
      | p[v[josh]]                             |
      | p[v[lop],v[josh],v[ripple]]            |
      | p[v[lop],v[josh]]                      |
      | p[v[lop],v[marko],v[vadas]]            |
      | p[v[lop],v[marko]]                     |
      | p[v[lop],v[peter]]                     |
      | p[v[lop]]                              |
      | p[v[marko],v[josh],v[ripple]]          |
      | p[v[marko],v[josh]]                    |
      | p[v[marko],v[lop],v[peter]]            |
      | p[v[marko],v[lop]]                     |
      | p[v[marko],v[vadas]]                   |
      | p[v[marko]]                            |
      | p[v[peter],v[lop],v[josh],v[ripple]]   |
      | p[v[peter],v[lop],v[josh]]             |
      | p[v[peter],v[lop],v[marko],v[vadas]]   |
      | p[v[peter],v[lop],v[marko]]            |
      | p[v[peter],v[lop]]                     |
      | p[v[peter]]                            |
      | p[v[ripple],v[josh],v[lop],v[peter]]   |
      | p[v[ripple],v[josh],v[lop]]            |
      | p[v[ripple],v[josh],v[marko],v[vadas]] |
      | p[v[ripple],v[josh],v[marko]]          |
      | p[v[ripple],v[josh]]                   |
      | p[v[ripple]]                           |
      | p[v[vadas],v[marko],v[josh],v[ripple]] |
      | p[v[vadas],v[marko],v[josh]]           |
      | p[v[vadas],v[marko],v[lop],v[peter]]   |
      | p[v[vadas],v[marko],v[lop]]            |
      | p[v[vadas],v[marko]]                   |
      | p[v[vadas]]                            |

  Scenario: g_V_both_dedup_shortestPath
    Given the modern graph
    And the traversal of
      """
      g.withComputer().V().both().dedup().shortestPath()
      """
    When iterated to list
    Then the result should be unordered
      | result                                 |
      | p[v[josh],v[lop],v[peter]]             |
      | p[v[josh],v[lop]]                      |
      | p[v[josh],v[marko],v[vadas]]           |
      | p[v[josh],v[marko]]                    |
      | p[v[josh],v[ripple]]                   |
      | p[v[josh]]                             |
      | p[v[lop],v[josh],v[ripple]]            |
      | p[v[lop],v[josh]]                      |
      | p[v[lop],v[marko],v[vadas]]            |
      | p[v[lop],v[marko]]                     |
      | p[v[lop],v[peter]]                     |
      | p[v[lop]]                              |
      | p[v[marko],v[josh],v[ripple]]          |
      | p[v[marko],v[josh]]                    |
      | p[v[marko],v[lop],v[peter]]            |
      | p[v[marko],v[lop]]                     |
      | p[v[marko],v[vadas]]                   |
      | p[v[marko]]                            |
      | p[v[peter],v[lop],v[josh],v[ripple]]   |
      | p[v[peter],v[lop],v[josh]]             |
      | p[v[peter],v[lop],v[marko],v[vadas]]   |
      | p[v[peter],v[lop],v[marko]]            |
      | p[v[peter],v[lop]]                     |
      | p[v[peter]]                            |
      | p[v[ripple],v[josh],v[lop],v[peter]]   |
      | p[v[ripple],v[josh],v[lop]]            |
      | p[v[ripple],v[josh],v[marko],v[vadas]] |
      | p[v[ripple],v[josh],v[marko]]          |
      | p[v[ripple],v[josh]]                   |
      | p[v[ripple]]                           |
      | p[v[vadas],v[marko],v[josh],v[ripple]] |
      | p[v[vadas],v[marko],v[josh]]           |
      | p[v[vadas],v[marko],v[lop],v[peter]]   |
      | p[v[vadas],v[marko],v[lop]]            |
      | p[v[vadas],v[marko]]                   |
      | p[v[vadas]]                            |

  Scenario: g_V_shortestPath_edgesIncluded
    Given the modern graph
    And the traversal of
      """
      g.withComputer().V().shortestPath().with("~tinkerpop.shortestPath.includeEdges")
      """
    When iterated to list
    Then the result should be unordered
      | result                                                                                                    |
      | p[v[josh],e[josh-created->lop],v[lop],e[peter-created->lop],v[peter]]                                     |
      | p[v[josh],e[josh-created->lop],v[lop]]                                                                    |
      | p[v[josh],e[josh-created->ripple],v[ripple]]                                                              |
      | p[v[josh],e[marko-knows->josh],v[marko],e[marko-knows->vadas],v[vadas]]                                   |
      | p[v[josh],e[marko-knows->josh],v[marko]]                                                                  |
      | p[v[josh]]                                                                                                |
      | p[v[lop],e[josh-created->lop],v[josh],e[josh-created->ripple],v[ripple]]                                  |
      | p[v[lop],e[josh-created->lop],v[josh]]                                                                    |
      | p[v[lop],e[marko-created->lop],v[marko],e[marko-knows->vadas],v[vadas]]                                   |
      | p[v[lop],e[marko-created->lop],v[marko]]                                                                  |
      | p[v[lop],e[peter-created->lop],v[peter]]                                                                  |
      | p[v[lop]]                                                                                                 |
      | p[v[marko],e[marko-created->lop],v[lop],e[peter-created->lop],v[peter]]                                   |
      | p[v[marko],e[marko-created->lop],v[lop]]                                                                  |
      | p[v[marko],e[marko-knows->josh],v[josh],e[josh-created->ripple],v[ripple]]                                |
      | p[v[marko],e[marko-knows->josh],v[josh]]                                                                  |
      | p[v[marko],e[marko-knows->vadas],v[vadas]]                                                                |
      | p[v[marko]]                                                                                               |
      | p[v[peter],e[peter-created->lop],v[lop],e[josh-created->lop],v[josh],e[josh-created->ripple],v[ripple]]   |
      | p[v[peter],e[peter-created->lop],v[lop],e[josh-created->lop],v[josh]]                                     |
      | p[v[peter],e[peter-created->lop],v[lop],e[marko-created->lop],v[marko],e[marko-knows->vadas],v[vadas]]    |
      | p[v[peter],e[peter-created->lop],v[lop],e[marko-created->lop],v[marko]]                                   |
      | p[v[peter],e[peter-created->lop],v[lop]]                                                                  |
      | p[v[peter]]                                                                                               |
      | p[v[ripple],e[josh-created->ripple],v[josh],e[josh-created->lop],v[lop],e[peter-created->lop],v[peter]]   |
      | p[v[ripple],e[josh-created->ripple],v[josh],e[josh-created->lop],v[lop]]                                  |
      | p[v[ripple],e[josh-created->ripple],v[josh],e[marko-knows->josh],v[marko],e[marko-knows->vadas],v[vadas]] |
      | p[v[ripple],e[josh-created->ripple],v[josh],e[marko-knows->josh],v[marko]]                                |
      | p[v[ripple],e[josh-created->ripple],v[josh]]                                                              |
      | p[v[ripple]]                                                                                              |
      | p[v[vadas],e[marko-knows->vadas],v[marko],e[marko-created->lop],v[lop],e[peter-created->lop],v[peter]]    |
      | p[v[vadas],e[marko-knows->vadas],v[marko],e[marko-created->lop],v[lop]]                                   |
      | p[v[vadas],e[marko-knows->vadas],v[marko],e[marko-knows->josh],v[josh],e[josh-created->ripple],v[ripple]] |
      | p[v[vadas],e[marko-knows->vadas],v[marko],e[marko-knows->josh],v[josh]]                                   |
      | p[v[vadas],e[marko-knows->vadas],v[marko]]                                                                |
      | p[v[vadas]]                                                                                               |

  Scenario: g_V_shortestPath_directionXINX
    Given the modern graph
    And the traversal of
      """
      g.withComputer().V().shortestPath().with("~tinkerpop.shortestPath.edges", Direction.IN)
      """
    When iterated to list
    Then the result should be unordered
      | result                        |
      | p[v[josh],v[marko]]           |
      | p[v[josh]]                    |
      | p[v[lop],v[josh]]             |
      | p[v[lop],v[marko]]            |
      | p[v[lop],v[peter]]            |
      | p[v[lop]]                     |
      | p[v[marko]]                   |
      | p[v[peter]]                   |
      | p[v[ripple],v[josh],v[marko]] |
      | p[v[ripple],v[josh]]          |
      | p[v[ripple]]                  |
      | p[v[vadas],v[marko]]          |
      | p[v[vadas]]                   |

  Scenario: g_V_shortestPath_edgesXoutEX
    Given the modern graph
    And the traversal of
      """
      g.withComputer().V().shortestPath().with("~tinkerpop.shortestPath.edges", __.outE())
      """
    When iterated to list
    Then the result should be unordered
      | result                        |
      | p[v[josh],v[lop]]             |
      | p[v[josh],v[ripple]]          |
      | p[v[josh]]                    |
      | p[v[lop]]                     |
      | p[v[marko],v[josh],v[ripple]] |
      | p[v[marko],v[josh]]           |
      | p[v[marko],v[lop]]            |
      | p[v[marko],v[vadas]]          |
      | p[v[marko]]                   |
      | p[v[peter],v[lop]]            |
      | p[v[peter]]                   |
      | p[v[ripple]]                  |
      | p[v[vadas]]                   |

  Scenario: g_V_shortestPath_edgesIncluded_edgesXoutEX
    Given the modern graph
    And the traversal of
      """
      g.withComputer().V().shortestPath().
          with("~tinkerpop.shortestPath.includeEdges").
          with("~tinkerpop.shortestPath.edges", __.outE())
      """
    When iterated to list
    Then the result should be unordered
      | result                                                                     |
      | p[v[josh],e[josh-created->lop],v[lop]]                                     |
      | p[v[josh],e[josh-created->ripple],v[ripple]]                               |
      | p[v[josh]]                                                                 |
      | p[v[lop]]                                                                  |
      | p[v[marko],e[marko-created->lop],v[lop]]                                   |
      | p[v[marko],e[marko-knows->josh],v[josh],e[josh-created->ripple],v[ripple]] |
      | p[v[marko],e[marko-knows->josh],v[josh]]                                   |
      | p[v[marko],e[marko-knows->vadas],v[vadas]]                                 |
      | p[v[marko]]                                                                |
      | p[v[peter],e[peter-created->lop],v[lop]]                                   |
      | p[v[peter]]                                                                |
      | p[v[ripple]]                                                               |
      | p[v[vadas]]                                                                |

  Scenario: g_V_hasXname_markoX_shortestPath
    Given the modern graph
    And the traversal of
      """
      g.withComputer().V().has("name","marko").shortestPath()
      """
    When iterated to list
    Then the result should be unordered
      | result                        |
      | p[v[marko],v[josh],v[ripple]] |
      | p[v[marko],v[josh]]           |
      | p[v[marko],v[lop],v[peter]]   |
      | p[v[marko],v[lop]]            |
      | p[v[marko],v[vadas]]          |
      | p[v[marko]]                   |

  Scenario: g_V_shortestPath_targetXhasXname_markoXX
    Given the modern graph
    And the traversal of
      """
      g.withComputer().V().shortestPath().with("~tinkerpop.shortestPath.target", __.has("name","marko"))
      """
    When iterated to list
    Then the result should be unordered
      | result                        |
      | p[v[josh],v[marko]]           |
      | p[v[lop],v[marko]]            |
      | p[v[marko]]                   |
      | p[v[peter],v[lop],v[marko]]   |
      | p[v[ripple],v[josh],v[marko]] |
      | p[v[vadas],v[marko]]          |

  Scenario: g_V_shortestPath_targetXvaluesXnameX_isXmarkoXX
    Given the modern graph
    And the traversal of
      """
      g.withComputer().V().shortestPath().with("~tinkerpop.shortestPath.target", __.values("name").is("marko"))
      """
    When iterated to list
    Then the result should be unordered
      | result                        |
      | p[v[josh],v[marko]]           |
      | p[v[lop],v[marko]]            |
      | p[v[marko]]                   |
      | p[v[peter],v[lop],v[marko]]   |
      | p[v[ripple],v[josh],v[marko]] |
      | p[v[vadas],v[marko]]          |

  Scenario: g_V_hasXname_markoX_shortestPath_targetXhasLabelXsoftwareXX
    Given the modern graph
    And the traversal of
      """
      g.withComputer().V().has("name","marko").shortestPath().with("~tinkerpop.shortestPath.target", __.hasLabel("software"))
      """
    When iterated to list
    Then the result should be unordered
      | result                        |
      | p[v[marko],v[josh],v[ripple]] |
      | p[v[marko],v[lop]]            |

  Scenario: g_V_hasXname_markoX_shortestPath_targetXhasXname_joshXX_distanceXweightX
    Given the modern graph
    And the traversal of
      """
      g.withComputer().V().has("name","marko").shortestPath().
          with("~tinkerpop.shortestPath.target", __.has("name","josh")).
          with("~tinkerpop.shortestPath.distance", "weight")
      """
    When iterated to list
    Then the result should be unordered
      | result                     |
      | p[v[marko],v[lop],v[josh]] |

  Scenario: g_V_hasXname_danielX_shortestPath_targetXhasXname_stephenXX_edgesXbothEXusesXX
    Given the crew graph
    And the traversal of
      """
      g.withComputer().V().has("name","daniel").shortestPath().
          with("~tinkerpop.shortestPath.target", __.has("name","stephen")).
          with("~tinkerpop.shortestPath.edges", __.bothE("uses"))
      """
    When iterated to list
    Then the result should be unordered
      | result                                 |
      | p[v[daniel],v[gremlin],v[stephen]]     |
      | p[v[daniel],v[tinkergraph],v[stephen]] |

  Scenario: g_V_hasXsong_name_MIGHT_AS_WELLX_shortestPath_targetXhasXsong_name_MAYBE_YOU_KNOW_HOW_I_FEELXX_edgesXoutEXfollowedByXX_distanceXweightX
    Given the grateful graph
    And the traversal of
      """
      g.withComputer().V().has("song","name","MIGHT AS WELL").
        shortestPath().
          with("~tinkerpop.shortestPath.target", __.has("song","name","MAYBE YOU KNOW HOW I FEEL")).
          with("~tinkerpop.shortestPath.edges", __.outE("followedBy")).
          with("~tinkerpop.shortestPath.distance", "weight")
      """
    When iterated to list
    Then the result should be unordered
      | result                                                            |
      | p[v[MIGHT AS WELL],v[DRUMS],v[MAYBE YOU KNOW HOW I FEEL]]         |
      | p[v[MIGHT AS WELL],v[SHIP OF FOOLS],v[MAYBE YOU KNOW HOW I FEEL]] |

  Scenario: g_V_hasXname_markoX_shortestPath_maxDistanceX1X
    Given the modern graph
    And the traversal of
      """
      g.withComputer().V().has("name","marko").shortestPath().with("~tinkerpop.shortestPath.maxDistance", 1)
      """
    When iterated to list
    Then the result should be unordered
      | result               |
      | p[v[marko],v[josh]]  |
      | p[v[marko],v[lop]]   |
      | p[v[marko],v[vadas]] |
      | p[v[marko]]          |

  Scenario: g_V_hasXname_vadasX_shortestPath_distanceXweightX_maxDistanceX1_3X
    Given the modern graph
    And the traversal of
      """
      g.withComputer().V().has("name","vadas").shortestPath().
          with("~tinkerpop.shortestPath.distance", "weight").
          with("~tinkerpop.shortestPath.maxDistance", 1.3)
      """
    When iterated to list
    Then the result should be unordered
      | result                               |
      | p[v[vadas],v[marko],v[lop],v[josh]]  |
      | p[v[vadas],v[marko],v[lop],v[peter]] |
      | p[v[vadas],v[marko],v[lop]]          |
      | p[v[vadas],v[marko]]                 |
      | p[v[vadas]]                          |
