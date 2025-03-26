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

@StepClassMap @StepVertex
Feature: Step - V(), out(), in(), both(), inE(), outE(), bothE()

  Scenario: g_VXnullX
    Given the modern graph
    And the traversal of
      """
      g.V(null)
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_VXlistXnullXX
    Given the modern graph
    And using the parameter xx1 defined as "l[null]"
    And the traversal of
      """
      g.V(xx1)
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_VX1_nullX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1,null)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |

  Scenario: g_VXlistX1_2_3XX_name
    Given the modern graph
    And using the parameter xx1 defined as "l[v[marko].id,v[vadas].id,v[lop].id]"
    And the traversal of
      """
      g.V(xx1).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko |
      | vadas |
      | lop |

  Scenario: g_VXlistXv1_v2_v3XX_name
    Given the modern graph
    And using the parameter xx1 defined as "l[v[marko],v[vadas],v[lop]]"
    And the traversal of
      """
      g.V(xx1).values("name")
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

  Scenario: g_VXv1X_out
    Given the modern graph
    And using the parameter v1 defined as "v[marko]"
    And the traversal of
      """
      g.V(v1).out()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[vadas] |
      | v[lop] |
      | v[josh] |

  Scenario: g_VX1X_out
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).out()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[vadas] |
      | v[lop] |
      | v[josh] |

  Scenario: g_VX2X_in
    Given the modern graph
    And using the parameter vid2 defined as "v[vadas].id"
    And the traversal of
      """
      g.V(vid2).in()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |

  Scenario: g_VX4X_both
    Given the modern graph
    And using the parameter vid4 defined as "v[josh].id"
    And the traversal of
      """
      g.V(vid4).both()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |
      | v[lop] |
      | v[ripple] |

  Scenario: g_VX1X_outE
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).outE()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | e[marko-created->lop] |
      | e[marko-knows->josh] |
      | e[marko-knows->vadas] |

  Scenario: g_VX2X_outE
    Given the modern graph
    And using the parameter vid2 defined as "v[vadas].id"
    And the traversal of
      """
      g.V(vid2).inE()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | e[marko-knows->vadas] |

  Scenario: g_VX4X_bothEXcreatedX
    Given the modern graph
    And using the parameter vid4 defined as "v[josh].id"
    And the traversal of
      """
      g.V(vid4).bothE("created")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | e[josh-created->lop] |
      | e[josh-created->ripple] |

  Scenario: g_VX4X_bothEXcreatedvarX
    Given the modern graph
    And using the parameter vid4 defined as "v[josh].id"
    And using the parameter xx1 defined as "created"
    And the traversal of
      """
      g.V(vid4).bothE(xx1)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | e[josh-created->lop] |
      | e[josh-created->ripple] |

  Scenario: g_VX4X_bothE
    Given the modern graph
    And using the parameter vid4 defined as "v[josh].id"
    And the traversal of
      """
      g.V(vid4).bothE()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | e[josh-created->lop] |
      | e[josh-created->ripple] |
      | e[marko-knows->josh] |

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

  Scenario: g_VX2X_inE
    Given the modern graph
    And using the parameter vid2 defined as "v[vadas].id"
    And the traversal of
    """
      g.V(vid2).bothE()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | e[marko-knows->vadas] |

  Scenario: g_VX1X_outXknowsX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).out("knows")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[vadas] |
      | v[josh] |

  Scenario: g_VX1AsStringX_outXknowsX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].sid"
    And the traversal of
      """
      g.V(vid1).out("knows")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[vadas] |
      | v[josh] |

  Scenario: g_VX1X_outXknows_createdX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).out("knows","created")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[vadas] |
      | v[josh] |
      | v[lop] |

  Scenario: g_VX1X_outXknowsvar_createdvarX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And using the parameter xx2 defined as "knows"
    And using the parameter xx3 defined as "created"
    And the traversal of
      """
      g.V(vid1).out(xx2,xx3)
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
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).out().out().out()
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_VX1X_out_name
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).out().values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | vadas |
      | josh |
      | lop |

  Scenario: g_VX1X_to_XOUT_knowsX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).to(Direction.OUT, "knows")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[vadas] |
      | v[josh] |

  # the point here is to test g.V() where an id is not present. to do this with the gherkin structure
  # the test establishes the modern graph, does a drop() of "lop" and then tries to query the 4 vertices
  # and we assert the count of 3
  Scenario: g_VX1_2_3_4X_name
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", "marko").property("age", 29).as("marko").
        addV("person").property("name", "vadas").property("age", 27).as("vadas").
        addV("software").property("name", "lop").property("lang", "java").as("lop").
        addV("person").property("name","josh").property("age", 32).as("josh").
        addV("software").property("name", "ripple").property("lang", "java").as("ripple").
        addV("person").property("name", "peter").property("age", 35).as('peter').
        addE("knows").from("marko").to("vadas").property("weight", 0.5d).
        addE("knows").from("marko").to("josh").property("weight", 1.0d).
        addE("created").from("marko").to("lop").property("weight", 0.4d).
        addE("created").from("josh").to("ripple").property("weight", 1.0d).
        addE("created").from("josh").to("lop").property("weight", 0.4d).
        addE("created").from("peter").to("lop").property("weight", 0.2d)
      """
    And using the parameter vid1 defined as "v[marko].id"
    And using the parameter vid2 defined as "v[vadas].id"
    And using the parameter vid3 defined as "v[lop].id"
    And using the parameter vid4 defined as "v[josh].id"
    And the traversal of
      """
      g.V().has('software','name','lop').drop()
      """
    When iterated to list
    Then the result should be empty
    And the graph should return 3 for count of "g.V(vid1, vid2, vid3, vid4)"

  @GraphComputerVerificationMidVNotSupported
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

  @GraphComputerVerificationInjectionNotSupported @GraphComputerVerificationMidVNotSupported
  Scenario: g_injectX1X_VXnullX
    Given the modern graph
    And the traversal of
      """
      g.inject(1).V(null)
      """
    When iterated to list
    Then the result should be empty

  @GraphComputerVerificationInjectionNotSupported @GraphComputerVerificationMidVNotSupported
  Scenario: g_injectX1X_VX1_nullX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.inject(1).V(vid1,null)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |