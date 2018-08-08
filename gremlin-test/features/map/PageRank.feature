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

Feature: Step - pageRank()
                
  Scenario: g_V_pageRank
    Given the modern graph
    And the traversal of
      """
      g.withComputer().V().pageRank()
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
    And the graph should return 6 for count of "g.withComputer().V().pageRank().has(\"gremlin.pageRankVertexProgram.pageRank\")"

  Scenario: g_V_outXcreatedX_pageRank_byXbothEX_byXprojectRankX_timesX0X_valueMapXname_projectRankX
    Given the modern graph
    And the traversal of
      """
      g.withComputer().V().out("created").pageRank().by(__.bothE()).by("projectRank").times(0).valueMap("name", "projectRank")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"name": ["lop"], "projectRank": [3.0]}] |
      | m[{"name": ["lop"], "projectRank": [3.0]}] |
      | m[{"name": ["lop"], "projectRank": [3.0]}] |
      | m[{"name": ["ripple"], "projectRank": [1.0]}] |

  # can't fully assert order here because some ranks are equivalent. the java test does an "or" type assert to deal
  # with this, which we can't express here in these tests. should probably change the test or
  Scenario: g_V_pageRank_order_byXpageRank_decrX_name
    Given the modern graph
    And the traversal of
      """
      g.withComputer().V().pageRank().order().by("gremlin.pageRankVertexProgram.pageRank", Order.decr).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | lop    |
      | ripple |
      | vadas  |
      | josh   |
      | marko  |
      | peter  |

  Scenario: g_V_pageRank_order_byXpageRank_decrX_name_limitX2X
    Given the modern graph
    And the traversal of
      """
      g.withComputer().V().pageRank().order().by("gremlin.pageRankVertexProgram.pageRank", Order.decr).values("name").limit(2)
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | lop    |
      | ripple |

  Scenario: g_V_pageRank_byXoutEXknowsXX_byXfriendRankX_valueMapXname_friendRankX
    Given the modern graph
    And the traversal of
      """
      g.withComputer().V().pageRank().by(__.outE("knows")).by("friendRank").valueMap("name", "friendRank")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"name": ["marko"], "friendRank": [0.15000000000000002]}] |
      | m[{"name": ["vadas"], "friendRank": [0.21375000000000002]}] |
      | m[{"name": ["lop"], "friendRank": [0.15000000000000002]}] |
      | m[{"name": ["josh"], "friendRank": [0.21375000000000002]}] |
      | m[{"name": ["ripple"], "friendRank": [0.15000000000000002]}] |
      | m[{"name": ["peter"], "friendRank": [0.15000000000000002]}] |

  Scenario: g_V_hasLabelXpersonX_pageRank_byXpageRankX_order_byXpageRankX_valueMapXname_pageRankX
    Given the modern graph
    And the traversal of
      """
      g.withComputer().V().hasLabel("person").pageRank().by("pageRank").order().by("pageRank").valueMap("name", "pageRank")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"name": ["marko"], "pageRank": [0.15000000000000002]}] |
      | m[{"name": ["vadas"], "pageRank": [0.19250000000000003]}] |
      | m[{"name": ["josh"], "pageRank": [0.19250000000000003]}] |
      | m[{"name": ["peter"], "pageRank": [0.15000000000000002]}] |

  Scenario: g_V_pageRank_byXpageRankX_asXaX_outXknowsX_pageRank_asXbX_selectXa_bX
    Given the modern graph
    And the traversal of
      """
      g.withComputer().V().pageRank().by("pageRank").as("a").out("knows").values("pageRank").as("b").select("a", "b")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a": "v[marko]", "b": 0.19250000000000003}] |
      | m[{"a": "v[marko]", "b": 0.19250000000000003}] |

  Scenario: g_V_hasLabelXsoftwareX_hasXname_rippleX_pageRankX1X_byXinEXcreatedXX_timesX1X_byXpriorsX_inXcreatedX_unionXboth__identityX_valueMapXname_priorsX
    Given the modern graph
    And the traversal of
      """
      g.withComputer().V().hasLabel("software").has("name", "ripple").pageRank(1.0).by(__.inE("created")).times(1).by("priors").in("created").union(__.both(), __.identity()).valueMap("name", "priors")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"name": ["josh"], "priors": [1.0]}] |
      | m[{"name": ["marko"], "priors": [0.0]}] |
      | m[{"name": ["lop"], "priors": [0.0]}] |
      | m[{"name": ["ripple"], "priors": [0.0]}] |

  Scenario: g_V_outXcreatedX_groupXmX_byXlabelX_pageRankX1X_byXpageRankX_byXinEX_timesX1X_inXcreatedX_groupXmX_byXpageRankX_capXmX()
    Given an unsupported test
    Then nothing should happen because
      """
      The result returned is not supported under GraphSON 2.x and therefore cannot be properly asserted. More
      specifically it has long keys which basically get toString()'d under GraphSON 2.x. This test can be supported
      with GraphSON 3.x.
      """
