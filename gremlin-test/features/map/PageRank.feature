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
                
  Scenario: g_V_pageRank_hasXpageRankX
    Given the modern graph
    And the traversal of
      """
      g.withComputer().V().pageRank().has("gremlin.pageRankVertexProgram.pageRank")
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

  Scenario: g_V_outXcreatedX_pageRank_withXedges_bothEX_withXpropertyName_projectRankX_withXtimes_0X_valueMapXname_projectRankX
    Given the modern graph
    And the traversal of
      """
      g.withComputer().V().out("created").pageRank().with("~tinkerpop.pageRank.edges",__.bothE()).with("~tinkerpop.pageRank.propertyName","projectRank").with("~tinkerpop.pageRank.times",0).valueMap("name", "projectRank")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"name": ["lop"], "projectRank": [3.0]}] |
      | m[{"name": ["lop"], "projectRank": [3.0]}] |
      | m[{"name": ["lop"], "projectRank": [3.0]}] |
      | m[{"name": ["ripple"], "projectRank": [1.0]}] |

  Scenario: g_V_pageRank_order_byXpageRank_decrX_byXnameX_name
    Given the modern graph
    And the traversal of
      """
      g.withComputer().V().pageRank().order().by("gremlin.pageRankVertexProgram.pageRank", Order.decr).by("name").values("name")
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | lop    |
      | ripple |
      | josh   |
      | vadas  |
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

  Scenario: g_V_pageRank_withXedges_outEXknowsXX_withXpropertyName_friendRankX_project_byXnameX_byXvaluesXfriendRankX_mathX
    Given the modern graph
    And the traversal of
      """
      g.withComputer().V().pageRank().with("~tinkerpop.pageRank.edges",__.outE("knows")).with("~tinkerpop.pageRank.propertyName","friendRank").project("name", "friendRank").by("name").by(__.values("friendRank").math("ceil(_ * 100)"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"name": "marko", "friendRank": 15.0}] |
      | m[{"name": "vadas", "friendRank": 21.0}] |
      | m[{"name": "lop", "friendRank": 15.0}] |
      | m[{"name": "josh", "friendRank": 21.0}] |
      | m[{"name": "ripple", "friendRank": 15.0}] |
      | m[{"name": "peter", "friendRank": 15.0}] |

  Scenario: g_V_hasLabelXpersonX_pageRank_withXpropertyName_kpageRankX_project_byXnameX_byXvaluesXpageRankX_mathX
    Given the modern graph
    And the traversal of
      """
      g.withComputer().V().hasLabel("person").pageRank().with("~tinkerpop.pageRank.propertyName","pageRank").project("name", "pageRank").by("name").by(__.values("pageRank").math("ceil(_ * 100)"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"name": "marko", "pageRank": 46.0}] |
      | m[{"name": "vadas", "pageRank": 59.0}] |
      | m[{"name": "josh", "pageRank": 59.0}] |
      | m[{"name": "peter", "pageRank": 46.0}] |

  Scenario: g_V_pageRank_withXpropertyName_pageRankX_asXaX_outXknowsX_pageRank_asXbX_selectXa_bX_by_byXmathX
    Given the modern graph
    And the traversal of
      """
      g.withComputer().V().pageRank().with("~tinkerpop.pageRank.propertyName","pageRank").as("a").out("knows").values("pageRank").as("b").select("a", "b").by().by(__.math("ceil(_ * 100)"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a": "v[marko]", "b": 15.0}] |
      | m[{"a": "v[marko]", "b": 15.0}] |

  Scenario: g_V_hasLabelXsoftwareX_hasXname_rippleX_pageRankX1X_withXedges_inEXcreatedX_withXtimes_1X_withXpropertyName_priorsX_inXcreatedX_unionXboth__identityX_valueMapXname_priorsX
    Given the modern graph
    And the traversal of
      """
      g.withComputer().V().hasLabel("software").has("name", "ripple").pageRank(1.0).with("~tinkerpop.pageRank.edges",__.inE("created")).with("~tinkerpop.pageRank.times",1).with("~tinkerpop.pageRank.propertyName","priors").in("created").union(__.both(), __.identity()).valueMap("name", "priors")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"name": ["josh"], "priors": [1.0]}] |
      | m[{"name": ["marko"], "priors": [0.0]}] |
      | m[{"name": ["lop"], "priors": [0.0]}] |
      | m[{"name": ["ripple"], "priors": [0.0]}] |

  Scenario: g_V_outXcreatedX_groupXmX_byXlabelX_pageRankX1X_withXpropertyName_pageRankX_withXedges_inEX_withXtimes_1X_inXcreatedX_groupXmX_byXpageRankX_capXmX
    Given the modern graph
    And the traversal of
      """
      g.withComputer().V().out("created").group("m").by(T.label).pageRank(1.0).with("~tinkerpop.pageRank.propertyName", "pageRank").with("~tinkerpop.pageRank.edges",__.inE()).with("~tinkerpop.pageRank.times", 1).in("created").group("m").by("pageRank").cap("m")
      """
    When iterated next
    Then the result should have a count of 3