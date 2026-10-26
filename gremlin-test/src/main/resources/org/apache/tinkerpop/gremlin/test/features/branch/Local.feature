# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

@StepClassBranch @StepLocal
Feature: Step - local()

  @MultiProperties @MetaProperties
  Scenario: g_V_localXpropertiesXlocationX_order_byXvalueX_limitX2XX_value
    Given the crew graph
    And the intent
      """
      For each person, find the two places they have lived whose names come first alphabetically, and return those place
      names.
      """
    And the traversal of
      """
      g.V().local(__.properties("location").order().by(T.value, Order.asc).range(0, 2)).value()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | brussels       |
      | san diego      |
      | centreville    |
      | dulles         |
      | baltimore      |
      | bremen         |
      | aachen         |
      | kaiserslautern |

  Scenario: g_V_hasXlabel_personX_asXaX_localXoutXcreatedX_asXbXX_selectXa_bX_byXnameX_byXidX
    Given the modern graph
    And the intent
      """
      For each person, pair them with each thing they created, as a map of the person's name ("a") and the id of what
      they created ("b").
      """
    And the traversal of
      """
      g.V().has(T.label, "person").as("a").local(__.out("created").as("b")).select("a", "b").by("name").by(T.id)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a":"marko","b":"v[lop].id"}] |
      | m[{"a":"josh","b":"v[ripple].id"}]  |
      | m[{"a":"josh","b":"v[lop].id"}]  |
      | m[{"a":"peter","b":"v[lop].id"}] |

  Scenario: g_V_localXoutE_countX
    Given the modern graph
    And the traversal of
      """
      g.V().local(__.outE().count())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[3].l |
      | d[0].l |
      | d[0].l |
      | d[2].l |
      | d[0].l |
      | d[1].l |

  Scenario: g_VX1X_localXoutEXknowsX_limitX1XX_inV_name
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).local(__.outE("knows").limit(1)).inV().values("name")
      """
    When iterated to list
    Then the result should be of
      | result |
      | vadas |
      | josh  |
    And the result should have a count of 1

  Scenario: g_V_localXbothEXcreatedX_limitX1XX_otherV_name
    Given the modern graph
    And the traversal of
      """
      g.V().local(__.bothE("created").limit(1)).otherV().values("name")
      """
    When iterated to list
    Then the result should be of
      | result |
      | marko  |
      | lop    |
      | ripple |
      | josh   |
      | peter  |
    And the result should have a count of 5

  Scenario: g_VX4X_localXbothEX1_createdX_limitX1XX
    Given the modern graph
    And using the parameter vid4 defined as "v[josh].id"
    And the traversal of
      """
      g.V(vid4).local(__.bothE("created").limit(1))
      """
    When iterated to list
    Then the result should be of
      | result |
      | e[josh-created->lop]    |
      | e[josh-created->ripple] |
    And the result should have a count of 1

  Scenario: g_VX4X_localXbothEXknows_createdX_limitX1XX
    Given the modern graph
    And using the parameter vid4 defined as "v[josh].id"
    And the traversal of
      """
      g.V(vid4).local(__.bothE("knows", "created").limit(1))
      """
    When iterated to list
    Then the result should be of
      | result |
      | e[marko-knows->josh]    |
      | e[josh-created->lop]    |
      | e[josh-created->ripple] |
    And the result should have a count of 1

  Scenario: g_VX4X_localXbothE_limitX1XX_otherV_name
    Given the modern graph
    And using the parameter vid4 defined as "v[josh].id"
    And the traversal of
      """
      g.V(vid4).local(__.bothE().limit(1)).otherV().values("name")
      """
    When iterated to list
    Then the result should be of
      | result |
      | marko  |
      | ripple |
      | lop    |
    And the result should have a count of 1

  Scenario: g_VX4X_localXbothE_limitX2XX_otherV_name
    Given the modern graph
    And using the parameter vid4 defined as "v[josh].id"
    And the traversal of
      """
      g.V(vid4).local(__.bothE().limit(2)).otherV().values("name")
      """
    When iterated to list
    Then the result should be of
      | result |
      | marko  |
      | ripple |
      | lop    |
    And the result should have a count of 2

  Scenario: g_V_localXinEXknowsX_limitX2XX_outV_name
    Given the modern graph
    And the traversal of
      """
      g.V().local(__.inE("knows").limit(2)).outV().values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko |
      | marko |

  @GraphComputerVerificationStarGraphExceeded
  Scenario: g_V_localXmatchXproject__created_person__person_name_nameX_selectXname_projectX_by_byXnameX
    Given the modern graph
    And the traversal of
      """
      g.V().local(__.match(
                    __.as("project").in("created").as("person"),
                    __.as("person").values("name").as("name"))).select("name", "project").by().by("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"name":"marko","project":"lop"}]   |
      | m[{"name":"josh","project":"lop"}]    |
      | m[{"name":"peter","project":"lop"}]   |
      | m[{"name":"josh","project":"ripple"}] |

  # Barrier should show that bulked traversers don't affect local() traversal

  Scenario: g_V_in_barrier_localXcountX
    Given the modern graph
    And the traversal of
      """
      g.V().in().barrier().local(__.count())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[1].l |
      | d[1].l |
      | d[1].l |
      | d[1].l |
      | d[1].l |
      | d[1].l |

  # Path of traversers isn't hidden in local

  @GraphComputerVerificationStarGraphExceeded
  Scenario: g_V_localXout_in_simplePathX_path
    Given the modern graph
    And the intent
      """
      For each vertex, find the other vertices that share an outgoing neighbour with it, and return each as a path from
      the vertex, through the shared neighbour, to the other vertex.
      """
    And the traversal of
      """
      g.V().local(__.out().in().simplePath()).path()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | p[v[josh],v[lop],v[marko]] |
      | p[v[josh],v[lop],v[peter]] |
      | p[v[marko],v[lop],v[josh]] |
      | p[v[marko],v[lop],v[peter]] |
      | p[v[peter],v[lop],v[marko]] |
      | p[v[peter],v[lop],v[josh]] |
  
  # Traverser's sack value should be carried over from local traversal

  Scenario: g_withSackX0LX_V_in_barrier_localXsackXsumX_byXageXX_sack
    Given the modern graph
    And the traversal of
      """
      g.withSack(0L).V().in().barrier().local(__.sack(sum).by("age")).sack()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[29].l |
      | d[29].l |
      | d[29].l |
      | d[32].l |
      | d[32].l |
      | d[35].l |

  # Nested local should return proper local count

  Scenario: g_V_localXout_localXcountXX
    Given the modern graph
    And the traversal of
      """
      g.V().local(__.out().local(__.count()))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[1].l |
      | d[1].l |
      | d[1].l |
      | d[1].l |
      | d[1].l |
      | d[1].l |

  # Local should be applied to union's global child

  Scenario: g_V_unionXoutE_count_localXinE_countXX
    Given the modern graph
    And the traversal of
      """
      g.V().union(__.outE().count(), __.local(inE().count()))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[6].l |
      | d[0].l |
      | d[1].l |
      | d[3].l |
      | d[1].l |
      | d[1].l |
      | d[0].l |

  Scenario: g_V_localXunionXidentity_bothE_countX_foldX
    Given the modern graph
    And the intent
      """
      For each vertex, return a two-item list of the vertex and the number of edges touching it in either direction.
      """
    And the traversal of
      """
      g.V().local(union(identity(),bothE().count()).fold())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[v[marko],d[3].l] |
      | l[v[vadas],d[1].l] |
      | l[v[lop],d[3].l] |
      | l[v[josh],d[3].l] |
      | l[v[ripple],d[1].l] |
      | l[v[peter],d[1].l] |

  Scenario: g_V_asXpersonX_localXpropertiesXlocationX_order_byXstartTime_ascX_limitX2XX_value_asXlocationX_selectXperson_locationX_byXnameX_by
    Given the crew graph
    And the intent
      """
      For every person, find the two places where they lived earliest, by the start time of their stay, and return a map
      of the person's name ("person") and the place ("location") for each.
      """
    And the traversal of
      """
      g.V().as("person").local(properties("location").order().by("startTime",Order.asc).limit(2)).value().as("location").select("person","location").by("name").by()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"person":"marko","location":"san diego"}] |
      | m[{"person":"marko","location":"santa cruz"}] |
      | m[{"person":"stephen","location":"centreville"}] |
      | m[{"person":"stephen","location":"dulles"}] |
      | m[{"person":"matthias","location":"bremen"}] |
      | m[{"person":"matthias","location":"baltimore"}] |
      | m[{"person":"daniel","location":"spremberg"}] |
      | m[{"person":"daniel","location":"kaiserslautern"}] |

  Scenario: g_V_hasXname_markoX_unionXprojectXdegreeX_byXbothE_countX_valueMap_withXtokensXX_unfold_group_byXkeysX_byXselectXvaluesX_unfoldX
    Given the modern graph
    And the intent
      """
      Return one map for marko holding his id, his label, each of his properties as a single value, and his number of
      edges in either direction under "degree".
      """
    And the traversal of
      """
      g.V().has("name","marko").union(project("degree").by(bothE().count()),valueMap().with(WithOptions.tokens)).unfold().group().by(Column.keys).by(select(Column.values).unfold())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"degree":"d[3].l","t[id]":"v[marko].id","t[label]":"person","name":"marko","age":"d[29].i"}] |
