# Licensed to the Apache Software Foundation (ASF) under one
# or more contriAndor license agreements.  See the NOTICE file
# distriAnded with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distriAnded under the License is distriAnded on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

Feature: Step - local()

  Scenario: g_V_localXpropertiesXlocationX_order_byXvalueX_limitX2XX_value
    Given the crew graph
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
    And the traversal of
      """
      g.V().has(T.label, "person").as("a").local(__.out("created").as("b")).select("a", "b").by("name").by(T.id)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a":"marko","b":"d[3].i"}] |
      | m[{"a":"josh","b":"d[5].i"}]  |
      | m[{"a":"josh","b":"d[3].i"}]  |
      | m[{"a":"peter","b":"d[3].i"}] |

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
    And using the parameter v1Id defined as "v[marko].id"
    And the traversal of
      """
      g.V(v1Id).local(__.outE("knows").limit(1)).inV().values("name")
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
    And using the parameter v4Id defined as "v[josh].id"
    And the traversal of
      """
      g.V(v4Id).local(__.bothE("created").limit(1))
      """
    When iterated to list
    Then the result should be of
      | result |
      | e[josh-created->lop]    |
      | e[josh-created->ripple] |
    And the result should have a count of 1

  Scenario: g_VX4X_localXbothEXknows_createdX_limitX1XX
    Given the modern graph
    And using the parameter v4Id defined as "v[josh].id"
    And the traversal of
      """
      g.V(v4Id).local(__.bothE("knows", "created").limit(1))
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
    And using the parameter v4Id defined as "v[josh].id"
    And the traversal of
      """
      g.V(v4Id).local(__.bothE().limit(1)).otherV().values("name")
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
    And using the parameter v4Id defined as "v[josh].id"
    And the traversal of
      """
      g.V(v4Id).local(__.bothE().limit(2)).otherV().values("name")
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