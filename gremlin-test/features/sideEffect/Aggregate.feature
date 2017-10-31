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

Feature: Step - aggregate()

  Scenario: g_V_valueXnameX_aggregateXxX_capXxX
    Given the modern graph
    And the traversal of
      """
      g.V().values("name").aggregate("x").cap("x")
      """
    When iterated next
    Then the result should be unordered
      | result |
      | marko |
      | josh |
      | peter |
      | lop |
      | vadas |
      | ripple |

  Scenario: g_V_aggregateXxX_byXnameX_capXxX
    Given the modern graph
    And the traversal of
      """
      g.V().aggregate("x").by("name").cap("x")
      """
    When iterated next
    Then the result should be unordered
      | result |
      | marko |
      | josh |
      | peter |
      | lop |
      | vadas |
      | ripple |

  Scenario: g_V_out_aggregateXaX_path
    Given the modern graph
    And the traversal of
      """
      g.V().out().aggregate("a").path()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | p[v[marko],v[lop]] |
      | p[v[marko],v[vadas]] |
      | p[v[marko],v[josh]] |
      | p[v[josh],v[ripple]] |
      | p[v[josh],v[lop]] |
      | p[v[peter],v[lop]] |

  Scenario: g_V_hasLabelXpersonX_aggregateXxX_byXageX_capXxX_asXyX_selectXyX
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("person").aggregate("x").by("age").cap("x").as("y").select("y")
      """
    When iterated next
    Then the result should be unordered
      | result |
      | d[29] |
      | d[27] |
      | d[32] |
      | d[35] |