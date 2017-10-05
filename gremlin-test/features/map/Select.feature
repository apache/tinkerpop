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

Feature: Step - select()

  Scenario: get_g_VX1X_asXaX_outXknowsX_asXbX_selectXa_bX
    Given the modern graph
    And using the parameter v1Id is "v[marko].id"
    And the traversal of
      """
      g.V(v1Id).as("a").out("knows").as("b").select("a", "b")
      """
    When iterated to list
    Then the result should be unordered
      | m[{"a": "v[marko]", "b": "v[vadas]"}] |
      | m[{"a": "v[marko]", "b": "v[josh]"}] |

  Scenario: g_VX1X_asXaX_outXknowsX_asXbX_selectXa_bX_byXnameX
    Given the modern graph
    And using the parameter v1Id is "v[marko].id"
    And the traversal of
      """
      g.V(v1Id).as("a").out("knows").as("b").select("a", "b").by("name")
      """
    When iterated to list
    Then the result should be unordered
      | m[{"a": "marko", "b": "vadas"}] |
      | m[{"a": "marko", "b": "josh"}] |

  Scenario: g_VX1X_asXaX_outXknowsX_asXbX_selectXaX
    Given the modern graph
    And using the parameter v1Id is "v[marko].id"
    And the traversal of
      """
      g.V(v1Id).as("a").out("knows").as("b").select("a")
      """
    When iterated to list
    Then the result should be unordered
      | v[marko] |
      | v[marko] |

  Scenario: g_VX1X_asXaX_outXknowsX_asXbX_selectXaX_byXnameX
    Given the modern graph
    And using the parameter v1Id is "v[marko].id"
    And the traversal of
      """
      g.V(v1Id).as("a").out("knows").as("b").select("a").by("name")
      """
    When iterated to list
    Then the result should be unordered
      | marko |
      | marko |

  Scenario: g_V_asXaX_out_asXbX_selectXa_bX_byXnameX
    Given the modern graph
    And the traversal of
      """
      g.V().as("a").out().as("b").select("a", "b").by("name")
      """
    When iterated to list
    Then the result should be unordered
      | m[{"a": "marko", "b": "lop"}] |
      | m[{"a": "marko", "b": "vadas"}] |
      | m[{"a": "marko", "b": "josh"}] |
      | m[{"a": "josh", "b": "ripple"}] |
      | m[{"a": "josh", "b": "lop"}] |
      | m[{"a": "peter", "b": "lop"}] |

  Scenario: g_V_asXaX_out_aggregateXxX_asXbX_selectXa_bX_byXnameX
    Given the modern graph
    And the traversal of
      """
      g.V().as("a").out().aggregate("x").as("b").select("a", "b").by("name")
      """
    When iterated to list
    Then the result should be unordered
      | m[{"a": "marko", "b": "lop"}] |
      | m[{"a": "marko", "b": "vadas"}] |
      | m[{"a": "marko", "b": "josh"}] |
      | m[{"a": "josh", "b": "ripple"}] |
      | m[{"a": "josh", "b": "lop"}] |
      | m[{"a": "peter", "b": "lop"}] |

  Scenario: g_V_asXaX_name_order_asXbX_selectXa_bX_byXnameX_by_XitX
    Given the modern graph
    And the traversal of
      """
      g.V().as("a").values("name").order().as("b").select("a", "b").by("name").by()
      """
    When iterated to list
    Then the result should be unordered
      | m[{"a": "marko", "b": "marko"}] |
      | m[{"a": "vadas", "b": "vadas"}] |
      | m[{"a": "josh", "b": "josh"}] |
      | m[{"a": "ripple", "b": "ripple"}] |
      | m[{"a": "lop", "b": "lop"}] |
      | m[{"a": "peter", "b": "peter"}] |