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

Feature: Step - where()

  Scenario: g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_selectXa_bX_whereXa_eqXbXX
    Given the modern graph
    And the traversal of
      """
      g.V().has("age").as("a").out().in().has("age").as("b").select("a", "b").where("a", P.eq("b"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a":"v[marko]","b":"v[marko]"}] |
      | m[{"a":"v[marko]","b":"v[marko]"}] |
      | m[{"a":"v[marko]","b":"v[marko]"}] |
      | m[{"a":"v[josh]","b":"v[josh]"}] |
      | m[{"a":"v[josh]","b":"v[josh]"}] |
      | m[{"a":"v[peter]","b":"v[peter]"}] |

  Scenario: g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_selectXa_bX_whereXa_neqXbXX
    Given the modern graph
    And the traversal of
      """
      g.V().has("age").as("a").out().in().has("age").as("b").select("a", "b").where("a", P.neq("b"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a":"v[marko]","b":"v[josh]"}] |
      | m[{"a":"v[marko]","b":"v[peter]"}] |
      | m[{"a":"v[josh]","b":"v[marko]"}] |
      | m[{"a":"v[josh]","b":"v[peter]"}] |
      | m[{"a":"v[peter]","b":"v[marko]"}] |
      | m[{"a":"v[peter]","b":"v[josh]"}] |

  Scenario: g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_selectXa_bX_whereXb_hasXname_markoXX
    Given the modern graph
    And the traversal of
      """
      g.V().has("age").as("a").out().in().has("age").as("b").select("a", "b").where(__.as("b").has("name", "marko"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a":"v[marko]","b":"v[marko]"}] |
      | m[{"a":"v[marko]","b":"v[marko]"}] |
      | m[{"a":"v[marko]","b":"v[marko]"}] |
      | m[{"a":"v[josh]","b":"v[marko]"}] |
      | m[{"a":"v[peter]","b":"v[marko]"}] |

  Scenario: g_V_hasXageX_asXaX_out_in_hasXageX_asXbX_selectXa_bX_whereXa_outXknowsX_bX
    Given the modern graph
    And the traversal of
      """
      g.V().has("age").as("a").out().in().has("age").as("b").select("a", "b").where(__.as("a").out("knows").as("b"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a":"v[marko]","b":"v[josh]"}] |

  Scenario: g_V_asXaX_outXcreatedX_whereXasXaX_name_isXjoshXX_inXcreatedX_name
    Given the modern graph
    And the traversal of
      """
      g.V().as("a").out("created").where(__.as("a").values("name").is("josh")).in("created").values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | josh |
      | josh |
      | marko |
      | peter |

  Scenario: g_withSideEffectXa_josh_peterX_VX1X_outXcreatedX_inXcreatedX_name_whereXwithinXaXX
    Given the modern graph
    And using the parameter l defined as "l[josh,peter]"
    And using the parameter v1Id defined as "v[marko].id"
    And the traversal of
      """
      g.withSideEffect("a", l).V(v1Id).out("created").in("created").values("name").where(P.within("a"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | josh |
      | peter |

