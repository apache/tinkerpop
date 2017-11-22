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

Feature: Step - match()

  Scenario: g_V_valueMap_matchXa_selectXnameX_bX
    Given the modern graph
    And the traversal of
      """
      g.V().valueMap().match(__.as("a").select("name").as("b"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a":{"name":["marko"],"age":[29]},"b":["marko"]}] |
      | m[{"a":{"name":["vadas"],"age":[27]},"b":["vadas"]}] |
      | m[{"a":{"name":["lop"],"lang":["java"]},"b":["lop"]}] |
      | m[{"a":{"name":["josh"],"age":[32]},"b":["josh"]}] |
      | m[{"a":{"name":["ripple"],"lang":["java"]},"b":["ripple"]}] |
      | m[{"a":{"name":["peter"],"age":[35]},"b":["peter"]}] |

  Scenario: g_V_matchXa_out_bX
    Given the modern graph
    And the traversal of
      """
      g.V().match(__.as("a").out().as("b"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a":"v[marko]","b":"v[lop]"}] |
      | m[{"a":"v[marko]","b":"v[vadas]"}] |
      | m[{"a":"v[marko]","b":"v[josh]"}] |
      | m[{"a":"v[josh]","b":"v[ripple]"}] |
      | m[{"a":"v[josh]","b":"v[lop]"}] |
      | m[{"a":"v[peter]","b":"v[lop]"}] |

  Scenario: g_V_matchXa_out_bX_selectXb_idX
    Given the modern graph
    And the traversal of
      """
      g.V().match(__.as("a").out().as("b")).select("b").by(T.id)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[3].l |
      | d[2].l |
      | d[4].l |
      | d[5].l |
      | d[3].l |
      | d[3].l |

  Scenario: g_V_matchXa_knows_b__b_created_cX
    Given the modern graph
    And the traversal of
      """
      g.V().match(__.as("a").out("knows").as("b"),
                  __.as("b").out("created").as("c"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a":"v[marko]","b":"v[josh]", "c":"v[ripple]"}] |
      | m[{"a":"v[marko]","b":"v[josh]", "c":"v[lop]"}] |

