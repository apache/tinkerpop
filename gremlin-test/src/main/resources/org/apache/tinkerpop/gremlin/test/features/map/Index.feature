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

@StepClassMap @StepIndex
Feature: Step - index()

  Scenario: g_V_hasLabelXsoftwareX_index_unfold
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("software").index().unfold()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[v[lop],d[0].i] |
      | l[v[ripple],d[0].i] |

  Scenario: g_V_hasLabelXsoftwareX_order_byXnameX_index_withXmapX
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("software").order().by("name").index().with(WithOptions.indexer, WithOptions.map)
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | m[{"d[0].i": "v[lop]"}] |
      | m[{"d[0].i": "v[ripple]"}] |

  Scenario: g_V_hasLabelXsoftwareX_name_fold_orderXlocalX_index_unfold_order_byXtailXlocal_1XX
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("software").values("name").fold().order(Scope.local).index().unfold().order().by(__.tail(Scope.local, 1))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[lop,d[0].i] |
      | l[ripple,d[1].i] |

  Scenario: g_V_hasLabelXpersonX_name_fold_orderXlocalX_index_withXmapX
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("person").values("name").fold().order(Scope.local).index().with(WithOptions.indexer, WithOptions.map)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"d[0].i": "josh", "d[1].i": "marko", "d[2].i": "peter", "d[3].i": "vadas"}] |
