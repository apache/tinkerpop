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

Feature: Step - valueMap()

  Scenario: g_V_valueMap
    Given the modern graph
    And the traversal of
      """
      g.V().valueMap()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"name": ["marko"], "age": [29]}] |
      | m[{"name": ["josh"], "age": [32]}] |
      | m[{"name": ["peter"], "age": [35]}] |
      | m[{"name": ["vadas"], "age": [27]}] |
      | m[{"name": ["lop"], "lang": ["java"]}] |
      | m[{"name": ["ripple"], "lang": ["java"]}] |

  Scenario: g_V_valueMapXtrueX
    Given the modern graph
    And the traversal of
      """
      g.V().valueMap(true)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"id": "v[marko].id", "label": "person", "name": ["marko"], "age": [29]}] |
      | m[{"id": "v[josh].id", "label": "person", "name": ["josh"], "age": [32]}] |
      | m[{"id": "v[peter].id", "label": "person", "name": ["peter"], "age": [35]}] |
      | m[{"id": "v[vadas].id", "label": "person", "name": ["vadas"], "age": [27]}] |
      | m[{"id": "v[lop].id", "label": "software", "name": ["lop"], "lang": ["java"]}] |
      | m[{"id": "v[ripple].id", "label": "software", "name": ["ripple"], "lang": ["java"]}] |

  Scenario: g_V_valueMapXname_ageX
    Given the modern graph
    And the traversal of
      """
      g.V().valueMap("name", "age")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"name": ["marko"], "age": [29]}] |
      | m[{"name": ["josh"], "age": [32]}] |
      | m[{"name": ["peter"], "age": [35]}] |
      | m[{"name": ["vadas"], "age": [27]}] |
      | m[{"name": ["lop"]}] |
      | m[{"name": ["ripple"]}] |

  Scenario: g_V_valueMapXtrue_name_ageX
    Given the modern graph
    And the traversal of
      """
      g.V().valueMap(true, "name", "age")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"id": "v[marko].id", "label": "person", "name": ["marko"], "age": [29]}] |
      | m[{"id": "v[josh].id", "label": "person", "name": ["josh"], "age": [32]}] |
      | m[{"id": "v[peter].id", "label": "person", "name": ["peter"], "age": [35]}] |
      | m[{"id": "v[vadas].id", "label": "person", "name": ["vadas"], "age": [27]}] |
      | m[{"id": "v[lop].id", "label": "software", "name": ["lop"]}] |
      | m[{"id": "v[ripple].id", "label": "software", "name": ["ripple"]}] |

  Scenario: g_VX1X_outXcreatedX_valueMap
    Given the modern graph
    And using the parameter v1Id defined as "v[marko].id"
    And the traversal of
      """
      g.V(v1Id).out("created").valueMap()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"name": ["lop"], "lang": ["java"]}] |