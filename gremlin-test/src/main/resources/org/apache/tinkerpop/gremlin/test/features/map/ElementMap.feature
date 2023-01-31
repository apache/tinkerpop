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

@StepClassMap @StepElementMap
Feature: Step - elementMap()

  Scenario: g_V_elementMap
    Given the modern graph
    And the traversal of
      """
      g.V().elementMap()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"t[id]": "v[marko].id", "t[label]": "person", "name": "marko", "age": 29}] |
      | m[{"t[id]": "v[josh].id", "t[label]": "person", "name": "josh", "age": 32}] |
      | m[{"t[id]": "v[peter].id", "t[label]": "person", "name": "peter", "age": 35}] |
      | m[{"t[id]": "v[vadas].id", "t[label]": "person", "name": "vadas", "age": 27}] |
      | m[{"t[id]": "v[lop].id", "t[label]": "software", "name": "lop", "lang": "java"}] |
      | m[{"t[id]": "v[ripple].id", "t[label]": "software", "name": "ripple", "lang": "java"}] |

  Scenario: g_V_elementMapXname_ageX
    Given the modern graph
    And the traversal of
      """
      g.V().elementMap("name", "age")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"t[id]": "v[marko].id", "t[label]": "person", "name": "marko", "age": 29}] |
      | m[{"t[id]": "v[josh].id", "t[label]": "person", "name": "josh", "age": 32}] |
      | m[{"t[id]": "v[peter].id", "t[label]": "person", "name": "peter", "age": 35}] |
      | m[{"t[id]": "v[vadas].id", "t[label]": "person", "name": "vadas", "age": 27}] |
      | m[{"t[id]": "v[lop].id", "t[label]": "software", "name": "lop"}] |
      | m[{"t[id]": "v[ripple].id", "t[label]": "software", "name": "ripple"}] |

  @GraphComputerVerificationReferenceOnly
  Scenario: g_EX11X_elementMap
    Given the modern graph
    And using the parameter eid11 defined as "e[josh-created->lop].id"
    And the traversal of
    """
      g.E(eid11).elementMap()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"t[id]": "e[josh-created->lop].id", "t[label]": "created", "weight": "d[0.4].d", "D[OUT]": "m[{\\"t[id]\\": \\"v[josh].id\\", \\"t[label]\\": \\"person\\"}]", "D[IN]": "m[{\\"t[id]\\": \\"v[lop].id\\", \\"t[label]\\": \\"software\\"}]"}] |

  Scenario: g_V_elementMapXname_age_nullX
    Given the modern graph
    And the traversal of
      """
      g.V().elementMap("name", "age", null)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"t[id]": "v[marko].id", "t[label]": "person", "name": "marko", "age": 29}] |
      | m[{"t[id]": "v[josh].id", "t[label]": "person", "name": "josh", "age": 32}] |
      | m[{"t[id]": "v[peter].id", "t[label]": "person", "name": "peter", "age": 35}] |
      | m[{"t[id]": "v[vadas].id", "t[label]": "person", "name": "vadas", "age": 27}] |
      | m[{"t[id]": "v[lop].id", "t[label]": "software", "name": "lop"}] |
      | m[{"t[id]": "v[ripple].id", "t[label]": "software", "name": "ripple"}] |