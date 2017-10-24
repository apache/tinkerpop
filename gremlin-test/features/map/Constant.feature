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

Feature: Step - constant()

  Scenario: g_V_constantX123X
    Given the modern graph
    And using the parameter v1Id defined as "v[marko].id"
    And the traversal of
      """
      g.V().constant(123)
      """
    When iterated to list
    Then the result should be unordered
      | d[123] |
      | d[123] |
      | d[123] |
      | d[123] |
      | d[123] |
      | d[123] |

  Scenario: g_V_chooseXhasLabelXpersonX_valuesXnameX_constantXinhumanXX
    Given the modern graph
    And the traversal of
      """
      g.V().choose(__.hasLabel("person"), __.values("name"), __.constant("inhuman"))
      """
    When iterated to list
    Then the result should be unordered
      | marko |
      | vadas |
      | inhuman |
      | josh |
      | inhuman |
      | peter |

