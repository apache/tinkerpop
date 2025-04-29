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

@StepClassMap @StepFlatMap
Feature: Step - flatMap()

  Scenario: g_V_asXaX_flatMapXselectXaXX
    Given the modern graph
    And the traversal of
      """
      g.V().as("a").flatMap(__.select("a"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |
      | v[vadas] |
      | v[lop] |
      | v[josh] |
      | v[ripple] |
      | v[peter]  |

  Scenario: g_V_valuesXnameX_flatMapXsplitXaX_unfoldX
    Given the modern graph
    And the traversal of
      """
      g.V().values("name").flatMap(split("a").unfold())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m |
      | rko |
      | v |
      | d |
      | s |
      | lop |
      | josh |
      | ripple |
      | peter |


