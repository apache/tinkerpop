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

@StepClassFilter @StepHasValue
Feature: Step - hasValue()

  Scenario: g_V_both_properties_dedup_hasKeyXageX_hasValueXgtX30XX_value
    Given the modern graph
    And the traversal of
    """
    g.V().both().properties().dedup().hasKey("age").hasValue(P.gt(30)).value()
    """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[32].i |
      | d[35].i |

  Scenario: g_V_bothE_properties_dedup_hasKeyXweightX_hasValueXltX0d3XX_value
    Given the modern graph
    And the traversal of
    """
    g.V().bothE().properties().dedup().hasKey("weight").hasValue(P.lt(0.3)).value()
    """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[0.2].d |

  Scenario: g_V_properties_hasValueXnullX
    Given the modern graph
    And the traversal of
    """
    g.V().properties().hasValue(null)
    """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_properties_hasValueXnull_nullX
    Given the modern graph
    And the traversal of
    """
    g.V().properties().hasValue(null,null)
    """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_properties_hasValueXnull_joshX_value
    Given the modern graph
    And the traversal of
    """
    g.V().properties().hasValue(null, "josh").value()
    """
    When iterated to list
    Then the result should be unordered
      | result |
      | josh |