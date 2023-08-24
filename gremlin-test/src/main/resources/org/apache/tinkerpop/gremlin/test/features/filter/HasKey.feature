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

@StepClassFilter @StepHasKey
Feature: Step - hasKey()

  Scenario: g_V_both_dedup_properties_hasKeyXageX_value
    Given the modern graph
    And the traversal of
    """
    g.V().both().properties().dedup().hasKey("age").value()
    """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[29].i |
      | d[27].i |
      | d[32].i |
      | d[35].i |

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

  Scenario: g_V_bothE_properties_dedup_hasKeyXweightX_value
    Given the modern graph
    And the traversal of
    """
    g.V().bothE().properties().dedup().hasKey("weight").value()
    """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[0.5].d |
      | d[1.0].d |
      | d[0.4].d |
      | d[0.2].d |

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

  Scenario: g_V_properties_hasKeyXnullX
    Given the modern graph
    And the traversal of
    """
    g.V().properties().hasKey(null)
    """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_properties_hasKeyXnull_nullX
    Given the modern graph
    And the traversal of
    """
    g.V().properties().hasKey(null,null)
    """
    When iterated to list
    Then the result should be empty

  Scenario: g_V_properties_hasKeyXnull_ageX_value
    Given the modern graph
    And the traversal of
    """
    g.V().properties().hasKey(null, "age").value()
    """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[29].i |
      | d[27].i |
      | d[32].i |
      | d[35].i |

  Scenario: g_E_properties_hasKeyXnullX
    Given the modern graph
    And the traversal of
    """
    g.E().properties().hasKey(null)
    """
    When iterated to list
    Then the result should be empty

  Scenario: g_E_properties_hasKeyXnull_nullX
    Given the modern graph
    And the traversal of
    """
    g.E().properties().hasKey(null,null)
    """
    When iterated to list
    Then the result should be empty

  Scenario: g_E_properties_hasKeyXnull_weightX_value
    Given the modern graph
    And the traversal of
    """
    g.E().properties().hasKey(null, "weight").value()
    """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[0.5].d |
      | d[1.0].d |
      | d[1.0].d |
      | d[0.4].d |
      | d[0.4].d |
      | d[0.2].d |