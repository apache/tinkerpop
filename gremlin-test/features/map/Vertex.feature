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

Feature: Step - V(), E(), out(), in(), both(), inE(), outE(), bothE()

  Scenario: g_VXlistX1_2_3XX_name
    Given the modern graph
    And using the parameter vx is "l[v[marko],v[vadas],v[lop]]"
    And the traversal of
      """
      g.V(vx).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | string | marko |
      | string | vadas |
      | string | lop |

  Scenario: g_VXlistXv1_v2_v3XX_name
    Given the modern graph
    And using the parameter vx is "l[v[marko].id,v[vadas].id,v[lop].id]"
    And the traversal of
      """
      g.V(vx).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | string | marko |
      | string | vadas |
      | string | lop |

  Scenario: g_V
    Given the modern graph
    And the traversal of
      """
      g.V()
      """
    When iterated to list
    Then the result should be unordered
      | vertex | marko |
      | vertex | vadas |
      | vertex | lop |
      | vertex | josh |
      | vertex | ripple |
      | vertex | peter |

  Scenario: g_VX1X_out
    Given the modern graph
    And using the parameter v1 is "v[marko]"
    And the traversal of
      """
      g.V(v1).out()
      """
    When iterated to list
    Then the result should be unordered
      | vertex | vadas |
      | vertex | lop |
      | vertex | josh |

  Scenario: g_VX2X_in
    Given the modern graph
    And using the parameter v1 is "v[vadas]"
    And the traversal of
      """
      g.V(v1).in()
      """
    When iterated to list
    Then the result should be unordered
      | vertex | marko |

  Scenario: g_VX4X_both
    Given the modern graph
    And using the parameter v1 is "v[josh]"
    And the traversal of
      """
      g.V(v1).both()
      """
    When iterated to list
    Then the result should be unordered
      | vertex | marko |
      | vertex | lop |
      | vertex | ripple |

  Scenario: g_E
    Given the modern graph
    And the traversal of
      """
      g.E()
      """
    When iterated to list
    Then the result should be unordered
      | edge | marko-created->lop |
      | edge | marko-knows->josh |
      | edge | marko-knows->vadas |
      | edge | peter-created->lop |
      | edge | josh-created->lop |
      | edge | josh-created->ripple |