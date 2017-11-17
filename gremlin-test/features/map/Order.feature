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

Feature: Step - order()

  Scenario: g_V_name_order
    Given the modern graph
    And the traversal of
      """
      g.V().values("name").order()
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | josh |
      | lop  |
      | marko |
      | peter |
      | ripple |
      | vadas  |

  Scenario: g_V_name_order_byXa1_b1X_byXb2_a2X
    Given the modern graph
    And using the parameter l1 defined as "c[a, b -> a.substring(1, 2).compareTo(b.substring(1, 2))]"
    And using the parameter l2 defined as "c[a, b -> b.substring(2, 3).compareTo(a.substring(2, 3))]"
    And the traversal of
      """
      g.V().values("name").order().by(l1).by(l2)
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | marko  |
      | vadas  |
      | peter  |
      | ripple |
      | josh   |
      | lop    |

  Scenario: g_V_order_byXname_incrX_name
    Given the modern graph
    And the traversal of
      """
      g.V().order().by("name", Order.incr).values("name")
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | josh |
      | lop  |
      | marko |
      | peter |
      | ripple |
      | vadas  |

  Scenario: g_V_order_byXnameX_name
    Given the modern graph
    And the traversal of
      """
      g.V().order().by("name").values("name")
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | josh |
      | lop  |
      | marko |
      | peter |
      | ripple |
      | vadas  |

  Scenario: g_V_outE_order_byXweight_decrX_weight
    Given the modern graph
    And the traversal of
      """
      g.V().outE().order().by("weight", Order.decr).values("weight")
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | d[1.0].d |
      | d[1.0].d |
      | d[0.5].d |
      | d[0.4].d |
      | d[0.4].d |
      | d[0.2].d |

  Scenario: g_V_order_byXname_a1_b1X_byXname_b2_a2X_name
    Given the modern graph
    And using the parameter l1 defined as "c[a, b -> a.substring(1, 2).compareTo(b.substring(1, 2))]"
    And using the parameter l2 defined as "c[a, b -> b.substring(2, 3).compareTo(a.substring(2, 3))]"
    And the traversal of
      """
      g.V().order().by("name", l1).by("name", l2).values("name")
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | marko  |
      | vadas  |
      | peter  |
      | ripple |
      | josh   |
      | lop    |

  Scenario: g_V_asXaX_outXcreatedX_asXbX_order_byXshuffleX_selectXa_bX
    Given the modern graph
    And the traversal of
      """
      g.V().as("a").out("created").as("b").order().by(Order.shuffle).select("a", "b")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a":"v[marko]","b":"v[lop]"}] |
      | m[{"a":"v[peter]","b":"v[lop]"}] |
      | m[{"a":"v[josh]","b":"v[ripple]"}] |
      | m[{"a":"v[josh]","b":"v[lop]"}] |
