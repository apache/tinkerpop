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
      | m[{"t[id]": "v[marko].id", "t[label]": "person", "name": ["marko"], "age": [29]}] |
      | m[{"t[id]": "v[josh].id", "t[label]": "person", "name": ["josh"], "age": [32]}] |
      | m[{"t[id]": "v[peter].id", "t[label]": "person", "name": ["peter"], "age": [35]}] |
      | m[{"t[id]": "v[vadas].id", "t[label]": "person", "name": ["vadas"], "age": [27]}] |
      | m[{"t[id]": "v[lop].id", "t[label]": "software", "name": ["lop"], "lang": ["java"]}] |
      | m[{"t[id]": "v[ripple].id", "t[label]": "software", "name": ["ripple"], "lang": ["java"]}] |

  Scenario: g_V_valueMap_withXtokensX
    Given the modern graph
    And the traversal of
      """
      g.V().valueMap().with(WithOptions.tokens)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"t[id]": "v[marko].id", "t[label]": "person", "name": ["marko"], "age": [29]}] |
      | m[{"t[id]": "v[josh].id", "t[label]": "person", "name": ["josh"], "age": [32]}] |
      | m[{"t[id]": "v[peter].id", "t[label]": "person", "name": ["peter"], "age": [35]}] |
      | m[{"t[id]": "v[vadas].id", "t[label]": "person", "name": ["vadas"], "age": [27]}] |
      | m[{"t[id]": "v[lop].id", "t[label]": "software", "name": ["lop"], "lang": ["java"]}] |
      | m[{"t[id]": "v[ripple].id", "t[label]": "software", "name": ["ripple"], "lang": ["java"]}] |

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
      | m[{"t[id]": "v[marko].id", "t[label]": "person", "name": ["marko"], "age": [29]}] |
      | m[{"t[id]": "v[josh].id", "t[label]": "person", "name": ["josh"], "age": [32]}] |
      | m[{"t[id]": "v[peter].id", "t[label]": "person", "name": ["peter"], "age": [35]}] |
      | m[{"t[id]": "v[vadas].id", "t[label]": "person", "name": ["vadas"], "age": [27]}] |
      | m[{"t[id]": "v[lop].id", "t[label]": "software", "name": ["lop"]}] |
      | m[{"t[id]": "v[ripple].id", "t[label]": "software", "name": ["ripple"]}] |

  Scenario: g_V_valueMapXname_ageX_withXtokensX
    Given the modern graph
    And the traversal of
      """
      g.V().valueMap("name", "age").with(WithOptions.tokens)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"t[id]": "v[marko].id", "t[label]": "person", "name": ["marko"], "age": [29]}] |
      | m[{"t[id]": "v[josh].id", "t[label]": "person", "name": ["josh"], "age": [32]}] |
      | m[{"t[id]": "v[peter].id", "t[label]": "person", "name": ["peter"], "age": [35]}] |
      | m[{"t[id]": "v[vadas].id", "t[label]": "person", "name": ["vadas"], "age": [27]}] |
      | m[{"t[id]": "v[lop].id", "t[label]": "software", "name": ["lop"]}] |
      | m[{"t[id]": "v[ripple].id", "t[label]": "software", "name": ["ripple"]}] |

  Scenario: g_V_valueMapXname_ageX_withXtokens_labelsX_byXunfoldX
    Given the modern graph
    And the traversal of
      """
      g.V().valueMap("name", "age").with(WithOptions.tokens, WithOptions.labels).by(__.unfold())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"t[label]": "person", "name": "marko", "age": 29}] |
      | m[{"t[label]": "person", "name": "josh", "age": 32}] |
      | m[{"t[label]": "person", "name": "peter", "age": 35}] |
      | m[{"t[label]": "person", "name": "vadas", "age": 27}] |
      | m[{"t[label]": "software", "name": "lop"}] |
      | m[{"t[label]": "software", "name": "ripple"}] |

  Scenario: g_V_valueMapXname_ageX_withXtokens_idsX_byXunfoldX
    Given the modern graph
    And the traversal of
      """
      g.V().valueMap("name", "age").with(WithOptions.tokens, WithOptions.ids).by(__.unfold())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"t[id]": "v[marko].id", "name": "marko", "age": 29}] |
      | m[{"t[id]": "v[josh].id", "name": "josh", "age": 32}] |
      | m[{"t[id]": "v[peter].id", "name": "peter", "age": 35}] |
      | m[{"t[id]": "v[vadas].id", "name": "vadas", "age": 27}] |
      | m[{"t[id]": "v[lop].id", "name": "lop"}] |
      | m[{"t[id]": "v[ripple].id", "name": "ripple"}] |

  Scenario: g_VX1X_outXcreatedX_valueMap
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).out("created").valueMap()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"name": ["lop"], "lang": ["java"]}] |

  Scenario: g_V_hasLabelXpersonX_filterXoutEXcreatedXX_valueMapXtrueX
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("person").filter(__.outE("created")).valueMap(true)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"name": ["marko"], "age": [29], "t[label]":"person", "t[id]":"v[marko].id"}] |
      | m[{"name": ["josh"], "age": [32], "t[label]":"person", "t[id]":"v[josh].id"}] |
      | m[{"name": ["peter"], "age": [35], "t[label]":"person", "t[id]":"v[peter].id"}] |

  Scenario: g_V_hasLabelXpersonX_filterXoutEXcreatedXX_valueMap_withXtokensX
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("person").filter(__.outE("created")).valueMap().with(WithOptions.tokens)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"name": ["marko"], "age": [29], "t[label]":"person", "t[id]":"v[marko].id"}] |
      | m[{"name": ["josh"], "age": [32], "t[label]":"person", "t[id]":"v[josh].id"}] |
      | m[{"name": ["peter"], "age": [35], "t[label]":"person", "t[id]":"v[peter].id"}] |

  Scenario: g_VX1X_valueMapXname_locationX_byXunfoldX_by
    Given the crew graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).valueMap("name", "location").by(__.unfold()).by()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"name": "marko", "location": ["san diego", "santa cruz", "brussels", "santa fe"]}] |

  Scenario: g_V_valueMapXname_age_nullX
    Given the modern graph
    And the traversal of
      """
      g.V().valueMap("name", "age", null)
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