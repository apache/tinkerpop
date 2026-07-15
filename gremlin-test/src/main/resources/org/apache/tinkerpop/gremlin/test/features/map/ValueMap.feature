# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

@StepClassMap @StepValueMap
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
      g.with("singlelabel").V().valueMap(true)
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
      g.with("singlelabel").V().valueMap().with(WithOptions.tokens)
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

  @SingleLabelDefault
  Scenario: g_V_valueMap_withXtokensX_single_label_only_graph_default
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

  @MultiLabelDefault
  Scenario: g_V_valueMap_withXtokensX_single_label_only_graph_multi_label_default
    Given the modern graph
    And the traversal of
      """
      g.V().valueMap().with(WithOptions.tokens)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"t[id]": "v[marko].id", "t[label]": "s[person]", "name": ["marko"], "age": [29]}] |
      | m[{"t[id]": "v[josh].id", "t[label]": "s[person]", "name": ["josh"], "age": [32]}] |
      | m[{"t[id]": "v[peter].id", "t[label]": "s[person]", "name": ["peter"], "age": [35]}] |
      | m[{"t[id]": "v[vadas].id", "t[label]": "s[person]", "name": ["vadas"], "age": [27]}] |
      | m[{"t[id]": "v[lop].id", "t[label]": "s[software]", "name": ["lop"], "lang": ["java"]}] |
      | m[{"t[id]": "v[ripple].id", "t[label]": "s[software]", "name": ["ripple"], "lang": ["java"]}] |

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
      g.with("singlelabel").V().valueMap(true, "name", "age")
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
      g.with("singlelabel").V().valueMap("name", "age").with(WithOptions.tokens)
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
      g.with("singlelabel").V().valueMap("name", "age").with(WithOptions.tokens, WithOptions.labels).by(__.unfold())
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
      g.with("singlelabel").V().hasLabel("person").filter(__.outE("created")).valueMap(true)
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
      g.with("singlelabel").V().hasLabel("person").filter(__.outE("created")).valueMap().with(WithOptions.tokens)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"name": ["marko"], "age": [29], "t[label]":"person", "t[id]":"v[marko].id"}] |
      | m[{"name": ["josh"], "age": [32], "t[label]":"person", "t[id]":"v[josh].id"}] |
      | m[{"name": ["peter"], "age": [35], "t[label]":"person", "t[id]":"v[peter].id"}] |

  @MultiProperties @MetaProperties
  Scenario: g_VX1X_valueMapXname_locationX_byXunfoldX_by
    Given the crew graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).valueMap("name", "location").by(__.unfold()).by()
      """
    When iterated next
    Then the traversal will raise an error with message containing text of "step can only have one by modulator"

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

  @InsertionOrderingRequired
  Scenario: g_V_valueMapXname_ageX_byXisXxXXbyXunfoldX
    Given the modern graph
    And the traversal of
      """
      g.V().valueMap("name", "age").by(__.is("x")).by(__.unfold())
      """
    When iterated to list
    Then the traversal will raise an error with message containing text of "step can only have one by modulator"

  Scenario: g_withXmultilabelX_VXmarkoX_valueMap_withXtokensX
    Given the modern graph
    And the traversal of
      """
      g.with("multilabel").V().has("name", "marko").valueMap().with(WithOptions.tokens)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"t[id]": "v[marko].id", "t[label]": "s[person]", "name": ["marko"], "age": [29]}] |

  @MultiLabel @SingleLabelDefault
  Scenario: g_V_valueMap_withXtokensX_single_label_default
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").addLabel("employee").property("name", "marko")
      """
    And the traversal of
      """
      g.V().valueMap().with(WithOptions.tokens)
      """
    When iterated to list
    Then the result should have a count of 1
    And the result should be of
      | result |
      | m[{"t[id]": "v[marko].id", "t[label]": "person", "name": ["marko"]}] |
      | m[{"t[id]": "v[marko].id", "t[label]": "employee", "name": ["marko"]}] |

  @MultiLabel
  Scenario: g_withXmultilabelX_V_valueMap_withXtokensX_multilabel
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").addLabel("employee").property("name", "marko")
      """
    And the traversal of
      """
      g.with("multilabel").V().valueMap().with(WithOptions.tokens)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"t[id]": "v[marko].id", "t[label]": "s[person,employee]", "name": ["marko"]}] |

  @MultiLabel @MultiLabelDefault
  Scenario: g_V_valueMap_withXtokensX_multi_label_default
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").addLabel("employee").property("name", "marko")
      """
    And the traversal of
      """
      g.V().valueMap().with(WithOptions.tokens)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"t[id]": "v[marko].id", "t[label]": "s[person,employee]", "name": ["marko"]}] |

  @MultiLabel @MultiLabelDefault
  Scenario: g_withXsinglelabelX_V_valueMap_withXtokensX_multi_label_default
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").addLabel("employee").property("name", "marko")
      """
    And the traversal of
      """
      g.with("singlelabel").V().valueMap().with(WithOptions.tokens)
      """
    When iterated to list
    Then the result should have a count of 1
    And the result should be of
      | result |
      | m[{"t[id]": "v[marko].id", "t[label]": "person", "name": ["marko"]}] |
      | m[{"t[id]": "v[marko].id", "t[label]": "employee", "name": ["marko"]}] |

  @MultiLabel
  Scenario: g_withXsinglelabelX_V_valueMapXtrueX_zero_label_vertex
    Given the empty graph
    And the graph initializer of
      """
      g.addV().property("name", "nobody")
      """
    And the traversal of
      """
      g.with("singlelabel").V().valueMap(true)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"t[id]": "v[nobody].id", "name": ["nobody"]}] |

  @MultiLabel
  Scenario: g_withXmultilabelX_V_valueMapXtrueX_zero_label_vertex
    Given the empty graph
    And the graph initializer of
      """
      g.addV().property("name", "nobody")
      """
    And the traversal of
      """
      g.with("multilabel").V().valueMap(true)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"t[id]": "v[nobody].id", "t[label]": "s[]", "name": ["nobody"]}] |

  @MultiLabel @MultiLabelDefault
  Scenario: g_V_valueMapXtrueX_zero_label_vertex_multi_label_default
    Given the empty graph
    And the graph initializer of
      """
      g.addV().property("name", "nobody")
      """
    And the traversal of
      """
      g.V().valueMap(true)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"t[id]": "v[nobody].id", "t[label]": "s[]", "name": ["nobody"]}] |

  @MultiLabel @SingleLabelDefault
  Scenario: g_V_valueMapXtrueX_zero_label_vertex_single_label_default
    Given the empty graph
    And the graph initializer of
      """
      g.addV().property("name", "nobody")
      """
    And the traversal of
      """
      g.V().valueMap(true)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"t[id]": "v[nobody].id", "name": ["nobody"]}] |