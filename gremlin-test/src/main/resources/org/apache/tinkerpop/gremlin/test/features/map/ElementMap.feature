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

@StepClassMap @StepElementMap
Feature: Step - elementMap()

  Scenario: g_V_elementMap
    Given the modern graph
    And the traversal of
      """
      g.with("singlelabel").V().elementMap()
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
      g.with("singlelabel").V().elementMap("name", "age")
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
      g.with("singlelabel").E(eid11).elementMap()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"t[id]": "e[josh-created->lop].id", "t[label]": "created", "weight": "d[0.4].d", "D[OUT]": "m[{\\"t[id]\\": \\"v[josh].id\\", \\"t[label]\\": \\"person\\"}]", "D[IN]": "m[{\\"t[id]\\": \\"v[lop].id\\", \\"t[label]\\": \\"software\\"}]"}] |

  Scenario: g_V_elementMapXname_age_nullX
    Given the modern graph
    And the traversal of
      """
      g.with("singlelabel").V().elementMap("name", "age", null)
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

  Scenario: g_withXmultilabelX_VXmarkoX_elementMap
    Given the modern graph
    And the traversal of
      """
      g.with("multilabel").V().has("name", "marko").elementMap()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"t[id]": "v[marko].id", "t[label]": "s[person]", "name": "marko", "age": 29}] |

  @MultiLabel @SingleLabelDefault
  Scenario: g_V_elementMap_single_label_default
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", "marko")
      """
    And the traversal of
      """
      g.V().elementMap()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"t[id]": "v[marko].id", "t[label]": "person", "name": "marko"}] |

  @MultiLabel
  Scenario: g_withXmultilabelX_V_elementMap_multilabel
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").addLabel("employee").property("name", "marko")
      """
    And the traversal of
      """
      g.with("multilabel").V().elementMap()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"t[id]": "v[marko].id", "t[label]": "s[person,employee]", "name": "marko"}] |

  @MultiLabel @MultiLabelDefault
  Scenario: g_V_elementMap_multi_label_default
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").addLabel("employee").property("name", "marko")
      """
    And the traversal of
      """
      g.V().elementMap()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"t[id]": "v[marko].id", "t[label]": "s[person,employee]", "name": "marko"}] |

  @MultiLabel @MultiLabelDefault
  Scenario: g_withXsinglelabelX_V_elementMap_multi_label_default
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").addLabel("employee").property("name", "marko")
      """
    And the traversal of
      """
      g.with("singlelabel").V().elementMap()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"t[id]": "v[marko].id", "t[label]": "person", "name": "marko"}] |

  @MultiLabel @MultiLabelDefault
  Scenario: g_V_elementMap_single_label_vertex_multi_label_default
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", "marko")
      """
    And the traversal of
      """
      g.V().elementMap()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"t[id]": "v[marko].id", "t[label]": "s[person]", "name": "marko"}] |

  @MultiLabel
  Scenario: g_withXsinglelabelX_V_elementMap_zero_label_vertex
    Given the empty graph
    And the graph initializer of
      """
      g.addV().property("name", "nobody")
      """
    And the traversal of
      """
      g.with("singlelabel").V().elementMap()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"t[id]": "v[nobody].id", "name": "nobody"}] |

  @MultiLabel
  Scenario: g_withXmultilabelX_V_elementMap_zero_label_vertex
    Given the empty graph
    And the graph initializer of
      """
      g.addV().property("name", "nobody")
      """
    And the traversal of
      """
      g.with("multilabel").V().elementMap()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"t[id]": "v[nobody].id", "t[label]": "s[]", "name": "nobody"}] |

  @MultiLabel @MultiLabelDefault
  Scenario: g_V_elementMap_zero_label_vertex_multi_label_default
    Given the empty graph
    And the graph initializer of
      """
      g.addV().property("name", "nobody")
      """
    And the traversal of
      """
      g.V().elementMap()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"t[id]": "v[nobody].id", "t[label]": "s[]", "name": "nobody"}] |

  @MultiLabel @SingleLabelDefault
  Scenario: g_V_elementMap_zero_label_vertex_single_label_default
    Given the empty graph
    And the graph initializer of
      """
      g.addV().property("name", "nobody")
      """
    And the traversal of
      """
      g.V().elementMap()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"t[id]": "v[nobody].id", "name": "nobody"}] |

  @MultiLabel
  Scenario: g_withXmultilabelX_E_elementMap
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", "marko").as("a").addV("person").property("name", "josh").as("b").addE("knows").from("a").to("b").property("weight", 0.5)
      """
    And the traversal of
      """
      g.with("multilabel").E().elementMap()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"t[id]": "e[marko-knows->josh].id", "t[label]": "s[knows]", "weight": "d[0.5].d", "D[OUT]": "m[{\\"t[id]\\": \\"v[marko].id\\", \\"t[label]\\": \\"person\\"}]", "D[IN]": "m[{\\"t[id]\\": \\"v[josh].id\\", \\"t[label]\\": \\"person\\"}]"}] |

  @GraphComputerVerificationReferenceOnly @MultiLabel @MultiLabelDefault
  Scenario: g_E_elementMap_multi_label_default
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", "marko").as("a").addV("person").property("name", "josh").as("b").addE("knows").from("a").to("b").property("weight", 0.5)
      """
    And the traversal of
      """
      g.E().elementMap()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"t[id]": "e[marko-knows->josh].id", "t[label]": "s[knows]", "weight": "d[0.5].d", "D[OUT]": "m[{\\"t[id]\\": \\"v[marko].id\\", \\"t[label]\\": \\"person\\"}]", "D[IN]": "m[{\\"t[id]\\": \\"v[josh].id\\", \\"t[label]\\": \\"person\\"}]"}] |

  @GraphComputerVerificationReferenceOnly @MultiLabel @SingleLabelDefault
  Scenario: g_E_elementMap_single_label_default
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", "marko").as("a").addV("person").property("name", "josh").as("b").addE("knows").from("a").to("b").property("weight", 0.5)
      """
    And the traversal of
      """
      g.E().elementMap()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"t[id]": "e[marko-knows->josh].id", "t[label]": "knows", "weight": "d[0.5].d", "D[OUT]": "m[{\\"t[id]\\": \\"v[marko].id\\", \\"t[label]\\": \\"person\\"}]", "D[IN]": "m[{\\"t[id]\\": \\"v[josh].id\\", \\"t[label]\\": \\"person\\"}]"}] |

  @GraphComputerVerificationReferenceOnly @MultiLabel
  Scenario: g_withXsinglelabelX_E_elementMap
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("name", "marko").as("a").addV("person").property("name", "josh").as("b").addE("knows").from("a").to("b").property("weight", 0.5)
      """
    And the traversal of
      """
      g.with("singlelabel").E().elementMap()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"t[id]": "e[marko-knows->josh].id", "t[label]": "knows", "weight": "d[0.5].d", "D[OUT]": "m[{\\"t[id]\\": \\"v[marko].id\\", \\"t[label]\\": \\"person\\"}]", "D[IN]": "m[{\\"t[id]\\": \\"v[josh].id\\", \\"t[label]\\": \\"person\\"}]"}] |