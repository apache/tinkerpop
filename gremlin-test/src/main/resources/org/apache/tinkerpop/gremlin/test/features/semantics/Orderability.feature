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

@StepClassSemantics
Feature: Orderability

  Scenario: g_V_values_order
    Given the modern graph
    And the traversal of
      """
      g.V().values().order()
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | d[27].i |
      | d[29].i |
      | d[32].i |
      | d[35].i |
      | java |
      | java |
      | josh |
      | lop  |
      | marko |
      | peter |
      | ripple |
      | vadas  |

  @UserSuppliedVertexPropertyIds
  Scenario: g_V_properties_order
    Given the modern graph
    And the traversal of
      """
      g.V().properties().order()
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | vp[marko-name->marko] |
      | vp[marko-age->d[29].i] |
      | vp[vadas-name->vadas]  |
      | vp[vadas-age->d[27].i] |
      | vp[lop-name->lop]  |
      | vp[lop-lang->java]  |
      | vp[josh-name->josh]  |
      | vp[josh-age->d[32].i] |
      | vp[ripple-name->ripple]  |
      | vp[ripple-lang->java]  |
      | vp[peter-name->peter]  |
      | vp[peter-age->d[35].i] |

  @UserSuppliedVertexPropertyIds
  Scenario: g_V_properties_order_id
    Given the modern graph
    And the traversal of
      """
      g.V().properties().order().id()
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | d[0].l |
      | d[1].l |
      | d[2].l |
      | d[3].l |
      | d[4].l |
      | d[5].l |
      | d[6].l |
      | d[7].l |
      | d[8].l |
      | d[9].l |
      | d[10].l |
      | d[11].l |

  Scenario: g_E_properties_order_value
    Given the modern graph
    And the traversal of
      """
      g.E().properties().order().value()
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | d[0.2].d |
      | d[0.4].d |
      | d[0.4].d |
      | d[0.5].d |
      | d[1.0].d |
      | d[1.0].d |

  Scenario: g_E_properties_order_byXdescX_value
    Given the modern graph
    And the traversal of
      """
      g.E().properties().order().by(desc).value()
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

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_inject_order
    Given the empty graph
    And using the parameter xx1 defined as "null"
    And using the parameter xx2 defined as "false"
    And using the parameter xx3 defined as "true"
    And using the parameter xx4 defined as "d[1].i"
    And using the parameter xx5 defined as "d[2.0].d"
    And using the parameter xx6 defined as "bar"
    And using the parameter xx7 defined as "foo"
    And using the parameter xx8 defined as "zzz"
    And using the parameter xx9 defined as "l[a,b,c]"
    And using the parameter xx10 defined as "l[a,b,c,d]"
    And using the parameter xx11 defined as "m[{\"a\":\"a\", \"b\":false, \"c\":\"c\"}]"
    And using the parameter xx12 defined as "m[{\"a\":\"a\", \"b\":\"b\"}]"
    And using the parameter xx13 defined as "d[Infinity]"
    And using the parameter xx14 defined as "d[NaN]"
    And using the parameter xx15 defined as "d[-Infinity]"

    # TODO add support for Set, UUID, Date once the framework supports it
    And the traversal of
      """
      g.inject(xx8,xx7,xx10,xx4,xx9,xx12,xx1,xx5,xx11,xx6,xx3,xx2,xx13,xx14,xx15).order()
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | null |
      | false |
      | true |
      | d[-Infinity] |
      | d[1].i |
      | d[2.0].d |
      | d[Infinity] |
      | d[NaN] |
      | bar |
      | foo |
      | zzz |
      | l[a,b,c] |
      | l[a,b,c,d] |
      | m[{"a":"a", "b":false, "c":"c"}] |
      | m[{"a":"a", "b":"b"}] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_inject_order_byXdescX
    Given the empty graph
    And using the parameter xx1 defined as "null"
    And using the parameter xx2 defined as "false"
    And using the parameter xx3 defined as "true"
    And using the parameter xx4 defined as "d[1].i"
    And using the parameter xx5 defined as "d[2.0].d"
    And using the parameter xx6 defined as "bar"
    And using the parameter xx7 defined as "foo"
    And using the parameter xx8 defined as "zzz"
    And using the parameter xx9 defined as "l[a,b,c]"
    And using the parameter xx10 defined as "l[a,b,c,d]"
    And using the parameter xx11 defined as "m[{\"a\":\"a\", \"b\":false, \"c\":\"c\"}]"
    And using the parameter xx12 defined as "m[{\"a\":\"a\", \"b\":\"b\"}]"
    And using the parameter xx13 defined as "d[Infinity]"
    And using the parameter xx14 defined as "d[NaN]"
    And using the parameter xx15 defined as "d[-Infinity]"
    # TODO add support for Set, UUID, Date once the framework supports it
    And the traversal of
      """
      g.inject(xx8,xx7,xx10,xx4,xx9,xx12,xx1,xx5,xx11,xx6,xx3,xx2,xx13,xx14,xx15).order().by(desc)
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | m[{"a":"a", "b":"b"}] |
      | m[{"a":"a", "b":false, "c":"c"}] |
      | l[a,b,c,d] |
      | l[a,b,c] |
      | zzz |
      | foo |
      | bar |
      | d[NaN] |
      | d[Infinity] |
      | d[2.0].d |
      | d[1].i |
      | d[-Infinity] |
      | true |
      | false |
      | null |

  @UserSuppliedVertexIds
  Scenario: g_V_out_out_order_byXascX
    Given the modern graph
    And the traversal of
      """
      g.V().out().out().order().by(asc)
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | v[lop] |
      | v[ripple] |

  @UserSuppliedVertexIds
  Scenario: g_V_out_out_order_byXdescX
    Given the modern graph
    And the traversal of
      """
      g.V().out().out().order().by(desc)
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | v[ripple] |
      | v[lop] |

  @UserSuppliedVertexIds
  Scenario: g_V_out_out_asXheadX_path_order_byXascX_selectXheadX
    Given the modern graph
    And the traversal of
      """
      g.V().out().out().as("head").path().order().by(asc).select("head")
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | v[lop] |
      | v[ripple] |

  @UserSuppliedVertexIds
  Scenario: g_V_out_out_asXheadX_path_order_byXdescX_selectXheadX
    Given the modern graph
    And the traversal of
      """
      g.V().out().out().as("head").path().order().by(desc).select("head")
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | v[ripple] |
      | v[lop] |

  @UserSuppliedEdgeIds
  Scenario: g_V_out_outE_order_byXascX
    Given the modern graph
    And the traversal of
      """
      g.V().out().outE().order().by(asc)
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | e[josh-created->ripple] |
      | e[josh-created->lop] |

  @UserSuppliedEdgeIds
  Scenario: g_V_out_outE_order_byXdescX
    Given the modern graph
    And the traversal of
      """
      g.V().out().outE().order().by(desc)
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | e[josh-created->lop] |
      | e[josh-created->ripple] |

  @UserSuppliedEdgeIds
  Scenario: g_V_out_outE_asXheadX_path_order_byXascX_selectXheadX
    Given the modern graph
    And the traversal of
      """
      g.V().out().outE().as("head").path().order().by(asc).select("head")
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | e[josh-created->ripple] |
      | e[josh-created->lop] |

  @UserSuppliedEdgeIds
  Scenario: g_V_out_outE_asXheadX_path_order_byXdescX_selectXheadX
    Given the modern graph
    And the traversal of
      """
      g.V().out().outE().as("head").path().order().by(desc).select("head")
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | e[josh-created->lop] |
      | e[josh-created->ripple] |

  @UserSuppliedVertexIds
  @UserSuppliedVertexPropertyIds
  Scenario: g_V_out_out_properties_asXheadX_path_order_byXascX_selectXheadX_value
    Given the modern graph
    And the traversal of
      """
      g.V().out().out().properties().as("head").path().order().by(asc).select("head").value()
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | lop |
      | java |
      | ripple |
      | java |

  @UserSuppliedVertexIds
  @UserSuppliedVertexPropertyIds
  Scenario: g_V_out_out_properties_asXheadX_path_order_byXdescX_selectXheadX_value
    Given the modern graph
    And the traversal of
      """
      g.V().out().out().properties().as("head").path().order().by(desc).select("head").value()
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | java |
      | ripple |
      | java |
      | lop |

  @UserSuppliedVertexIds
  Scenario: g_V_out_out_values_asXheadX_path_order_byXascX_selectXheadX
    Given the modern graph
    And the traversal of
      """
      g.V().out().out().values().as("head").path().order().by(asc).select("head")
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | java |
      | lop |
      | java |
      | ripple |

  @UserSuppliedVertexIds
  Scenario: g_V_out_out_values_asXheadX_path_order_byXdescX_selectXheadX
    Given the modern graph
    And the traversal of
      """
      g.V().out().out().values().as("head").path().order().by(desc).select("head")
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | ripple |
      | java |
      | lop |
      | java |
