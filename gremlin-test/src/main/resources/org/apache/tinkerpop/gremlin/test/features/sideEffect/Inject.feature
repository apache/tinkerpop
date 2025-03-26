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

@StepClassSideEffect @StepInject
Feature: Step - inject()

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_VX1X_out_injectXv2X_name
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And using the parameter v2 defined as "v[vadas]"
    And the traversal of
      """
      g.V(vid1).out().inject(v2).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | vadas |
      | lop   |
      | vadas |
      | josh  |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_VX1X_out_name_injectXdanielX_asXaX_mapXlengthX_path
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And using the parameter l1 defined as "c[it.get().length()]"
    And the traversal of
      """
      g.V(vid1).out().values("name").inject("daniel").as("a").map(l1).path()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | p[daniel,d[6].i] |
      | p[v[marko],v[lop],lop,d[3].i] |
      | p[v[marko],v[vadas],vadas,d[5].i] |
      | p[v[marko],v[josh],josh,d[4].i] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_VX1X_injectXg_VX4XX_out_name
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And using the parameter v2 defined as "v[josh]"
    And the traversal of
      """
      g.V(vid1).inject(v2).out().values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | ripple |
      | lop   |
      | lop   |
      | vadas |
      | josh  |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXnull_1_3_nullX
    Given the modern graph
    And the traversal of
      """
      g.inject(null, 1, 3, null)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | null |
      | d[1].i |
      | d[3].i |
      | null |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX10_20_null_20_10_10X_groupCountXxX_dedup_asXyX_projectXa_bX_by_byXselectXxX_selectXselectXyXXX
    Given the modern graph
    And the traversal of
      """
      g.inject(10,20,null,20,10,10).groupCount("x").dedup().as("y").project("a","b").by().by(__.select("x").select(__.select("y")))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a":"d[10].i", "b":"d[3].l"}] |
      | m[{"a":"d[20].i", "b":"d[2].l"}] |
      | m[{"a":null, "b":"d[1].l"}] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXname_marko_age_nullX_selectXname_ageX
    Given the modern graph
    And using the parameter xx1 defined as "m[{\"name\":\"marko\", \"age\":null}]"
    And the traversal of
      """
      g.inject(xx1).select("name","age")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"name":"marko", "age":null}] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXnull_nullX
    Given the modern graph
    And the traversal of
      """
      g.inject(null, null)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | null |
      | null |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXnullX
    Given the modern graph
    And the traversal of
      """
      g.inject(null)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | null |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_inject
    Given the modern graph
    And the traversal of
      """
      g.inject()
      """
    When iterated to list
    Then the result should be empty

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_VX1X_valuesXageX_injectXnull_nullX
    Given the modern graph
    And using the parameter xx1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(xx1).values("age").inject(null, null)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[29].i |
      | null |
      | null |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_VX1X_valuesXageX_injectXnullX
    Given the modern graph
    And using the parameter xx1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(xx1).values("age").inject(null)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[29].i |
      | null |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_VX1X_valuesXageX_inject
    Given the modern graph
    And using the parameter xx1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(xx1).values("age").inject()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[29].i |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXnull_1_3_nullX_asXaX_selectXaX
    Given the modern graph
    And the traversal of
      """
      g.inject(null, 1, 3, null).as("a").select("a")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | null |
      | d[1].i |
      | d[3].i |
      | null |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX1_3X_injectX100_300X
    Given the modern graph
    And the traversal of
      """
      g.inject(1, 3).inject(100, 300)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[100].i |
      | d[300].i |
      | d[1].i |
      | d[3].i |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX1var_3varX_injectX100var_300varX
    Given the modern graph
    And using the parameter xx1 defined as "d[1].i"
    And using the parameter xx2 defined as "d[3].i"
    And using the parameter xx3 defined as "d[100].i"
    And using the parameter xx4 defined as "d[300].i"
    And the traversal of
      """
      g.inject(xx1, xx2).inject(xx3, xx4)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[100].i |
      | d[300].i |
      | d[1].i |
      | d[3].i |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX1_3_100_300X_list
    Given the modern graph
    And the traversal of
      """
      g.inject([1, 3, 100, 300])
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[d[1].i,d[3].i,d[100].i,d[300].i] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX1_3_100_300X_listvar
    Given the modern graph
    And using the parameter xx1 defined as "l[d[1].i,d[3].i,d[100].i,d[300].i]"
    And the traversal of
      """
      g.inject(xx1)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[d[1].i,d[3].i,d[100].i,d[300].i] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX1_3_100_300X_set
    Given the modern graph
    And the traversal of
      """
      g.inject({1, 3, 100, 300})
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | s[d[100].i,d[300].i,d[1].i,d[3].i] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX1_3_100_300X_setvar
    Given the modern graph
    And using the parameter xx1 defined as "s[d[100].i,d[300].i,d[1].i,d[3].i]"
    And the traversal of
      """
      g.inject(xx1)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | s[d[100].i,d[300].i,d[1].i,d[3].i] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX1_1X_set
    Given the modern graph
    And the traversal of
      """
      g.inject({1, 1})
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | s[d[1].i] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX1_1X_setvar
    Given the modern graph
    And using the parameter xx1 defined as "s[d[1].i,d[1].i]"
    And the traversal of
      """
      g.inject(xx1)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | s[d[1].i] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectX_set
    Given the modern graph
    And using the parameter xx1 defined as "s[]"
    And the traversal of
      """
      g.inject(xx1)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | s[] |