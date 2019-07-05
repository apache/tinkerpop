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

Feature: Step - branch()

  Scenario: g_V_branchXlabel_eq_person__a_bX_optionXa__ageX_optionXb__langX_optionXb__nameX
    Given the modern graph
    And using the parameter l1 defined as "c[it.get().label() == 'person' ? 'a' : 'b']"
    And the traversal of
      """
      g.V().
        branch(l1).
          option("a", __.values("age")).
          option("b", __.values("lang")).
          option("b", __.values("name"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | java |
      | java |
      | lop |
      | ripple |
      | d[29].i |
      | d[27].i |
      | d[32].i |
      | d[35].i |

  Scenario: g_V_branchXlabel_isXpersonX_countX_optionX1__ageX_optionX0__langX_optionX0__nameX
    Given the modern graph
    And the traversal of
      """
      g.V().
        branch(__.label().is("person").count()).
          option(1L, __.values("age")).
          option(0L, __.values("lang")).
          option(0L, __.values("name"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | java |
      | java |
      | lop |
      | ripple |
      | d[29].i |
      | d[27].i |
      | d[32].i |
      | d[35].i |

  Scenario: g_V_branchXlabel_isXpersonX_countX_optionX1__ageX_optionX0__langX_optionX0__nameX_optionXany__labelX
    Given the modern graph
    And the traversal of
      """
      g.V().
        branch(__.label().is("person").count()).
          option(1L, __.values("age")).
          option(0L, __.values("lang")).
          option(0L, __.values("name")).
          option(Pick.any, __.label())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | java |
      | java |
      | lop |
      | ripple |
      | d[29].i |
      | d[27].i |
      | d[32].i |
      | d[35].i |
      | person |
      | person |
      | person |
      | person |
      | software |
      | software |

  Scenario: g_V_branchXageX_optionXltX30X__youngX_optionXgtX30X__oldX_optionXnone__on_the_edgeX
    Given the modern graph
    And the traversal of
      """
      g.V().hasLabel("person").
        branch(__.values("age")).
          option(P.lt(30), __.constant("young")).
          option(P.gt(30), __.constant("old")).
          option(Pick.none, __.constant("on the edge"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | young |
      | young |
      | old |
      | old |

  Scenario: g_V_branchXidentityX_optionXhasLabelXsoftwareX__inXcreatedX_name_order_foldX_optionXhasXname_vadasX__ageX_optionXneqX123X__bothE_countX
    Given the modern graph
    And the traversal of
      """
      g.V().
        branch(__.identity()).
          option(__.hasLabel("software"), __.in("created").values("name").order().fold()).
          option(__.has("name","vadas"), __.values("age")).
          option(P.neq(123), __.bothE().count())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | l[josh,josh,marko,peter] |
      | d[27].i |
      | d[12].l |
