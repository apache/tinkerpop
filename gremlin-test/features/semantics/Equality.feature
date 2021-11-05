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
Feature: Equality

  @GraphComputerVerificationInjectionNotSupported
  Scenario: Primitives_Number_eqXbyteX
    Given the empty graph
    And using the parameter xx1 defined as "l[d[1].b,d[1].s,d[1].i,d[1].l,d[1].f,d[1].d,d[1000].i,d[1].m,d[1].n]"
    And using the parameter xx2 defined as "d[1].b"
    And the traversal of
      """
      g.inject(xx1).unfold().where(__.is(xx2))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[1].i |
      | d[1].l |
      | d[1].f |
      | d[1].d |
      | d[1].s |
      | d[1].n |
      | d[1].m |
      | d[1].b |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: Primitives_Number_eqXshortX
    Given the empty graph
    And using the parameter xx1 defined as "l[d[1].b,d[1].s,d[1].i,d[1].l,d[1].f,d[1].d,d[1000].i,d[1].m,d[1].n]"
    And using the parameter xx2 defined as "d[1].s"
    And the traversal of
      """
      g.inject(xx1).unfold().where(__.is(xx2))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[1].i |
      | d[1].l |
      | d[1].f |
      | d[1].d |
      | d[1].s |
      | d[1].n |
      | d[1].m |
      | d[1].b |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: Primitives_Number_eqXintX
    Given the empty graph
    And using the parameter xx1 defined as "l[d[1].b,d[1].s,d[1].i,d[1].l,d[1].f,d[1].d,d[1000].i,d[1].m,d[1].n]"
    And using the parameter xx2 defined as "d[1].i"
    And the traversal of
      """
      g.inject(xx1).unfold().where(__.is(xx2))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[1].i |
      | d[1].l |
      | d[1].f |
      | d[1].d |
      | d[1].s |
      | d[1].n |
      | d[1].m |
      | d[1].b |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: Primitives_Number_eqXlongX
    Given the empty graph
    And using the parameter xx1 defined as "l[d[1].b,d[1].s,d[1].i,d[1].l,d[1].f,d[1].d,d[1000].i,d[1].m,d[1].n]"
    And using the parameter xx2 defined as "d[1].l"
    And the traversal of
      """
      g.inject(xx1).unfold().where(__.is(xx2))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[1].i |
      | d[1].l |
      | d[1].f |
      | d[1].d |
      | d[1].s |
      | d[1].n |
      | d[1].m |
      | d[1].b |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: Primitives_Number_eqXbigintX
    Given the empty graph
    And using the parameter xx1 defined as "l[d[1].b,d[1].s,d[1].i,d[1].l,d[1].f,d[1].d,d[1000].i,d[1].m,d[1].n]"
    And using the parameter xx2 defined as "d[1].n"
    And the traversal of
      """
      g.inject(xx1).unfold().where(__.is(xx2))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[1].i |
      | d[1].l |
      | d[1].f |
      | d[1].d |
      | d[1].s |
      | d[1].n |
      | d[1].m |
      | d[1].b |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: Primitives_Number_eqXfloatX
    Given the empty graph
    And using the parameter xx1 defined as "l[d[1].b,d[1].s,d[1].i,d[1].l,d[1].f,d[1].d,d[1000].i,d[1].m,d[1].n]"
    And using the parameter xx2 defined as "d[1].f"
    And the traversal of
      """
      g.inject(xx1).unfold().where(__.is(xx2))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[1].i |
      | d[1].l |
      | d[1].f |
      | d[1].d |
      | d[1].s |
      | d[1].n |
      | d[1].m |
      | d[1].b |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: Primitives_Number_eqXdoubleX
    Given the empty graph
    And using the parameter xx1 defined as "l[d[1].b,d[1].s,d[1].i,d[1].l,d[1].f,d[1].d,d[1000].i,d[1].m,d[1].n]"
    And using the parameter xx2 defined as "d[1].d"
    And the traversal of
      """
      g.inject(xx1).unfold().where(__.is(xx2))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[1].i |
      | d[1].l |
      | d[1].f |
      | d[1].d |
      | d[1].s |
      | d[1].n |
      | d[1].m |
      | d[1].b |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: Primitives_Number_eqXbigdecimalX
    Given the empty graph
    And using the parameter xx1 defined as "l[d[1].b,d[1].s,d[1].i,d[1].l,d[1].f,d[1].d,d[1000].i,d[1].m,d[1].n]"
    And using the parameter xx2 defined as "d[1].m"
    And the traversal of
      """
      g.inject(xx1).unfold().where(__.is(xx2))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[1].i |
      | d[1].l |
      | d[1].f |
      | d[1].d |
      | d[1].s |
      | d[1].n |
      | d[1].m |
      | d[1].b |