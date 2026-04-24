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

@StepClassFilter @StepHas
Feature: Step - has() with traversal arguments

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_hasXname_VXvid1X_valuesXnameXX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V().has("name", __.V(vid1).values("name"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_hasXname_VXvid1X_outXknowsX_valuesXnameXX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V().has("name", __.V(vid1).out("knows").values("name"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[vadas] |
      | v[josh] |

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_hasXage_VXvid1X_outXknowsX_valuesXageXX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V().has("age", __.V(vid1).out("knows").values("age"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[vadas] |
      | v[josh] |

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_hasXname_VXvid1X_valuesXnonexistentXX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V().has("name", __.V(vid1).values("nonexistent"))
      """
    When iterated to list
    Then the result should be empty

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_hasXname_notXidentityXX
    Given the modern graph
    And the traversal of
      """
      g.V().has("name", __.not(__.identity()))
      """
    When iterated to list
    Then the result should be empty

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_hasXage_gtXVXvid1X_valuesXageXXX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V().has("age", P.gt(__.V(vid1).values("age")))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[josh] |
      | v[peter] |

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_hasXage_lteXVXvid2X_valuesXageXXX
    Given the modern graph
    And using the parameter vid2 defined as "v[vadas].id"
    And the traversal of
      """
      g.V().has("age", P.lte(__.V(vid2).values("age")))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[vadas] |

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_hasXage_neqXVXvid3X_valuesXageXXX
    Given the modern graph
    And using the parameter vid3 defined as "v[josh].id"
    And the traversal of
      """
      g.V().has("age", P.neq(__.V(vid3).values("age")))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |
      | v[vadas] |
      | v[peter] |

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_hasXname_eqXVXvid1X_valuesXnameXXX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V().has("name", P.eq(__.V(vid1).values("name")))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_hasXage_ltXVXvid1X_valuesXageXXX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V().has("age", P.lt(__.V(vid1).values("age")))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[vadas] |

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_hasXage_gteXVXvid3X_valuesXageXXX
    Given the modern graph
    And using the parameter vid3 defined as "v[josh].id"
    And the traversal of
      """
      g.V().has("age", P.gte(__.V(vid3).values("age")))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[josh] |
      | v[peter] |

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_hasXage_eqXVXvid1X_valuesXnonexistentXXX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V().has("age", P.eq(__.V(vid1).values("nonexistent")))
      """
    When iterated to list
    Then the result should be empty

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_hasXlabel_VXvid1X_labelXX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V().has(T.label, __.V(vid1).label())
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | v[marko] |
      | v[vadas] |
      | v[josh] |
      | v[peter] |

  @GraphComputerVerificationMidVNotSupported
  Scenario: g_V_hasXperson_name_VXvid1X_valuesXnameXX_age
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V().has("person", "name", __.V(vid1).values("name")).values("age")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[29].i |
