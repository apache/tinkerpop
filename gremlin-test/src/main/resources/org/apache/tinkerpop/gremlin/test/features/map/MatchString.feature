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

@TinkerGQL @StepClassMap @StepMatch
Feature: Step - match() (String form)

  Scenario: g_match_person_selectXpX_byXnameX
    Given the modern graph
    And the traversal of
      """
      g.match("MATCH (p:person)").select("p").by("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | marko  |
      | vadas  |
      | josh   |
      | peter  |

  Scenario: g_match_personXknowsX_person_selectXa_bX_byXnameX
    Given the modern graph
    And the traversal of
      """
      g.match("MATCH (a:person)-[:knows]->(b:person)").select("a","b").by("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a":"marko","b":"vadas"}] |
      | m[{"a":"marko","b":"josh"}]  |

  Scenario: g_match_personXknowsX_personXcreatedX_software_selectXa_b_sX_byXnameX
    Given the modern graph
    And the traversal of
      """
      g.match("MATCH (a:person)-[:knows]->(b:person)-[:created]->(s:software)").select("a","b","s").by("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a":"marko","b":"josh","s":"ripple"}] |
      | m[{"a":"marko","b":"josh","s":"lop"}]    |

  Scenario: g_match_softwareXreversedCreatedX_person_selectXp_sX_byXnameX
    Given the modern graph
    And the traversal of
      """
      g.match("MATCH (s:software)<-[:created]-(p:person)").select("p","s").by("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"p":"marko","s":"lop"}]   |
      | m[{"p":"josh","s":"ripple"}] |
      | m[{"p":"josh","s":"lop"}]    |
      | m[{"p":"peter","s":"lop"}]   |

  Scenario: g_match_personXundirectedKnowsX_person_selectXa_bX_byXnameX
    Given the modern graph
    And the traversal of
      """
      g.match("MATCH (a:person)-[:knows]-(b:person)").select("a","b").by("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a":"marko","b":"vadas"}] |
      | m[{"a":"marko","b":"josh"}]  |
      | m[{"a":"vadas","b":"marko"}] |
      | m[{"a":"josh","b":"marko"}]  |

  Scenario: g_match_personXname_markoX_knowsPerson_selectXp_fX_byXnameX
    Given the modern graph
    And the traversal of
      """
      g.match("MATCH (p:person {name: 'marko'})-[:knows]->(f:person)").select("p","f").by("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"p":"marko","f":"vadas"}] |
      | m[{"p":"marko","f":"josh"}]  |

  Scenario: g_match_personXname_paramX_knowsPerson_selectXp_fX_byXnameX
    Given the modern graph
    And the traversal of
      """
      g.match('MATCH (p:person {name: $who})-[:knows]->(f:person)', ["who": "marko"]).select("p","f").by("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"p":"marko","f":"vadas"}] |
      | m[{"p":"marko","f":"josh"}]  |

  Scenario: g_match_multiPattern_sharedVariable_whereXa_neqXbXX_selectXa_b_sX_byXnameX
    Given the modern graph
    And the traversal of
      """
      g.match("MATCH (a:person)-[:created]->(s:software), (b:person)-[:created]->(s)").where("a", neq("b")).select("a","b","s").by("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a":"marko","b":"josh","s":"lop"}]  |
      | m[{"a":"marko","b":"peter","s":"lop"}] |
      | m[{"a":"josh","b":"marko","s":"lop"}]  |
      | m[{"a":"josh","b":"peter","s":"lop"}]  |
      | m[{"a":"peter","b":"marko","s":"lop"}] |
      | m[{"a":"peter","b":"josh","s":"lop"}]  |

  Scenario: g_match_terminalBindingMap
    Given the modern graph
    And the traversal of
      """
      g.match("MATCH (a:person)-[:knows]->(b:person)")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a":"v[marko]","b":"v[vadas]"}] |
      | m[{"a":"v[marko]","b":"v[josh]"}]  |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_inject_match_midTraversal_selectXa_bX_byXnameX
    Given the modern graph
    And the traversal of
      """
      g.inject(1).match("MATCH (a:person)-[:knows]->(b:person)").select("a","b").by("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"a":"marko","b":"vadas"}] |
      | m[{"a":"marko","b":"josh"}]  |
