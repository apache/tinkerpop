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

@StepClassIntegrated
Feature: Step - PartitionStrategy

  @WithPartitionStrategy
  Scenario: g_withStrategiesXPartitionStrategyXwrite_a_read_aXX_V_name
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("_partition","a").property("name","alice").
        addV("person").property("_partition","b").property("name","bob")
      """
    And the traversal of
      """
      g.withStrategies(new PartitionStrategy(partitionKey: "_partition", writePartition: "a", readPartitions: ["a"])).V().values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | alice |

  @WithPartitionStrategy
  Scenario: g_withStrategiesXPartitionStrategyXwrite_a_read_a_bXX_V_name
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("_partition","a").property("name","alice").
        addV("person").property("_partition","b").property("name","bob")
      """
    And the traversal of
      """
      g.withStrategies(new PartitionStrategy(partitionKey: "_partition", writePartition: "a", readPartitions: ["a", "b"])).V().values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | alice |
      | bob |

  @WithPartitionStrategy
  Scenario: g_withStrategiesXPartitionStrategyXwrite_a_read_cXX_V_name
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("_partition","a").property("name","alice").
        addV("person").property("_partition","b").property("name","bob")
      """
    And the traversal of
      """
      g.withStrategies(new PartitionStrategy(partitionKey: "_partition", writePartition: "a", readPartitions: ["c"])).V().values("name")
      """
    When iterated to list
    Then the result should have a count of 0

  @WithPartitionStrategy
  Scenario: g_withStrategiesXPartitionStrategyXwrite_a_read_aXX_V_bothE_weight
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("_partition","a").property("name","alice").as("a").
        addV("person").property("_partition","b").property("name","bob").as("b").
        addE("knows").from("a").to("b").property("_partition","a").property("weight",1).
        addE("knows").from("b").to("a").property("_partition","b").property("weight",2)
      """
    And the traversal of
      """
      g.withStrategies(new PartitionStrategy(partitionKey: "_partition", writePartition: "a", readPartitions: ["a"])).V().
        bothE().values("weight")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[1].i |

  @WithPartitionStrategy
  Scenario: g_withStrategiesXPartitionStrategyXwrite_a_read_bXX_V_bothE_weight
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("_partition","a").property("name","alice").as("a").
        addV("person").property("_partition","b").property("name","bob").as("b").
        addE("knows").from("a").to("b").property("_partition","a").property("weight",1).
        addE("knows").from("b").to("a").property("_partition","b").property("weight",2)
      """
    And the traversal of
      """
      g.withStrategies(new PartitionStrategy(partitionKey: "_partition", writePartition: "a", readPartitions: ["b"])).V().
        bothE().values("weight")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[2].i |

  @WithPartitionStrategy
  Scenario: g_withStrategiesXPartitionStrategyXwrite_a_read_a_bXX_V_bothE_dedup_weight
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("_partition","a").property("name","alice").as("a").
        addV("person").property("_partition","b").property("name","bob").as("b").
        addE("knows").from("a").to("b").property("_partition","a").property("weight",1).
        addE("knows").from("b").to("a").property("_partition","b").property("weight",2)
      """
    And the traversal of
      """
      g.withStrategies(new PartitionStrategy(partitionKey: "_partition", writePartition: "a", readPartitions: ["a", "b"])).V().
        bothE().dedup().values("weight")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[1].i |
      | d[2].i |

  @WithPartitionStrategy
  Scenario: g_withStrategiesXPartitionStrategyXwrite_a_read_cXX_V_bothE_weight
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("_partition","a").property("name","alice").as("a").
        addV("person").property("_partition","b").property("name","bob").as("b").
        addE("knows").from("a").to("b").property("_partition","a").property("weight",1).
        addE("knows").from("b").to("a").property("_partition","b").property("weight",2)
      """
    And the traversal of
      """
      g.withStrategies(new PartitionStrategy(partitionKey: "_partition", writePartition: "a", readPartitions: ["c"])).V().
        bothE().values("weight")
      """
    When iterated to list
    Then the result should have a count of 0

  @WithPartitionStrategy
  Scenario: g_withStrategiesXPartitionStrategyXwrite_a_read_aXX_V_both_name
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("_partition","a").property("name","alice").as("a").
        addV("person").property("_partition","b").property("name","bob").as("b").
        addE("knows").from("a").to("b").property("_partition","a").property("weight",1).
        addE("knows").from("b").to("a").property("_partition","b").property("weight",2)
      """
    And the traversal of
      """
      g.withStrategies(new PartitionStrategy(partitionKey: "_partition", writePartition: "a", readPartitions: ["a"])).V().
        both().values("name")
      """
    When iterated to list
    Then the result should have a count of 0

  @WithPartitionStrategy
  Scenario: g_withStrategiesXPartitionStrategyXwrite_a_read_bXX_V_both_name
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("_partition","a").property("name","alice").as("a").
        addV("person").property("_partition","b").property("name","bob").as("b").
        addE("knows").from("a").to("b").property("_partition","a").property("weight",1).
        addE("knows").from("b").to("a").property("_partition","b").property("weight",2)
      """
    And the traversal of
      """
      g.withStrategies(new PartitionStrategy(partitionKey: "_partition", writePartition: "a", readPartitions: ["b"])).V().
        both().values("name")
      """
    When iterated to list
    Then the result should have a count of 0

  @WithPartitionStrategy
  Scenario: g_withStrategiesXPartitionStrategyXwrite_a_read_a_bXX_V_both_dedup_name
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("_partition","a").property("name","alice").as("a").
        addV("person").property("_partition","b").property("name","bob").as("b").
        addE("knows").from("a").to("b").property("_partition","a").property("weight",1).
        addE("knows").from("b").to("a").property("_partition","b").property("weight",2)
      """
    And the traversal of
      """
      g.withStrategies(new PartitionStrategy(partitionKey: "_partition", writePartition: "a", readPartitions: ["a", "b"])).V().
        both().dedup().values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | alice |
      | bob |

  @WithPartitionStrategy
  Scenario: g_withStrategiesXPartitionStrategyXwrite_a_read_cXX_V_both_name
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("_partition","a").property("name","alice").as("a").
        addV("person").property("_partition","b").property("name","bob").as("b").
        addE("knows").from("a").to("b").property("_partition","a").property("weight",1).
        addE("knows").from("b").to("a").property("_partition","b").property("weight",2)
      """
    And the traversal of
      """
      g.withStrategies(new PartitionStrategy(partitionKey: "_partition", writePartition: "a", readPartitions: ["c"])).V().
        both().values("name")
      """
    When iterated to list
    Then the result should have a count of 0

  @WithPartitionStrategy
  Scenario: g_withStrategiesXPartitionStrategyXwrite_a_read_aXX_V_out_name
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("_partition","a").property("name","alice").as("a").
        addV("person").property("_partition","b").property("name","bob").as("b").
        addE("knows").from("a").to("b").property("_partition","a").property("weight",1).
        addE("knows").from("b").to("a").property("_partition","b").property("weight",2)
      """
    And the traversal of
      """
      g.withStrategies(new PartitionStrategy(partitionKey: "_partition", writePartition: "a", readPartitions: ["a"])).V().
        out().values("name")
      """
    When iterated to list
    Then the result should have a count of 0

  @WithPartitionStrategy
  Scenario: g_withStrategiesXPartitionStrategyXwrite_a_read_bXX_V_in_name
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("_partition","a").property("name","alice").as("a").
        addV("person").property("_partition","b").property("name","bob").as("b").
        addE("knows").from("a").to("b").property("_partition","a").property("weight",1).
        addE("knows").from("b").to("a").property("_partition","b").property("weight",2)
      """
    And the traversal of
      """
      g.withStrategies(new PartitionStrategy(partitionKey: "_partition", writePartition: "a", readPartitions: ["b"])).V().
        in().values("name")
      """
    When iterated to list
    Then the result should have a count of 0

  @WithPartitionStrategy
  Scenario: g_withStrategiesXPartitionStrategyXwrite_a_read_a_bXX_V_out_name
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("_partition","a").property("name","alice").as("a").
        addV("person").property("_partition","b").property("name","bob").as("b").
        addE("knows").from("a").to("b").property("_partition","a").property("weight",1).
        addE("knows").from("b").to("a").property("_partition","b").property("weight",2)
      """
    And the traversal of
      """
      g.withStrategies(new PartitionStrategy(partitionKey: "_partition", writePartition: "a", readPartitions: ["a", "b"])).V().
        out().values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | alice |
      | bob |

  @WithPartitionStrategy
  Scenario: g_withStrategiesXPartitionStrategyXwrite_a_read_cXX_V_out_name
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("_partition","a").property("name","alice").as("a").
        addV("person").property("_partition","b").property("name","bob").as("b").
        addE("knows").from("a").to("b").property("_partition","a").property("weight",1).
        addE("knows").from("b").to("a").property("_partition","b").property("weight",2)
      """
    And the traversal of
      """
      g.withStrategies(new PartitionStrategy(partitionKey: "_partition", writePartition: "a", readPartitions: ["c"])).V().
        out().values("name")
      """
    When iterated to list
    Then the result should have a count of 0

  @WithPartitionStrategy
  Scenario: g_withStrategiesXPartitionStrategyXwrite_a_read_aXX_addVXpersonX_propertyXname_aliceX_addXselfX
    Given the empty graph
    And the traversal of
      """
      g.withStrategies(new PartitionStrategy(partitionKey: "_partition", writePartition: "a", readPartitions: ["a"])).
        addV("person").property("name","alice").
        addE("self")
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 1 for count of "g.V().has(\"person\",\"name\",\"alice\").has(\"_partition\",\"a\")"
    And the graph should return 1 for count of "g.V()"
    And the graph should return 1 for count of "g.E().has(\"_partition\",\"a\")"
    And the graph should return 1 for count of "g.E()"

  @WithPartitionStrategy
  Scenario: g_withStrategiesXPartitionStrategyXwrite_a_read_aXX_injectXzeroX_addVXpersonX_propertyXname_aliceX_addXselfX
    Given the empty graph
    And the traversal of
      """
      g.withStrategies(new PartitionStrategy(partitionKey: "_partition", writePartition: "a", readPartitions: ["a"])).
        inject(0).
        addV("person").property("name","alice").
        addE("self")
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 1 for count of "g.V().has(\"person\",\"name\",\"alice\").has(\"_partition\",\"a\")"
    And the graph should return 1 for count of "g.V()"
    And the graph should return 1 for count of "g.E().has(\"_partition\",\"a\")"
    And the graph should return 1 for count of "g.E()"

  @WithPartitionStrategy
  Scenario: g_withStrategiesXPartitionStrategyXwrite_a_read_aXX_mergeV
    Given the empty graph
    And using the parameter xx1 defined as "m[{\"t[label]\": \"person\", \"name\":\"alice\"}]"
    And the traversal of
      """
      g.withStrategies(new PartitionStrategy(partitionKey: "_partition", writePartition: "a", readPartitions: ["a"])).
        mergeV(xx1)
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 1 for count of "g.V().has(\"person\",\"name\",\"alice\").has(\"_partition\",\"a\")"
    And the graph should return 1 for count of "g.V()"

  @WithPartitionStrategy
  Scenario: g_withStrategiesXPartitionStrategyXwrite_a_read_aXX_injectX0X_mergeV
    Given the empty graph
    And using the parameter xx1 defined as "m[{\"t[label]\": \"person\", \"name\":\"alice\"}]"
    And the traversal of
      """
      g.withStrategies(new PartitionStrategy(partitionKey: "_partition", writePartition: "a", readPartitions: ["a"])).
        inject(0).
        mergeV(xx1)
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 1 for count of "g.V().has(\"person\",\"name\",\"alice\").has(\"_partition\",\"a\")"
    And the graph should return 1 for count of "g.V()"

  @WithPartitionStrategy
  Scenario: g_withStrategiesXPartitionStrategyXwrite_a_read_aXX_mergeE
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("_partition","a").property("name","alice").
        addV("person").property("_partition","a").property("name","bob")
      """
    And using the parameter xx1 defined as "m[{\"t[label]\": \"knows\", \"D[OUT]\":\"v[alice].id\", \"D[IN]\":\"v[bob].id\"}]"
    And the traversal of
      """
      g.withStrategies(new PartitionStrategy(partitionKey: "_partition", writePartition: "a", readPartitions: ["a"])).
        mergeE(xx1)
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 1 for count of "g.E().has(\"knows\",\"_partition\",\"a\")"
    And the graph should return 1 for count of "g.E()"

  @WithPartitionStrategy
  Scenario: g_withStrategiesXPartitionStrategyXwrite_a_read_aXX_injectX0XmergeE
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("_partition","a").property("name","alice").
        addV("person").property("_partition","a").property("name","bob")
      """
    And using the parameter xx1 defined as "m[{\"t[label]\": \"knows\", \"D[OUT]\":\"v[alice].id\", \"D[IN]\":\"v[bob].id\"}]"
    And the traversal of
      """
      g.withStrategies(new PartitionStrategy(partitionKey: "_partition", writePartition: "a", readPartitions: ["a"])).
        inject(0).
        mergeE(xx1)
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 1 for count of "g.E().has(\"knows\",\"_partition\",\"a\")"
    And the graph should return 1 for count of "g.E()"

  @WithPartitionStrategy
  Scenario: g_withStrategiesXPartitionStrategyXwrite_a_read_aXX_mergeVXlabel_person_name_aliceX_optionXonMatch_name_bobX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("_partition","a").property("name","alice").
        addV("person").property("_partition","b").property("name","alice")
      """
    And using the parameter xx1 defined as "m[{\"t[label]\": \"person\", \"name\":\"alice\"}]"
    And using the parameter xx2 defined as "m[{\"name\":\"bob\"}]"
    And the traversal of
      """
      g.withStrategies(new PartitionStrategy(partitionKey: "_partition", writePartition: "a", readPartitions: ["a"])).
        mergeV(xx1).option(Merge.onMatch, xx2)
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 1 for count of "g.V().has(\"person\",\"name\",\"bob\").has(\"_partition\",\"a\")"
    And the graph should return 2 for count of "g.V()"

  @WithPartitionStrategy
  Scenario: g_withStrategiesXPartitionStrategyXwrite_a_read_aXX_mergeV_optionXonCreateX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("_partition","b").property("name","alice")
      """
    And using the parameter xx1 defined as "m[{\"name\":\"alice\"}]"
    And using the parameter xx2 defined as "m[{\"age\":\"d[35].i\"}]"
    And the traversal of
      """
      g.withStrategies(new PartitionStrategy(partitionKey: "_partition", writePartition: "a", readPartitions: ["a"])).
        mergeV(xx1).option(Merge.onCreate, xx2)
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 1 for count of "g.V().has(\"name\",\"alice\").has(\"age\",35).has(\"_partition\",\"a\")"
    And the graph should return 2 for count of "g.V()"

  @WithPartitionStrategy
  Scenario: g_withStrategiesXPartitionStrategyXwrite_a_read_aXX_injectX0X__mergeV_optionXonCreateX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("person").property("_partition","b").property("name","alice")
      """
    And using the parameter xx1 defined as "m[{\"name\":\"alice\"}]"
    And using the parameter xx2 defined as "m[{\"age\":\"d[35].i\"}]"
    And the traversal of
      """
      g.withStrategies(new PartitionStrategy(partitionKey: "_partition", writePartition: "a", readPartitions: ["a"])).
        inject(0).
        mergeV(xx1).option(Merge.onCreate, xx2)
      """
    When iterated to list
    Then the result should have a count of 1
    And the graph should return 1 for count of "g.V().has(\"name\",\"alice\").has(\"age\",35).has(\"_partition\",\"a\")"
    And the graph should return 2 for count of "g.V()"