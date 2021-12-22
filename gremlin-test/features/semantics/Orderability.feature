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

@StepClassOrderability
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

  Scenario: g_inject_order
    Given the empty graph
    And using the parameter xxnull defined as "null"
    And using the parameter xx1i defined as "d[1].i"
    And using the parameter xxt defined as "true"
    And using the parameter xxf defined as "false"
    And using the parameter xx2d defined as "d[2.0].d"
    And using the parameter xxs1 defined as "foo"
    And using the parameter xxs2 defined as "bar"
    And using the parameter xxs3 defined as "zzz"
    And using the parameter xxl1 defined as "l[a,b,c]"
    And using the parameter xxl2 defined as "l[a,b,c,d]"
    And using the parameter xxm1 defined as "m[{\"a\":\"a\", \"b\":\"b\"}]"
    And using the parameter xxm2 defined as "m[{\"a\":\"a\", \"b\":false, \"c\":\"c\"}]"
    # TODO add support for Set, UUID, Date once the framework supports it
    And the traversal of
      """
      g.inject(xxs1,xxs3,xxl2,xx1i,xxl1,xxm1,xxnull,xx2d,xxm2,xxs2,xxt,xxf).order()
      """
    When iterated to list
    Then the result should be ordered
      | result |
      | null |
      | false |
      | true |
      | d[1].i |
      | d[2.0].d |
      | bar |
      | foo |
      | zzz |
      | l[a,b,c] |
      | l[a,b,c,d] |
      | m[{"a":"a", "b":false, "c":"c"}] |
      | m[{"a":"a", "b":"b"}] |
