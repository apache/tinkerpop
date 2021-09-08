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

@StepClassMap @StepCoalesce
Feature: Step - coalesce()

  Scenario: g_V_coalesceXoutXfooX_outXbarXX
    Given the modern graph
    And the traversal of
      """
      g.V().coalesce(__.out("foo"), __.out("bar"))
      """
    When iterated to list
    Then the result should be empty

  Scenario: g_VX1X_coalesceXoutXknowsX_outXcreatedXX_valuesXnameX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).coalesce(__.out("knows"), __.out("created")).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | vadas |
      | josh |

  Scenario: g_VX1X_coalesceXoutXcreatedX_outXknowsXX_valuesXnameX
    Given the modern graph
    And using the parameter vid1 defined as "v[marko].id"
    And the traversal of
      """
      g.V(vid1).coalesce(__.out("created"), __.out("knows")).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | lop |

  Scenario: g_V_coalesceXoutXlikesX_outXknowsX_inXcreatedXX_groupCount_byXnameX
    Given the modern graph
    And the traversal of
      """
      g.V().coalesce(__.out("likes"), __.out("knows"), __.out("created")).groupCount().by("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"ripple":"d[1].l", "vadas":"d[1].l", "josh":"d[1].l", "lop":"d[2].l"}] |

  @GraphComputerVerificationReferenceOnly
  Scenario: g_V_coalesceXoutEXknowsX_outEXcreatedXX_otherV_path_byXnameX_byXlabelX
    Given the modern graph
    And the traversal of
      """
      g.V().coalesce(__.outE("knows"), __.outE("created")).otherV().path().by("name").by(T.label)
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | p[marko,knows,vadas] |
      | p[marko,knows,josh] |
      | p[josh,created,ripple] |
      | p[josh,created,lop] |
      | p[peter,created,lop] |

  Scenario: g_V_outXcreatedX_order_byXnameX_coalesceXname_constantXxXX
    Given the modern graph
    And the traversal of
      """
      g.V().out("created").order().by("name").coalesce(__.values("name"), __.constant("x"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | lop |
      | lop |
      | lop |
      | ripple |
