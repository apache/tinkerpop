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

@StepClassData @DataUUID
Feature: Data - UUID

  @AllowUUIDPropertyValues
  Scenario: g_V_valuesXuuidX_isXtypeOfXGType_UUIDXX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("data").property("uuid", UUID("f47af10b-58cc-4372-a567-0f02b2f3d479"))
      """
    And the traversal of
      """
      g.V().values("uuid").is(P.typeOf(GType.UUID))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | uuid[f47af10b-58cc-4372-a567-0f02b2f3d479] |

  @AllowUUIDPropertyValues
  Scenario: g_V_hasXuuid_typeOfXGType_UUIDXX_valuesXnameX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("data").property("name", "test").property("uuid", UUID("f47af10b-58cc-4372-a567-0f02b2f3d479"))
      """
    And the traversal of
      """
      g.V().has("uuid", P.typeOf(GType.UUID)).values("name")
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | test |

  @AllowUUIDPropertyValues
  Scenario: g_V_valuesXuuidX_isXtypeOfXGType_UUIDXX_project_byXidentityX_byXconstantXuuidXX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("data").property("uuid", UUID("f47af10b-58cc-4372-a567-0f02b2f3d479"))
      """
    And the traversal of
      """
      g.V().values("uuid").is(P.typeOf(GType.UUID)).project("original", "type").by(identity()).by(constant("uuid"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"original":"uuid[f47af10b-58cc-4372-a567-0f02b2f3d479]","type":"uuid"}] |

  @AllowUUIDPropertyValues
  Scenario: g_V_valuesXuuidX_isXtypeOfXGType_UUIDXX_whereXisXeqXuuidXX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("data").property("uuid", UUID("f47af10b-58cc-4372-a567-0f02b2f3d479"))
      """
    And the traversal of
      """
      g.V().values("uuid").is(P.typeOf(GType.UUID)).where(__.is(P.eq(UUID("f47af10b-58cc-4372-a567-0f02b2f3d479"))))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | uuid[f47af10b-58cc-4372-a567-0f02b2f3d479] |

  @AllowUUIDPropertyValues
  Scenario: g_V_valuesXuuidX_isXtypeOfXGType_UUIDXX_chooseXisXeqXuuidXX_constantXmatchX_constantXnoMatchXX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("data").property("uuid", UUID("f47af10b-58cc-4372-a567-0f02b2f3d479"))
      """
    And the traversal of
      """
      g.V().values("uuid").is(P.typeOf(GType.UUID)).choose(__.is(P.eq(UUID("f47af10b-58cc-4372-a567-0f02b2f3d479"))), constant("match"), constant("noMatch"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | match |

  @AllowUUIDPropertyValues
  Scenario: g_V_valuesXuuidX_isXtypeOfXGType_UUIDXX_localXaggregateXaXX_capXaX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("data").property("uuid", UUID("f47af10b-58cc-4372-a567-0f02b2f3d479"))
      """
    And the traversal of
      """
      g.V().values("uuid").is(P.typeOf(GType.UUID)).local(aggregate("a")).cap("a")
      """
    When iterated next
    Then the result should be unordered
      | result |
      | uuid[f47af10b-58cc-4372-a567-0f02b2f3d479] |

  @AllowUUIDPropertyValues
  Scenario: g_V_valuesXuuidX_isXtypeOfXGType_UUIDXX_aggregateXaX_capXaX
    Given the empty graph
    And the graph initializer of
      """
      g.addV("data").property("uuid", UUID("f47af10b-58cc-4372-a567-0f02b2f3d479"))
      """
    And the traversal of
      """
      g.V().values("uuid").is(P.typeOf(GType.UUID)).aggregate("a").cap("a")
      """
    When iterated next
    Then the result should be unordered
      | result |
      | uuid[f47af10b-58cc-4372-a567-0f02b2f3d479] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXuuidX_isXtypeOfXGType_UUIDXX_groupCount
    Given the empty graph
    And the traversal of
      """
      g.inject(UUID("f47af10b-58cc-4372-a567-0f02b2f3d479")).is(P.typeOf(GType.UUID)).groupCount()
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | m[{"uuid[f47af10b-58cc-4372-a567-0f02b2f3d479]":"d[1].l"}] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXUUIDX47af10b_58cc_4372_a567_0f02b2f3d479XX
    Given the empty graph
    And the traversal of
      """
      g.inject(UUID("f47af10b-58cc-4372-a567-0f02b2f3d479"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | uuid[f47af10b-58cc-4372-a567-0f02b2f3d479] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: g_injectXUUIDXXX
    Given the empty graph
    And the traversal of
      """
      g.inject(UUID())
      """
    When iterated to list
    Then the result should have a count of 1
