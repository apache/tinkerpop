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
Feature: Comparability

  ######################################################
  ## Null vs. Null
  ######################################################

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectXnullX_eqXnullX
    Given the empty graph
    And the traversal of
      """
      g.inject(null).is(P.eq(null))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | null |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectXnullX_neqXnullX
    Given the empty graph
    And the traversal of
      """
      g.inject(null).is(P.neq(null))
      """
    When iterated to list
    Then the result should be empty

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectXnullX_ltXnullX
    Given the empty graph
    And the traversal of
      """
      g.inject(null).is(P.lt(null))
      """
    When iterated to list
    Then the result should be empty

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectXnullX_lteXnullX
    Given the empty graph
    And the traversal of
      """
      g.inject(null).is(P.lte(null))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | null |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectXnullX_gtXnullX
    Given the empty graph
    And the traversal of
      """
      g.inject(null).is(P.gt(null))
      """
    When iterated to list
    Then the result should be empty

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectXnullX_gteXnullX
    Given the empty graph
    And the traversal of
      """
      g.inject(null).is(P.gte(null))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | null |

  ######################################################
  ## NaN vs. NaN
  ######################################################

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectXNaNX_eqXNaNX
    Given the empty graph
    And using the parameter xx1 defined as "d[NaN]"
    And the traversal of
      """
      g.inject(xx1).is(P.eq(NaN))
      """
    When iterated to list
    Then the result should be empty

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectXNaNX_neqXNaNX
    Given the empty graph
    And using the parameter xx1 defined as "d[NaN]"
    And the traversal of
      """
      g.inject(xx1).is(P.neq(NaN))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[NaN] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectXNaNX_ltXNaNX
    Given the empty graph
    And using the parameter xx1 defined as "d[NaN]"
    And the traversal of
      """
      g.inject(xx1).is(P.lt(NaN))
      """
    When iterated to list
    Then the result should be empty

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectXNaNX_lteXNaNX
    Given the empty graph
    And using the parameter xx1 defined as "d[NaN]"
    And the traversal of
      """
      g.inject(xx1).is(P.lte(NaN))
      """
    When iterated to list
    Then the result should be empty

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectXNaNX_gtXNaNX
    Given the empty graph
    And using the parameter xx1 defined as "d[NaN]"
    And the traversal of
      """
      g.inject(xx1).is(P.gt(NaN))
      """
    When iterated to list
    Then the result should be empty

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectXNaNX_gteXNaNX
    Given the empty graph
    And using the parameter xx1 defined as "d[NaN]"
    And the traversal of
      """
      g.inject(xx1).is(P.gte(NaN))
      """
    When iterated to list
    Then the result should be empty

  ######################################################
  ## NaN vs. Double
  ######################################################

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectX1dX_eqXNaNX
    Given the empty graph
    And the traversal of
      """
      g.inject(1.0d).is(P.eq(NaN))
      """
    When iterated to list
    Then the result should be empty

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectX1dX_neqXNaNX
    Given the empty graph
    And the traversal of
      """
      g.inject(1.0d).is(P.neq(NaN))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[1].d |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectX1dX_ltXNaNX
    Given the empty graph
    And the traversal of
      """
      g.inject(1.0d).is(P.lt(NaN))
      """
    When iterated to list
    Then the result should be empty

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectX1dX_lteXNaNX
    Given the empty graph
    And the traversal of
      """
      g.inject(1.0d).is(P.lte(NaN))
      """
    When iterated to list
    Then the result should be empty

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectX1dX_gtXNaNX
    Given the empty graph
    And the traversal of
      """
      g.inject(1.0d).is(P.gt(NaN))
      """
    When iterated to list
    Then the result should be empty

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectX1dX_gteXNaNX
    Given the empty graph
    And the traversal of
      """
      g.inject(1.0d).is(P.gte(NaN))
      """
    When iterated to list
    Then the result should be empty
    
  ######################################################
  ## NaN vs. null
  ######################################################

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectXNaNX_eqXnullX
    Given the empty graph
    And the traversal of
      """
      g.inject(NaN).is(P.eq(null))
      """
    When iterated to list
    Then the result should be empty

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectXNaNX_neqXnullX
    Given the empty graph
    And the traversal of
      """
      g.inject(NaN).is(P.neq(null))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[NaN] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectXNaNX_ltXnullX
    Given the empty graph
    And the traversal of
      """
      g.inject(NaN).is(P.lt(null))
      """
    When iterated to list
    Then the result should be empty

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectXNaNX_lteXnullX
    Given the empty graph
    And the traversal of
      """
      g.inject(NaN).is(P.lte(null))
      """
    When iterated to list
    Then the result should be empty

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectXNaNX_gtXnullX
    Given the empty graph
    And the traversal of
      """
      g.inject(NaN).is(P.gt(null))
      """
    When iterated to list
    Then the result should be empty

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectXNaNX_gteXnullX
    Given the empty graph
    And the traversal of
      """
      g.inject(NaN).is(P.gte(null))
      """
    When iterated to list
    Then the result should be empty

    
  ######################################################
  ## Compare across types
  ######################################################

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectXfooX_eqX1dX
    Given the empty graph
    And the traversal of
      """
      g.inject("foo").is(P.eq(1.0d))
      """
    When iterated to list
    Then the result should be empty

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectXfooX_neqX1dX
    Given the empty graph
    And the traversal of
      """
      g.inject("foo").is(P.neq(1.0d))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | foo |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectXfooX_ltX1dX
    Given the empty graph
    And the traversal of
      """
      g.inject("foo").is(P.lt(1.0d))
      """
    When iterated to list
    Then the result should be empty

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectXfooX_lteX1dX
    Given the empty graph
    And the traversal of
      """
      g.inject("foo").is(P.lte(1.0d))
      """
    When iterated to list
    Then the result should be empty

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectXfooX_gtX1dX
    Given the empty graph
    And the traversal of
      """
      g.inject("foo").is(P.gt(1.0d))
      """
    When iterated to list
    Then the result should be empty

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectXfooX_gteX1dX
    Given the empty graph
    And the traversal of
      """
      g.inject("foo").is(P.gte(1.0d))
      """
    When iterated to list
    Then the result should be empty

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectX1dX_eqXfooX
    Given the empty graph
    And the traversal of
      """
      g.inject(1.0d).is(P.eq("foo"))
      """
    When iterated to list
    Then the result should be empty

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectX1dX_neqXfooX
    Given the empty graph
    And the traversal of
      """
      g.inject(1.0d).is(P.neq("foo"))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[1].d |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectX1dX_ltXfooX
    Given the empty graph
    And the traversal of
      """
      g.inject(1.0d).is(P.lt("foo"))
      """
    When iterated to list
    Then the result should be empty

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectX1dX_lteXfooX
    Given the empty graph
    And the traversal of
      """
      g.inject(1.0d).is(P.lte("foo"))
      """
    When iterated to list
    Then the result should be empty

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectX1dX_gtXfooX
    Given the empty graph
    And the traversal of
      """
      g.inject(1.0d).is(P.gt("foo"))
      """
    When iterated to list
    Then the result should be empty

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectX1dX_gteXfooX
    Given the empty graph
    And the traversal of
      """
      g.inject(1.0d).is(P.gte("foo"))
      """
    When iterated to list
    Then the result should be empty

  ######################################################
  ## AND
  ######################################################

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectX1dX_andXtrue_trueX
    Given the empty graph
    And the traversal of
      """
      g.inject(1d).and(is(P.eq(1)),is(P.gt(0)))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[1].d |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectX1dX_isXtrue_trueX
    Given the empty graph
    And the traversal of
      """
      g.inject(1d).is(P.eq(1).and(P.gt(0)))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[1].d |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectX1dX_andXtrue_falseX
    Given the empty graph
    And the traversal of
      """
      g.inject(1d).and(is(P.eq(1)),is(P.lt(0)))
      """
    When iterated to list
    Then the result should be empty

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectX1dX_isXtrue_falseX
    Given the empty graph
    And the traversal of
      """
      g.inject(1d).is(P.eq(1).and(P.lt(0)))
      """
    When iterated to list
    Then the result should be empty

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectX1dX_andXtrue_errorX
    Given the empty graph
    And the traversal of
      """
      g.inject(1d).and(is(P.eq(1)),is(P.lt(NaN)))
      """
    When iterated to list
    Then the result should be empty

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectX1dX_isXtrue_errorX
    Given the empty graph
    And the traversal of
      """
      g.inject(1d).is(P.eq(1).and(P.lt(NaN)))
      """
    When iterated to list
    Then the result should be empty

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectX1dX_andXfalse_trueX
    Given the empty graph
    And the traversal of
      """
      g.inject(1d).and(is(P.neq(1)),is(P.gt(0)))
      """
    When iterated to list
    Then the result should be empty

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectX1dX_isXfalse_trueX
    Given the empty graph
    And the traversal of
      """
      g.inject(1d).is(P.neq(1).and(P.gt(0)))
      """
    When iterated to list
    Then the result should be empty

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectX1dX_andXfalse_falseX
    Given the empty graph
    And the traversal of
      """
      g.inject(1d).and(is(P.neq(1)),is(P.lt(0)))
      """
    When iterated to list
    Then the result should be empty

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectX1dX_isXfalse_falseX
    Given the empty graph
    And the traversal of
      """
      g.inject(1d).is(P.neq(1).and(P.lt(0)))
      """
    When iterated to list
    Then the result should be empty

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectX1dX_andXfalse_errorX
    Given the empty graph
    And the traversal of
      """
      g.inject(1d).and(is(P.neq(1)),is(P.lt(NaN)))
      """
    When iterated to list
    Then the result should be empty

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectX1dX_isXfalse_errorX
    Given the empty graph
    And the traversal of
      """
      g.inject(1d).is(P.neq(1).and(P.lt(NaN)))
      """
    When iterated to list
    Then the result should be empty

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectX1dX_andXerror_trueX
    Given the empty graph
    And the traversal of
      """
      g.inject(1d).and(is(P.lt(NaN)),is(P.gt(0)))
      """
    When iterated to list
    Then the result should be empty

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectX1dX_isXerror_trueX
    Given the empty graph
    And the traversal of
      """
      g.inject(1d).is(P.lt(NaN).and(P.gt(0)))
      """
    When iterated to list
    Then the result should be empty

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectX1dX_andXerror_falseX
    Given the empty graph
    And the traversal of
      """
      g.inject(1d).and(is(P.lt(NaN)),is(P.gt(2)))
      """
    When iterated to list
    Then the result should be empty

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectX1dX_isXerror_falseX
    Given the empty graph
    And the traversal of
      """
      g.inject(1d).is(P.lt(NaN).and(P.gt(2)))
      """
    When iterated to list
    Then the result should be empty

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectX1dX_andXerror_errorX
    Given the empty graph
    And the traversal of
      """
      g.inject(1d).and(is(P.lt(NaN)),is(P.gt(NaN)))
      """
    When iterated to list
    Then the result should be empty

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectX1dX_isXerror_errorX
    Given the empty graph
    And the traversal of
      """
      g.inject(1d).is(P.lt(NaN).and(P.gt(NaN)))
      """
    When iterated to list
    Then the result should be empty

  ######################################################
  ## OR
  ######################################################

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectX1dX_orXtrue_trueX
    Given the empty graph
    And the traversal of
      """
      g.inject(1d).or(is(P.eq(1)),is(P.gt(0)))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[1].d |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectX1dX_isXtrue_or_trueX
    Given the empty graph
    And the traversal of
      """
      g.inject(1d).is(P.eq(1).or(P.gt(0)))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[1].d |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectX1dX_orXtrue_falseX
    Given the empty graph
    And the traversal of
      """
      g.inject(1d).or(is(P.eq(1)),is(P.lt(0)))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[1].d |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectX1dX_isXtrue_or_falseX
    Given the empty graph
    And the traversal of
      """
      g.inject(1d).is(P.eq(1).or(P.lt(0)))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[1].d |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectX1dX_orXtrue_errorX
    Given the empty graph
    And the traversal of
      """
      g.inject(1d).or(is(P.eq(1)),is(P.lt(NaN)))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[1].d |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectX1dX_isXtrue_or_errorX
    Given the empty graph
    And the traversal of
      """
      g.inject(1d).is(P.eq(1).or(P.lt(NaN)))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[1].d |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectX1dX_orXfalse_trueX
    Given the empty graph
    And the traversal of
      """
      g.inject(1d).or(is(P.neq(1)),is(P.gt(0)))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[1].d |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectX1dX_isXfalse_or_trueX
    Given the empty graph
    And the traversal of
      """
      g.inject(1d).is(P.neq(1).or(P.gt(0)))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[1].d |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectX1dX_orXfalse_falseX
    Given the empty graph
    And the traversal of
      """
      g.inject(1d).or(is(P.neq(1)),is(P.lt(0)))
      """
    When iterated to list
    Then the result should be empty

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectX1dX_isXfalse_or_falseX
    Given the empty graph
    And the traversal of
      """
      g.inject(1d).is(P.neq(1).or(P.lt(0)))
      """
    When iterated to list
    Then the result should be empty

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectX1dX_orXfalse_errorX
    Given the empty graph
    And the traversal of
      """
      g.inject(1d).or(is(P.neq(1)),is(P.lt(NaN)))
      """
    When iterated to list
    Then the result should be empty

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectX1dX_isXfalse_or_errorX
    Given the empty graph
    And the traversal of
      """
      g.inject(1d).is(P.neq(1).or(P.lt(NaN)))
      """
    When iterated to list
    Then the result should be empty

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectX1dX_orXerror_trueX
    Given the empty graph
    And the traversal of
      """
      g.inject(1d).or(is(P.lt(NaN)),is(P.gt(0)))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[1].d |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectX1dX_isXerror_or_trueX
    Given the empty graph
    And the traversal of
      """
      g.inject(1d).is(P.lt(NaN).or(P.gt(0)))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[1].d |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectX1dX_orXerror_falseX
    Given the empty graph
    And the traversal of
      """
      g.inject(1d).or(is(P.lt(NaN)),is(P.gt(2)))
      """
    When iterated to list
    Then the result should be empty

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectX1dX_isXerror_or_falseX
    Given the empty graph
    And the traversal of
      """
      g.inject(1d).is(P.lt(NaN).or(P.gt(2)))
      """
    When iterated to list
    Then the result should be empty

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectX1dX_orXerror_errorX
    Given the empty graph
    And the traversal of
      """
      g.inject(1d).or(is(P.lt(NaN)),is(P.gt(NaN)))
      """
    When iterated to list
    Then the result should be empty

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectX1dX_isXerror_or_errorX
    Given the empty graph
    And the traversal of
      """
      g.inject(1d).is(P.lt(NaN).or(P.gt(NaN)))
      """
    When iterated to list
    Then the result should be empty

  ######################################################
  ## NOT
  ######################################################

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectX1dX_notXtrueX
    Given the empty graph
    And the traversal of
      """
      g.inject(1d).not(is(P.gt(0)))
      """
    When iterated to list
    Then the result should be empty

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectX1dX_notXfalseX
    Given the empty graph
    And the traversal of
      """
      g.inject(1d).not(is(P.lt(0)))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[1].d |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectX1dX_notXerrorX
    Given the empty graph
    And the traversal of
      """
      g.inject(1d).not(is(P.gt(NaN)))
      """
    When iterated to list
    Then the result should be empty

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectX1dX_notXisXeqXNaNXXX
    Given the empty graph
    And the traversal of
      """
      g.inject(1d).not(is(P.eq(NaN)))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[1].d |

  ######################################################
  ## Infinity vs. Infinity
  ######################################################
    
  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectXInfX_eqXInfX
    Given the empty graph
    And the traversal of
      """
      g.inject(Infinity).is(P.eq(+Infinity))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[Infinity] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectXInfArgX_eqXInfX
    Given the empty graph
    And using the parameter xx1 defined as "d[Infinity]"
    And the traversal of
      """
      g.inject(xx1).is(P.eq(+Infinity))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[Infinity] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectXInfX_neqXInfX
    Given the empty graph
    And the traversal of
      """
      g.inject(Infinity).is(P.neq(+Infinity))
      """
    When iterated to list
    Then the result should be empty

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectXInfArgX_neqXInfX
    Given the empty graph
    And using the parameter xx1 defined as "d[Infinity]"
    And the traversal of
      """
      g.inject(xx1).is(P.neq(+Infinity))
      """
    When iterated to list
    Then the result should be empty

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectXNegInfX_eqXNegInfX
    Given the empty graph
    And the traversal of
      """
      g.inject(-Infinity).is(P.eq(-Infinity))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[-Infinity] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectXNegInfArgX_eqXNegInfX
    Given the empty graph
    And using the parameter xx1 defined as "d[-Infinity]"
    And the traversal of
      """
      g.inject(xx1).is(P.eq(-Infinity))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[-Infinity] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectXNegInfX_neqXNegInfX
    Given the empty graph
    And the traversal of
      """
      g.inject(-Infinity).is(P.neq(-Infinity))
      """
    When iterated to list
    Then the result should be empty

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectXNegInfArgX_neqXNegInfX
    Given the empty graph
    And using the parameter xx1 defined as "d[-Infinity]"
    And the traversal of
      """
      g.inject(xx1).is(P.neq(-Infinity))
      """
    When iterated to list
    Then the result should be empty

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectXInfX_gtXNegInfX
    Given the empty graph
    And the traversal of
      """
      g.inject(Infinity).is(P.gt(-Infinity))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[Infinity] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectXInfX_ltXNegInfX
    Given the empty graph
    And the traversal of
      """
      g.inject(Infinity).is(P.lt(-Infinity))
      """
    When iterated to list
    Then the result should be empty

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectXNegInfX_ltXInfX
    Given the empty graph
    And the traversal of
      """
      g.inject(-Infinity).is(P.lt(Infinity))
      """
    When iterated to list
    Then the result should be unordered
      | result |
      | d[-Infinity] |

  @GraphComputerVerificationInjectionNotSupported
  Scenario: InjectXNegInfX_gtXInfX
    Given the empty graph
    And the traversal of
      """
      g.inject(-Infinity).is(P.gt(Infinity))
      """
    When iterated to list
    Then the result should be empty





