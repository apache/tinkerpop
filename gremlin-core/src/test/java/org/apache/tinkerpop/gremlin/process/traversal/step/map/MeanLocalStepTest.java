/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.process.traversal.step.map;

import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.StepTest;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public class MeanLocalStepTest extends StepTest {

    @Override
    protected List<Traversal> getTraversals() {
        return Collections.singletonList(__.mean(Scope.local));
    }

    @Test
    public void testReturnTypes() {
        assertEquals(3d, __.__(2, 4).fold().mean(Scope.local).next());
        assertEquals(3d, __.__((short) 2, (short) 4).fold().mean(Scope.local).next()); // why? because the internal counter is a Long value
        assertEquals(BigDecimal.ONE, __.__(BigInteger.ONE, BigInteger.ONE).fold().mean(Scope.local).next());
        assertEquals(BigDecimal.ONE, __.__((short) 1, BigInteger.ONE).fold().mean(Scope.local).next());
        assertEquals(BigDecimal.ONE, __.__(BigInteger.ONE, (short) 1).fold().mean(Scope.local).next());
    }
}
