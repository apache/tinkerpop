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
package org.apache.tinkerpop.machine;

import org.apache.tinkerpop.language.Gremlin;
import org.apache.tinkerpop.language.Traversal;
import org.apache.tinkerpop.language.TraversalSource;
import org.apache.tinkerpop.machine.coefficients.LongCoefficients;
import org.apache.tinkerpop.machine.compiler.BytecodeToFunction;
import org.junit.jupiter.api.Test;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TraversalTest {

    @Test
    public void shouldHaveBytecode() throws Exception {
        TraversalSource<Long> g = Gremlin.traversal();
        g = g.coefficients(LongCoefficients.instance());
        final Traversal<Long, Long, Long> traversal = g.inject(7L).is(7L).incr().incr();
        System.out.println(traversal.getBytecode());
        System.out.println(BytecodeToFunction.compile(traversal.getBytecode()));
    }
}
