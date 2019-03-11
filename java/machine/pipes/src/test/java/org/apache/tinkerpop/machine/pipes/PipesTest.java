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
package org.apache.tinkerpop.machine.pipes;

import org.apache.tinkerpop.language.Gremlin;
import org.apache.tinkerpop.language.Traversal;
import org.apache.tinkerpop.language.TraversalSource;
import org.apache.tinkerpop.language.TraversalUtil;
import org.apache.tinkerpop.language.__;
import org.apache.tinkerpop.machine.coefficients.LongCoefficient;
import org.apache.tinkerpop.machine.strategies.IdentityStrategy;
import org.junit.jupiter.api.Test;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PipesTest {

    @Test
    public void shouldWork() {
        final TraversalSource<Long> g = Gremlin.<Long>traversal()
                .withCoefficient(LongCoefficient.class)
                .withProcessor(PipesProcessor.class)
                .withStrategy(IdentityStrategy.class);

        Traversal<Long, Long, Long> traversal = g.inject(7L, 10L, 12L).as("a").c(3L).map(__.incr()).identity().incr().identity().identity();
        System.out.println(TraversalUtil.getBytecode(traversal));
        System.out.println(traversal);
        System.out.println(traversal.toList());
        System.out.println("\n----------\n");
        traversal = g.inject(7L, 10L, 12L).as("a").c(3L).map(__.incr()).identity().incr().is(44L).count();
        System.out.println(TraversalUtil.getBytecode(traversal));
        System.out.println(traversal);
        System.out.println(traversal.toList());
    }
}
