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
package org.apache.tinkerpop.machine.processor.pipes;

import org.apache.tinkerpop.language.gremlin.Gremlin;
import org.apache.tinkerpop.language.gremlin.Traversal;
import org.apache.tinkerpop.language.gremlin.TraversalSource;
import org.apache.tinkerpop.language.gremlin.TraversalUtil;
import org.apache.tinkerpop.language.gremlin.core.__;
import org.apache.tinkerpop.machine.Machine;
import org.apache.tinkerpop.machine.coefficient.LongCoefficient;
import org.apache.tinkerpop.machine.species.LocalMachine;
import org.apache.tinkerpop.machine.strategy.optimization.IdentityStrategy;
import org.apache.tinkerpop.machine.structure.blueprints.BlueprintsStructure;
import org.junit.jupiter.api.Test;

import static org.apache.tinkerpop.language.gremlin.core.__.count;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class PipesTest {

    @Test
    void doStuff() {
        final Machine machine = LocalMachine.open();
        final TraversalSource<Long> g = Gremlin.<Long>traversal(machine)
                .withCoefficient(LongCoefficient.class)
                .withProcessor(PipesProcessor.class)
                .withStructure(BlueprintsStructure.class)
                .withStrategy(IdentityStrategy.class);

        Traversal<Long, ?, ?> traversal = g.V().identity().union(count(), count()).map(__.<Long, Long>count().identity()).explain();
        System.out.println(TraversalUtil.getBytecode(traversal));
        System.out.println(traversal);
        System.out.println(TraversalUtil.getBytecode(traversal));
        System.out.println(traversal.next());
        System.out.println("\n----------\n");
    }

}
