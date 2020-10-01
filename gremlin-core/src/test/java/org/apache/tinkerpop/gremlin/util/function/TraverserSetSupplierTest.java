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
package org.apache.tinkerpop.gremlin.util.function;

import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;

/**
 * @author Norio Akagi
 */
public class TraverserSetSupplierTest {
    @Test
    public void shouldSupplyTraverserSet() {
        assertThat(TraverserSetSupplier.instance().get(), hasSize(0));
    }

    @Test
    public void shouldSupplyTraverserSetInstance() {
        assertThat(TraverserSetSupplier.instance().get(), hasSize(0));
        assertThat(TraverserSetSupplier.instance().get(), instanceOf(TraverserSet.class));
    }

    @Test
    public void shouldSupplyNewTraverserSetOnEachInvocation() {
        final TraverserSet<Object> ts1 = TraverserSetSupplier.instance().get();
        final TraverserSet<Object> ts2 = TraverserSetSupplier.instance().get();
        final TraverserSet<Object> ts3 = TraverserSetSupplier.instance().get();

        assertThat(ts1, not(sameInstance(ts2)));
        assertThat(ts1, not(sameInstance(ts3)));
        assertThat(ts2, not(sameInstance(ts3)));
    }
}
