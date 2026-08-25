/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration;

import org.apache.commons.configuration2.MapConfiguration;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedFactory;
import org.junit.Test;

import java.util.Collections;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThrows;

public class HaltedTraverserStrategyTest {

    private static boolean unapprovedFactoryInitialized = false;

    @Test
    public void shouldCreateStrategyForSupportedFactory() {
        assertThat(create(DetachedFactory.class.getName()).getHaltedTraverserFactory(), is(DetachedFactory.class));
    }

    @Test
    public void shouldRejectUnsupportedFactoryWithoutLoadingIt() {
        final String unapprovedFactory = HaltedTraverserStrategyTest.class.getName() + "$UnapprovedFactory";

        assertThrows(IllegalArgumentException.class, () -> create(unapprovedFactory));
        assertThat(unapprovedFactoryInitialized, is(false));
    }

    private static HaltedTraverserStrategy create(final String factory) {
        return HaltedTraverserStrategy.create(new MapConfiguration(
                Collections.singletonMap(HaltedTraverserStrategy.HALTED_TRAVERSER_FACTORY, factory)));
    }

    private static final class UnapprovedFactory {
        static {
            unapprovedFactoryInitialized = true;
        }
    }
}
