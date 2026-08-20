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
package org.apache.tinkerpop.gremlin.process.traversal.strategy.finalization;

import org.apache.commons.configuration2.MapConfiguration;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchStep;
import org.junit.Test;

import java.util.Collections;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertThrows;

public class MatchAlgorithmStrategyTest {

    private static boolean loadableProviderAlgorithmInitialized = false;
    private static boolean invalidAlgorithmInitialized = false;

    @Test
    public void shouldCreateStrategyForDefaultMatchAlgorithm() {
        final String defaultAlgorithm = MatchStep.CountMatchAlgorithm.class.getName();
        final MatchAlgorithmStrategy strategy = create(defaultAlgorithm);

        assertThat(strategy.getConfiguration().getString("matchAlgorithm"), is(defaultAlgorithm));
    }

    @Test
    public void shouldLoadValidProviderMatchAlgorithm() {
        final String providerAlgorithm = LoadableProviderMatchAlgorithm.class.getName();
        final Traversal.Admin<?, ?> traversal = __.match(__.as("a").out().as("b")).asAdmin();

        create(providerAlgorithm).apply(traversal);
        final MatchStep<?, ?> matchStep = (MatchStep<?, ?>) traversal.getStartStep();

        assertThat(matchStep.getMatchAlgorithm(), instanceOf(LoadableProviderMatchAlgorithm.class));
        assertThat(loadableProviderAlgorithmInitialized, is(true));
    }

    @Test
    public void shouldRejectInvalidMatchAlgorithmWithoutInitializingIt() {
        final String invalidAlgorithm = InvalidAlgorithm.class.getName();

        assertThrows(IllegalArgumentException.class, () -> create(invalidAlgorithm));
        assertThat(invalidAlgorithmInitialized, is(false));
    }

    private static MatchAlgorithmStrategy create(final String matchAlgorithm) {
        return MatchAlgorithmStrategy.create(new MapConfiguration(
                Collections.singletonMap("matchAlgorithm", matchAlgorithm)));
    }

    public static final class LoadableProviderMatchAlgorithm extends MatchStep.CountMatchAlgorithm {
        static {
            loadableProviderAlgorithmInitialized = true;
        }
    }

    private static final class InvalidAlgorithm {
        static {
            invalidAlgorithmInitialized = true;
        }
    }
}
