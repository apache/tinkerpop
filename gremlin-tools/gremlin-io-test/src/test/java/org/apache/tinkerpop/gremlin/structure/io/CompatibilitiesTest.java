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
package org.apache.tinkerpop.gremlin.structure.io;

import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class CompatibilitiesTest {

    @Test
    public void shouldFindVersionsBeforeRelease3_2_4() {
        final List<Compatibility> compatibilityList = Compatibilities.with(MockCompatibility.class)
                .beforeRelease("3.2.4").match();
        assertThat(compatibilityList, containsInAnyOrder(MockCompatibility.V1D0_3_2_3,
                MockCompatibility.V1D0_3_2_2, MockCompatibility.V1D0_3_2_2_PARTIAL));
    }

    @Test
    public void shouldFindVersionsAfterRelease3_2_x() {
        final List<Compatibility> compatibilityList = Compatibilities.with(MockCompatibility.class)
                .afterRelease("3.2.7").match();
        assertThat(compatibilityList, containsInAnyOrder(
                MockCompatibility.V1D0_3_3_0, MockCompatibility.V1D0_3_3_1,
                MockCompatibility.V3D0_3_3_0, MockCompatibility.V3D0_3_3_1));
    }

    @Test
    public void shouldFindVersionsBetweenReleases3_2_3And3_2_5() {
        final List<Compatibility> compatibilityList = Compatibilities.with(MockCompatibility.class)
                .betweenReleases("3.2.3", "3.2.5").match();
        assertThat(compatibilityList, containsInAnyOrder(MockCompatibility.V1D0_3_2_4,
                MockCompatibility.V1D0_3_2_4_PARTIAL));
    }

    @Test
    public void shouldFindVersionsBefore3_0() {
        final List<Compatibility> compatibilityList = Compatibilities.with(MockCompatibility.class)
                .before("3.0").match();
        assertThat(compatibilityList, containsInAnyOrder(MockCompatibility.V1D0_3_2_2,
                MockCompatibility.V1D0_3_2_2_PARTIAL,
                MockCompatibility.V1D0_3_2_4_PARTIAL,
                MockCompatibility.V1D0_3_2_3,
                MockCompatibility.V1D0_3_2_4, MockCompatibility.V1D0_3_2_5,
                MockCompatibility.V1D0_3_2_6, MockCompatibility.V1D0_3_3_0, MockCompatibility.V1D0_3_3_1));
    }

    @Test
    public void shouldFindVersionsAfter1_0() {
        final List<Compatibility> compatibilityList = Compatibilities.with(MockCompatibility.class)
                .after("1.0").match();
        assertThat(compatibilityList, containsInAnyOrder(MockCompatibility.V3D0_3_3_0, MockCompatibility.V3D0_3_3_1));
    }

    @Test
    public void shouldFindVersionsAfterRelease3_2_4AndAfter1_0() {
        final List<Compatibility> compatibilityList = Compatibilities.with(MockCompatibility.class)
                .afterRelease("3.2.7")
                .after("1.0")
                .match();
        assertThat(compatibilityList, containsInAnyOrder(MockCompatibility.V3D0_3_3_0, MockCompatibility.V3D0_3_3_1));
    }

    @Test
    public void shouldFindGraphSONWithConfigurationPartial() {
        final List<Compatibility> compatibilityList = Compatibilities.with(MockCompatibility.class)
                .configuredAs(".*partial.*").match();
        assertThat(compatibilityList, containsInAnyOrder(MockCompatibility.V1D0_3_2_4_PARTIAL, MockCompatibility.V1D0_3_2_2_PARTIAL));
    }

    @Test
    public void shouldFindGraphSONAfterVersion3_2_3WithConfigurationPartial() {
        final List<Compatibility> compatibilityList = Compatibilities.with(MockCompatibility.class)
                .afterRelease("3.2.3")
                .configuredAs(".*partial.*").match();
        assertThat(compatibilityList, containsInAnyOrder(MockCompatibility.V1D0_3_2_4_PARTIAL));
    }

    @Test
    public void shouldJoinCompatibilities() {
        final List<Compatibility> compatibilityList = Compatibilities.with(MockCompatibility.class)
                .afterRelease("3.3.0")
                .after("1.0")
                .join(Compatibilities.with(MockCompatibility.class)
                        .configuredAs(".*partial.*"))
                .match();
        assertThat(compatibilityList, containsInAnyOrder(
                MockCompatibility.V1D0_3_2_4_PARTIAL, MockCompatibility.V1D0_3_2_2_PARTIAL,
                MockCompatibility.V3D0_3_3_1));
    }

    enum MockCompatibility implements Compatibility {
        V1D0_3_2_2("3.2.2", "1.0", "v1d0"),
        V1D0_3_2_2_PARTIAL("3.2.2", "1.0", "v1d0-partial"),
        V1D0_3_2_3("3.2.3", "1.0", "v1d0"),
        V1D0_3_2_4("3.2.4", "1.0", "v1d0"),
        V1D0_3_2_4_PARTIAL("3.2.4", "1.0", "v1d0-partial"),
        V1D0_3_2_5("3.2.5", "1.0", "v1d0"),
        V1D0_3_2_6("3.2.6", "1.0", "v1d0"),
        V1D0_3_3_0("3.3.0", "1.0", "v1d0"),
        V3D0_3_3_0("3.3.0", "3.0", "v3d0"),
        V1D0_3_3_1("3.3.1", "1.0", "v1d0"),
        V3D0_3_3_1("3.3.1", "3.0", "v3d0");

        private final String mockVersion;
        private final String tinkerpopVersion;
        private final String configuration;

        MockCompatibility(final String tinkerpopVersion, final String mockVersion, final String configuration) {
            this.tinkerpopVersion = tinkerpopVersion;
            this.mockVersion = mockVersion;
            this.configuration = configuration;
        }

        @Override
        public byte[] readFromResource(final String resource) throws IOException {
            return new byte[0];
        }

        @Override
        public String getReleaseVersion() {
            return tinkerpopVersion;
        }

        @Override
        public String getVersion() {
            return mockVersion;
        }

        @Override
        public String getConfiguration() {
            return configuration;
        }

        @Override
        public String toString() {
            return tinkerpopVersion + "-" + configuration;
        }
    }
}
