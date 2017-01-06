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

import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONCompatibility;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoCompatibility;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class CompatibilitiesTest {
    @Test
    public void shouldFindGryoVersionsBeforeRelease3_2_4() {
        final List<Compatibility> compatibilityList = Compatibilities.with(GryoCompatibility.class)
                .beforeRelease("3.2.4").match();
        assertThat(compatibilityList, containsInAnyOrder(GryoCompatibility.V1D0_3_2_3));
    }

    @Test
    public void shouldFindGryoVersionsAfterRelease3_2_4() {
        final List<Compatibility> compatibilityList = Compatibilities.with(GryoCompatibility.class)
                .afterRelease("3.2.4").match();
        assertThat(compatibilityList, containsInAnyOrder(GryoCompatibility.V1D0_3_3_0, GryoCompatibility.V3D0_3_3_0));
    }

    @Test
    public void shouldFindGryoVersionsBetweenReleases3_2_3And3_3_0() {
        final List<Compatibility> compatibilityList = Compatibilities.with(GryoCompatibility.class)
                .betweenReleases("3.2.3", "3.3.0").match();
        assertThat(compatibilityList, containsInAnyOrder(GryoCompatibility.V1D0_3_2_4));
    }

    @Test
    public void shouldFindGryoVersionsBefore3_0() {
        final List<Compatibility> compatibilityList = Compatibilities.with(GryoCompatibility.class)
                .before("3.0").match();
        assertThat(compatibilityList, containsInAnyOrder(GryoCompatibility.V1D0_3_2_3, GryoCompatibility.V1D0_3_2_4, GryoCompatibility.V1D0_3_3_0));
    }

    @Test
    public void shouldFindGryoVersionsAfter1_0() {
        final List<Compatibility> compatibilityList = Compatibilities.with(GryoCompatibility.class)
                .after("1.0").match();
        assertThat(compatibilityList, containsInAnyOrder(GryoCompatibility.V3D0_3_3_0));
    }

    @Test
    public void shouldFindGryoVersionsAfterRelease3_2_4AndAfter1_0() {
        final List<Compatibility> compatibilityList = Compatibilities.with(GryoCompatibility.class)
                .afterRelease("3.2.4")
                .after("1.0")
                .match();
        assertThat(compatibilityList, containsInAnyOrder(GryoCompatibility.V3D0_3_3_0));
    }

    @Test
    public void shouldFindGraphSONWithConfigurationPartial() {
        final List<Compatibility> compatibilityList = Compatibilities.with(GraphSONCompatibility.class)
                .configuredAs(".*partial.*").match();
        assertThat(compatibilityList, containsInAnyOrder(GraphSONCompatibility.V2D0_PARTIAL_3_2_3,
                GraphSONCompatibility.V2D0_PARTIAL_3_2_4, GraphSONCompatibility.V2D0_PARTIAL_3_3_0));
    }

    @Test
    public void shouldFindGraphSONAfterVersion3_2_3WithConfigurationPartial() {
        final List<Compatibility> compatibilityList = Compatibilities.with(GraphSONCompatibility.class)
                .afterRelease("3.2.4")
                .configuredAs(".*partial.*").match();
        assertThat(compatibilityList, containsInAnyOrder(GraphSONCompatibility.V2D0_PARTIAL_3_3_0));
    }

    @Test
    public void shouldJoinCompatibilities() {
        final List<Compatibility> compatibilityList = Compatibilities.with(GryoCompatibility.class)
                .afterRelease("3.2.4")
                .after("1.0")
                .join(Compatibilities.with(GraphSONCompatibility.class)
                        .configuredAs(".*partial.*"))
                .match();
        assertThat(compatibilityList, containsInAnyOrder(GryoCompatibility.V3D0_3_3_0,
                GraphSONCompatibility.V2D0_PARTIAL_3_2_3, GraphSONCompatibility.V2D0_PARTIAL_3_2_4,
                GraphSONCompatibility.V2D0_PARTIAL_3_3_0));
    }
}
