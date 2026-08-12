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
package org.apache.tinkerpop.tinkeradoc;

import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class GraphCatalogTest {

    @Test
    public void shouldExposeInitStatementDisplayAndAnchorForKnownGraph() {
        final GraphCatalog.Entry modern = GraphCatalog.entry("modern");
        assertThat(modern.initStatement, is("graph = TinkerFactory.createModern()"));
        assertThat(modern.displayName, is("modern"));
        assertThat(modern.docAnchor, is("tinkerpop-modern"));
    }

    @Test
    public void shouldTreatCrewAndTheCrewAsTheSameDataset() {
        final GraphCatalog.Entry crew = GraphCatalog.entry("crew");
        final GraphCatalog.Entry theCrew = GraphCatalog.entry("theCrew");
        assertThat(crew.initStatement, is("graph = TinkerFactory.createTheCrew()"));
        assertThat(theCrew.initStatement, is(crew.initStatement));
        assertThat(theCrew.displayName, is(crew.displayName));
        assertThat(theCrew.docAnchor, is(crew.docAnchor));
    }

    @Test
    public void shouldReportNullAnchorForGraphWithoutReferenceSection() {
        assertThat(GraphCatalog.entry("classic").docAnchor, nullValue());
        assertThat(GraphCatalog.entry("sink").docAnchor, nullValue());
    }

    @Test
    public void shouldReturnNoEntryForNullOrUnknownToken() {
        assertThat(GraphCatalog.entry(null), nullValue());
        assertThat(GraphCatalog.entry("bogus"), nullValue());
    }

    @Test
    public void shouldFallBackToEmptyGraphInitForNullOrUnknownToken() {
        assertThat(GraphCatalog.initStatement(null), is(GraphCatalog.DEFAULT_INIT));
        assertThat(GraphCatalog.initStatement("bogus"), is(GraphCatalog.DEFAULT_INIT));
        assertThat(GraphCatalog.DEFAULT_INIT, is("graph = TinkerGraph.open()"));
    }
}
