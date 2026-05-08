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
package org.apache.tinkerpop.gremlin.driver;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;

public class HostHealthTrackerTest {

    @Test
    public void shouldMarkUnavailableAfterThresholdFailures() {
        final HostHealthTracker tracker = new HostHealthTracker(3);
        final Host host = mock(Host.class);

        assertThat(tracker.isAvailable(host), is(true));
        assertThat(tracker.recordFailure(host), is(false));
        assertThat(tracker.isAvailable(host), is(true));
        assertThat(tracker.recordFailure(host), is(false));
        assertThat(tracker.isAvailable(host), is(true));
        assertThat(tracker.recordFailure(host), is(true));
        assertThat(tracker.isAvailable(host), is(false));
    }

    @Test
    public void shouldResetOnSuccess() {
        final HostHealthTracker tracker = new HostHealthTracker(3);
        final Host host = mock(Host.class);

        tracker.recordFailure(host);
        tracker.recordFailure(host);
        assertThat(tracker.isAvailable(host), is(true));

        tracker.recordSuccess(host);
        assertThat(tracker.isAvailable(host), is(true));

        // after reset, need full threshold again
        assertThat(tracker.recordFailure(host), is(false));
        assertThat(tracker.recordFailure(host), is(false));
        assertThat(tracker.recordFailure(host), is(true));
    }

    @Test
    public void shouldTrackMultipleHostsIndependently() {
        final HostHealthTracker tracker = new HostHealthTracker(2);
        final Host host1 = mock(Host.class);
        final Host host2 = mock(Host.class);

        tracker.recordFailure(host1);
        assertThat(tracker.isAvailable(host1), is(true));
        assertThat(tracker.isAvailable(host2), is(true));

        tracker.recordFailure(host1);
        assertThat(tracker.isAvailable(host1), is(false));
        assertThat(tracker.isAvailable(host2), is(true));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectZeroThreshold() {
        new HostHealthTracker(0);
    }

    @Test
    public void shouldHandleSuccessWithNoFailures() {
        final HostHealthTracker tracker = new HostHealthTracker(3);
        final Host host = mock(Host.class);

        // recordSuccess on a host that has never failed should not throw
        tracker.recordSuccess(host);
        assertThat(tracker.isAvailable(host), is(true));
    }
}
