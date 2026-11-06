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
package org.apache.tinkerpop.gremlin.hadoop.structure.io.util;

import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class OlapClassLoadingPolicyTest {

    @Test
    public void shouldReportMembership() {
        final OlapClassLoadingPolicy policy = OlapClassLoadingPolicy.build().approve("a.B", "c.D").create();
        assertTrue(policy.isApproved("a.B"));
        assertTrue(policy.isApproved("c.D"));
        assertFalse(policy.isApproved("x.Y"));
        assertFalse(policy.isApproved(null));
    }

    @Test
    public void shouldResolveApprovedAssignableClass() {
        final OlapClassLoadingPolicy policy = OlapClassLoadingPolicy.build().approve(ArrayList.class).create();
        final Class<? extends List> resolved = policy.resolve("java.util.ArrayList", List.class);
        assertEquals(ArrayList.class, resolved);
    }

    @Test
    public void shouldRejectUnapprovedClass() {
        final OlapClassLoadingPolicy policy = OlapClassLoadingPolicy.build().approve("java.util.ArrayList").create();
        try {
            policy.resolve("java.util.LinkedList", List.class);
            fail("resolve() must reject a class that is not on the allow-list");
        } catch (final IllegalArgumentException iae) {
            assertTrue(iae.getMessage().contains(OlapClassLoadingPolicy.APPROVED_CLASSES));
        }
    }

    /**
     * Pins two security invariants: (a) an unapproved name is rejected <em>before</em> a class loader is consulted, so
     * its static initializer never runs; and (b) resolving an approved name loads it <em>without</em> initializing it
     * (non-initializing {@code Class.forName}), so a class that would fail the assignability check never runs its
     * initializer either. Uses a probe class referenced only by name (never as a class literal, so it is not loaded by
     * this test) whose static initializer records a side effect. Part (c) is a positive control proving the side effect
     * is genuinely observable, so (a) and (b) are not vacuous.
     */
    @Test
    public void shouldRejectUnapprovedBeforeLoadingAndResolveApprovedWithoutInitializing() throws Exception {
        final String probeName = OlapClassLoadingPolicyTest.class.getName() + "$InitProbe";
        assertFalse("probe must start uninitialized", PROBE_INITIALIZED);

        // (a) unapproved -> rejected before Class.forName -> initializer must not run
        final OlapClassLoadingPolicy empty = OlapClassLoadingPolicy.build().create();
        try {
            empty.resolve(probeName, Object.class);
            fail("resolve() must reject the unapproved probe");
        } catch (final IllegalArgumentException iae) {
            assertTrue(iae.getMessage().contains(OlapClassLoadingPolicy.APPROVED_CLASSES));
        }
        assertFalse("unapproved probe must be rejected WITHOUT loading/initializing it", PROBE_INITIALIZED);

        // (b) approved -> resolve() loads it but does NOT initialize it (non-initializing load)
        final OlapClassLoadingPolicy approved = OlapClassLoadingPolicy.build().approve(probeName).create();
        final Class<?> resolved = approved.resolve(probeName, Object.class);
        assertEquals(probeName, resolved.getName());
        assertFalse("resolve() must load the approved class WITHOUT running its static initializer", PROBE_INITIALIZED);

        // (c) positive control: actually using the class initializes it -- proves the probe is observable
        Class.forName(probeName); // initializing load
        assertTrue("using the resolved class must run its initializer", PROBE_INITIALIZED);
    }

    @Test
    public void shouldRejectApprovedButNotAssignableClass() {
        final OlapClassLoadingPolicy policy = OlapClassLoadingPolicy.build().approve("java.lang.String").create();
        try {
            policy.resolve("java.lang.String", List.class);
            fail("resolve() must reject a class that is not assignable to the expected type");
        } catch (final IllegalArgumentException iae) {
            assertTrue(iae.getMessage().contains("is not a"));
        }
    }

    @Test
    public void shouldRejectApprovedButUnloadableClass() {
        final OlapClassLoadingPolicy policy = OlapClassLoadingPolicy.build().approve("does.not.Exist").create();
        try {
            policy.resolve("does.not.Exist", Object.class);
            fail("resolve() must fail when an approved class cannot be loaded");
        } catch (final IllegalArgumentException iae) {
            assertTrue(iae.getCause() instanceof ClassNotFoundException);
        }
    }

    @Test
    public void shouldApproveFromCommaSeparatedConfiguration() {
        final Configuration configuration = new BaseConfiguration();
        configuration.setProperty(OlapClassLoadingPolicy.APPROVED_CLASSES, "a.B, c.D ,e.F");
        final OlapClassLoadingPolicy policy = OlapClassLoadingPolicy.build().approveFrom(configuration).create();
        assertTrue(policy.isApproved("a.B"));
        assertTrue(policy.isApproved("c.D"));
        assertTrue(policy.isApproved("e.F"));
    }

    @Test
    public void shouldIgnoreBlankEntriesInCommaSeparatedConfiguration() {
        final Configuration configuration = new BaseConfiguration();
        configuration.setProperty(OlapClassLoadingPolicy.APPROVED_CLASSES, "a.B,,c.D, ,");
        final OlapClassLoadingPolicy policy = OlapClassLoadingPolicy.build().approveFrom(configuration).create();
        assertTrue(policy.isApproved("a.B"));
        assertTrue(policy.isApproved("c.D"));
        assertFalse(policy.isApproved(""));
        assertEquals(2, policy.approvedClasses().size());
    }

    @Test
    public void shouldIgnoreNullClassEntries() {
        assertTrue(OlapClassLoadingPolicy.build().approve((Class<?>) null).create().approvedClasses().isEmpty());
    }

    @Test
    public void shouldApproveFromMultiValuedConfiguration() {
        final Configuration configuration = new BaseConfiguration();
        configuration.addProperty(OlapClassLoadingPolicy.APPROVED_CLASSES, Arrays.asList("a.B", "c.D"));
        final OlapClassLoadingPolicy policy = OlapClassLoadingPolicy.build().approveFrom(configuration).create();
        assertTrue(policy.isApproved("a.B"));
        assertTrue(policy.isApproved("c.D"));
    }

    @Test
    public void shouldIgnoreMissingConfigurationKey() {
        final OlapClassLoadingPolicy policy = OlapClassLoadingPolicy.build().approveFrom(new BaseConfiguration()).create();
        assertTrue(policy.approvedClasses().isEmpty());
    }

    @Test
    public void shouldAutoSeedFromConfiguredClassValuedKeys() {
        final Configuration configuration = new BaseConfiguration();
        configuration.setProperty("gremlin.hadoop.graphReader", "com.provider.Reader");
        configuration.setProperty("gremlin.hadoop.graphWriter", "com.provider.Writer");
        final OlapClassLoadingPolicy policy = OlapClassLoadingPolicy.build()
                .approveFromConfigValues(configuration, "gremlin.hadoop.graphReader", "gremlin.hadoop.graphWriter", "absent.key")
                .create();
        assertTrue(policy.isApproved("com.provider.Reader"));
        assertTrue(policy.isApproved("com.provider.Writer"));
        assertFalse(policy.isApproved("absent.key"));
    }

    @Test
    public void shouldDefaultToUntrusted() {
        assertFalse(OlapClassLoadingPolicy.isTrusted(new BaseConfiguration()));
        assertFalse(OlapClassLoadingPolicy.isTrusted(null));
    }

    @Test
    public void shouldReadTrustedFlagFromConfiguration() {
        final Configuration configuration = new BaseConfiguration();
        configuration.setProperty(OlapClassLoadingPolicy.TRUSTED, true);
        assertTrue(OlapClassLoadingPolicy.isTrusted(configuration));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldExposeApprovedNamesAsUnmodifiable() {
        OlapClassLoadingPolicy.build().approve("a.B").create().approvedClasses().add("c.D");
    }

    /** Set by {@link InitProbe}'s static initializer. Kept on the (already-loaded) test class so reading it does not
     * initialize the probe. */
    public static volatile boolean PROBE_INITIALIZED = false;

    /** A class that is never referenced as a class literal, so it is loaded only if something resolves it by name. Its
     * static initializer flips {@link #PROBE_INITIALIZED} so a test can observe whether it was loaded. */
    public static class InitProbe {
        static {
            PROBE_INITIALIZED = true;
        }
    }
}
