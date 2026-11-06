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

import java.util.Collections;

import static org.apache.tinkerpop.gremlin.hadoop.structure.io.util.OlapConfigKeyPolicy.APPROVED_COMPUTER_CONFIG_KEYS;
import static org.apache.tinkerpop.gremlin.hadoop.structure.io.util.OlapConfigKeyPolicy.APPROVED_GRAPH_CONFIG_KEYS;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Unit tests for the shared OLAP config-key policy used by both {@code configure()} and {@code io().with()}. The two
 * surfaces reuse the policy <em>mechanism</em> but keep <em>separate</em> operator lists.
 */
public class OlapConfigKeyPolicyTest {

    private static Configuration untrusted() {
        return new BaseConfiguration();
    }

    private static Configuration trusted() {
        final Configuration c = new BaseConfiguration();
        c.setProperty(OlapClassLoadingPolicy.TRUSTED, true);
        return c;
    }

    @Test
    public void shouldDenyUnapprovedKeyWhenUntrusted() {
        assertFalse(OlapConfigKeyPolicy.isConfigKeyPermitted(untrusted(), "spark.executor.extraJavaOptions",
                Collections.emptySet(), APPROVED_GRAPH_CONFIG_KEYS));
    }

    @Test
    public void shouldPermitBuiltinKeyWhenUntrusted() {
        assertTrue(OlapConfigKeyPolicy.isConfigKeyPermitted(untrusted(), "framework.key",
                Collections.singleton("framework.key"), APPROVED_COMPUTER_CONFIG_KEYS));
    }

    @Test
    public void shouldPermitOperatorApprovedKeyWhenUntrusted() {
        final Configuration c = untrusted();
        c.setProperty(APPROVED_GRAPH_CONFIG_KEYS, "my.graph.key");
        assertTrue(OlapConfigKeyPolicy.isConfigKeyPermitted(c, "my.graph.key",
                Collections.emptySet(), APPROVED_GRAPH_CONFIG_KEYS));
    }

    // The point of separate lists: approving a key for one surface must NOT open it on the other surface.
    @Test
    public void shouldKeepComputerAndGraphListsSeparate() {
        final Configuration computerApproved = untrusted();
        computerApproved.setProperty(APPROVED_COMPUTER_CONFIG_KEYS, "shared.key");
        assertTrue("approved for the computer surface",
                OlapConfigKeyPolicy.isConfigKeyPermitted(computerApproved, "shared.key", Collections.emptySet(), APPROVED_COMPUTER_CONFIG_KEYS));
        assertFalse("must NOT be permitted on the graph/io surface via the computer list",
                OlapConfigKeyPolicy.isConfigKeyPermitted(computerApproved, "shared.key", Collections.emptySet(), APPROVED_GRAPH_CONFIG_KEYS));

        final Configuration graphApproved = untrusted();
        graphApproved.setProperty(APPROVED_GRAPH_CONFIG_KEYS, "shared.key");
        assertTrue("approved for the graph/io surface",
                OlapConfigKeyPolicy.isConfigKeyPermitted(graphApproved, "shared.key", Collections.emptySet(), APPROVED_GRAPH_CONFIG_KEYS));
        assertFalse("must NOT be permitted on the computer surface via the graph list",
                OlapConfigKeyPolicy.isConfigKeyPermitted(graphApproved, "shared.key", Collections.emptySet(), APPROVED_COMPUTER_CONFIG_KEYS));
    }

    @Test
    public void shouldPermitAnyNonMetaKeyWhenTrusted() {
        assertTrue(OlapConfigKeyPolicy.isConfigKeyPermitted(trusted(), "spark.executor.extraJavaOptions",
                Collections.emptySet(), APPROVED_GRAPH_CONFIG_KEYS));
    }

    // meta-keys govern the trust boundary and are never settable via either surface, even when trusted (self-elevation).
    @Test
    public void shouldRejectMetaKeysEvenWhenTrusted() {
        for (final String metaKey : new String[]{OlapClassLoadingPolicy.TRUSTED, OlapClassLoadingPolicy.APPROVED_CLASSES,
                APPROVED_COMPUTER_CONFIG_KEYS, APPROVED_GRAPH_CONFIG_KEYS}) {
            assertFalse("meta-key '" + metaKey + "' must never be permitted",
                    OlapConfigKeyPolicy.isConfigKeyPermitted(trusted(), metaKey, Collections.emptySet(), APPROVED_GRAPH_CONFIG_KEYS));
            assertTrue("isMetaKey must classify '" + metaKey + "'", OlapConfigKeyPolicy.isMetaKey(metaKey));
        }
    }

    @Test
    public void shouldParseCommaSeparatedApprovedKeysAndIgnoreBlanks() {
        final Configuration c = untrusted();
        c.setProperty(APPROVED_GRAPH_CONFIG_KEYS, "a.key, b.key ,, c.key");
        for (final String key : new String[]{"a.key", "b.key", "c.key"})
            assertTrue(key, OlapConfigKeyPolicy.isConfigKeyPermitted(c, key, Collections.emptySet(), APPROVED_GRAPH_CONFIG_KEYS));
        assertFalse("blank entry must not become an approved key",
                OlapConfigKeyPolicy.isConfigKeyPermitted(c, "", Collections.emptySet(), APPROVED_GRAPH_CONFIG_KEYS));
    }

    @Test
    public void shouldThrowNamingTheSurfaceListForUnapprovedUntrustedKey() {
        try {
            OlapConfigKeyPolicy.checkConfigKeyPermitted(untrusted(), "spark.executor.extraJavaOptions",
                    Collections.emptySet(), APPROVED_GRAPH_CONFIG_KEYS);
            fail("an unapproved key must be rejected for an untrusted traversal");
        } catch (final IllegalArgumentException iae) {
            assertTrue("message must name the surface's approved-key list",
                    iae.getMessage().contains(APPROVED_GRAPH_CONFIG_KEYS));
            assertTrue("message must name the trust flag", iae.getMessage().contains(OlapClassLoadingPolicy.TRUSTED));
        }
    }

    @Test
    public void shouldThrowMetaKeyMessageForTrustBoundaryKey() {
        try {
            OlapConfigKeyPolicy.checkConfigKeyPermitted(trusted(), OlapClassLoadingPolicy.TRUSTED,
                    Collections.emptySet(), APPROVED_COMPUTER_CONFIG_KEYS);
            fail("a trust-boundary meta-key must never be settable, even when trusted");
        } catch (final IllegalArgumentException iae) {
            assertTrue(iae.getMessage().contains(OlapClassLoadingPolicy.TRUSTED));
        }
    }
}
