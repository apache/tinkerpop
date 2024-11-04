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
package org.apache.tinkerpop.gremlin.driver;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.driver.interceptor.PayloadSerializingInterceptor;
import org.junit.Test;

import java.util.List;

import static org.apache.tinkerpop.gremlin.driver.Cluster.SERIALIZER_INTERCEPTOR_NAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test Cluster and Cluster.Builder.
 */
public class ClusterTest {
    private static final RequestInterceptor TEST_INTERCEPTOR = httpRequest -> new HttpRequest(null, null, null);

    @Test
    public void shouldNotAllowModifyingRelativeToNonExistentInterceptor() {
        try {
            Cluster.build().addInterceptorAfter("none", "test", req -> req);
            fail("Should not have allowed interceptor to be added.");
        } catch (Exception e) {
            assertThat(e, instanceOf(IllegalArgumentException.class));
            assertEquals("none interceptor not found", e.getMessage());
        }

        try {
            Cluster.build().addInterceptorBefore("none", "test", req -> req);
            fail("Should not have allowed interceptor to be added.");
        } catch (Exception e) {
            assertThat(e, instanceOf(IllegalArgumentException.class));
            assertEquals("none interceptor not found", e.getMessage());
        }

        try {
            Cluster.build().removeInterceptor("nonexistent");
            fail("Should not have allowed interceptor to be removed.");
        } catch (Exception e) {
            assertThat(e, instanceOf(IllegalArgumentException.class));
            assertEquals("nonexistent interceptor not found", e.getMessage());
        }
    }

    @Test
    public void shouldAddToInterceptorToBeginningIfBeforeFirst() {
        final Cluster testCluster = Cluster.build()
                .addInterceptor("b", req -> req)
                .addInterceptorBefore(SERIALIZER_INTERCEPTOR_NAME, "a", TEST_INTERCEPTOR)
                .create();
        assertEquals("a", testCluster.getRequestInterceptors().get(0).getLeft());
        assertEquals(TEST_INTERCEPTOR, testCluster.getRequestInterceptors().get(0).getRight());
        assertEquals(SERIALIZER_INTERCEPTOR_NAME, testCluster.getRequestInterceptors().get(1).getLeft());
    }

    @Test
    public void shouldAddToInterceptorAfter() {
        final Cluster testCluster = Cluster.build()
                .addInterceptor("b", req -> req)
                .addInterceptorAfter(SERIALIZER_INTERCEPTOR_NAME, "a", TEST_INTERCEPTOR)
                .create();
        assertEquals(SERIALIZER_INTERCEPTOR_NAME, testCluster.getRequestInterceptors().get(0).getLeft());
        assertEquals("a", testCluster.getRequestInterceptors().get(1).getLeft());
        assertEquals(TEST_INTERCEPTOR, testCluster.getRequestInterceptors().get(1).getRight());
        assertEquals("b", testCluster.getRequestInterceptors().get(2).getLeft());

    }

    @Test
    public void shouldAddToInterceptorLast() {
        final Cluster testCluster = Cluster.build()
                .addInterceptor("c", req -> req)
                .addInterceptor("b", req -> req)
                .addInterceptor("a", req -> req)
                .create();
        assertEquals(SERIALIZER_INTERCEPTOR_NAME, testCluster.getRequestInterceptors().get(0).getLeft());
        assertEquals("c", testCluster.getRequestInterceptors().get(1).getLeft());
        assertEquals("b", testCluster.getRequestInterceptors().get(2).getLeft());
        assertEquals("a", testCluster.getRequestInterceptors().get(3).getLeft());
    }

    @Test
    public void shouldNotAllowAddingDuplicateName() {
        try {
            Cluster.build().addInterceptor("name", req -> req).addInterceptor("name", req -> req);
            fail("Should not have allowed interceptor to be added.");
        } catch (Exception e) {
            assertThat(e, instanceOf(IllegalArgumentException.class));
            assertEquals("name interceptor already exists", e.getMessage());
        }

        try {
            Cluster.build().addInterceptor("name", req -> req).addInterceptorAfter("name", "name", req -> req);
            fail("Should not have allowed interceptor to be added.");
        } catch (Exception e) {
            assertThat(e, instanceOf(IllegalArgumentException.class));
            assertEquals("name interceptor already exists", e.getMessage());
        }

        try {
            Cluster.build().addInterceptor("name", req -> req).addInterceptorBefore("name", "name", req -> req);
            fail("Should not have allowed interceptor to be added.");
        } catch (Exception e) {
            assertThat(e, instanceOf(IllegalArgumentException.class));
            assertEquals("name interceptor already exists", e.getMessage());
        }
    }

    @Test
    public void shouldContainBodySerializerByDefault() {
        final List<Pair<String, ? extends RequestInterceptor>> interceptors = Cluster.build().create().getRequestInterceptors();
        assertEquals(1, interceptors.size());
        assertTrue(interceptors.get(0).getRight() instanceof PayloadSerializingInterceptor);
    }

    @Test
    public void shouldRemoveDefaultSerializer() {
        final Cluster testCluster = Cluster.build().removeInterceptor(SERIALIZER_INTERCEPTOR_NAME).create();
        assertEquals(0, testCluster.getRequestInterceptors().size());
    }
}
