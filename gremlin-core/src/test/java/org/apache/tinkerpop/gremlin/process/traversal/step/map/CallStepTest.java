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
package org.apache.tinkerpop.gremlin.process.traversal.step.map;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValueStepTest;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversal;
import org.apache.tinkerpop.gremlin.structure.service.Service;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CallStepTest extends GValueStepTest {

    @Override
    protected List<Traversal> getTraversals() {
        return Arrays.asList(
                __.call("service"),
                __.call("--list").with("service", "tinker.search"),
                __.call("--list", Map.of("service", "tinker.search")),
                __.call("xyz-service", Map.of("foo", "bar")),
                __.call("xyz-service").with("foo", "bar"),
                __.call("xyz-service", __.inject(Map.of("foo", "bar"))),
                __.call("xyz-service").with("a", __.inject("b")),
                __.call("xyz-service", Map.of("foo", "bar"), __.inject(Map.of("a", "b"))),
                __.call("xyz-service", GValue.of("params", Map.of("foo", "bar"))),
                __.call("xyz-service", GValue.of("params", Map.of("foo", "bar")), __.inject(Map.of("a", "b")))
        );
    }

    @Override
    protected List<Pair<Traversal, Set<String>>> getGValueTraversals() {
        return List.of(
                Pair.of(__.call("xyz-service", GValue.of("params", Map.of("foo", "bar"))), Set.of("params")),
                Pair.of(__.call("xyz-service", GValue.of("params", Map.of("foo", "bar")), __.inject(Map.of("a", "b"))), Set.of("params"))
        );
    }

    @Test
    public void testSerializationRoundTrip() {
        final byte[] serialized = SerializationUtils.serialize(__.call("--list"));
        final DefaultTraversal<Object, Object> deserialized = SerializationUtils.deserialize(serialized);

        assertEquals(1, deserialized.getSteps().size());
        assertTrue(deserialized.getSteps().get(0) instanceof CallStep);
        assertEquals("--list", ((CallStep) deserialized.getSteps().get(0)).getServiceName());
    }

    @Test
    public void traversalReplacementTest() {
        final Traversal.Admin a = __.inject("a").asAdmin();
        final Traversal.Admin b = __.inject("b").asAdmin();

        final CallStep step = new CallStep(a, false, "service");
        assertEquals("a", getContextTraversalValue(step));
        step.setTraversal(b);
        assertEquals("b", getContextTraversalValue(step));
    }

    private Object getContextTraversalValue(final CallStep step) {
        try {
            final Method privateMethod = CallStep.class.getDeclaredMethod("ctx");
            privateMethod.setAccessible(true);
            final Service.ServiceCallContext context = (Service.ServiceCallContext) privateMethod.invoke(step);
            return context.getTraversal().next();
        } catch (final NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            // This should never happen.
            throw new RuntimeException("ctx() method in class CallStep is renamed or removed. Please fix test.");
        }
    }
}
