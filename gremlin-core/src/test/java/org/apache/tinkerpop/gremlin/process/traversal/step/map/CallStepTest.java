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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValueStepTest;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversal;
import org.apache.tinkerpop.gremlin.structure.service.Service;
import org.apache.tinkerpop.gremlin.structure.service.ServiceRegistry;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactoryTest;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

public class CallStepTest extends GValueStepTest {

    private static final String TEST_SERVICE = "test-service";
    private static final Map<String, String> STATIC_PARAMS = Map.of("foo", "bar", "fizz", "fuzz");
    @Mock
    public ServiceRegistry mockedRegistry;
    @Mock
    public Service<?, ?> mockedService;
    @Rule
    public MockitoRule mockitoRule = MockitoJUnit.rule();
    
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
    
    @Test
    public void getMergedParamsShouldPinVariables() {
        GraphTraversal.Admin<Object, Object> traversal = getGValueTraversal();
        assertEquals(STATIC_PARAMS, ((CallStepPlaceholder<?, ?>) traversal.getSteps().get(0)).getMergedParams());
        verifyVariables(traversal, Set.of("params"), Set.of());
    }

    @Test
    public void getStaticParamsAsGValueShouldNotPinVariables() {
        GraphTraversal.Admin<Object, Object> traversal = getGValueTraversal();
        assertEquals(GValue.of("params", STATIC_PARAMS), ((CallStepPlaceholder<?, ?>) traversal.getSteps().get(0)).getStaticParamsAsGValue());
        verifyVariables(traversal, Set.of(), Set.of("params"));
    }

    @Test
    public void getMergedParamsFromConcreteStep() {
        GraphTraversal.Admin<Object, Object> traversal = getGValueTraversal();
        assertEquals(STATIC_PARAMS, ((CallStepPlaceholder<?, ?>) traversal.getSteps().get(0))
                .asConcreteStep().getMergedParams());
    }

    @Test
    public void getMergedParamsWithChildTraversalShouldPinVariables() {
        when(mockedRegistry.get(TEST_SERVICE, false, STATIC_PARAMS)).thenReturn(mockedService);
        when(mockedService.getRequirements()).thenReturn(Set.of());
        GraphTraversal.Admin<?, ?> traversal = getGValueWithChildTraversal();
        assertEquals(Map.of("foo", "bar", "fizz", "fuzz", "abc", 123), 
                ((CallStepPlaceholder<?, ?>) traversal.getSteps().get(1)).getMergedParams());
        verifyVariables(traversal, Set.of("params"), Set.of());
    }

    @Test
    public void getMergedParamsWithChildTraversalFromConcreteStep() {
        when(mockedRegistry.get(TEST_SERVICE, false, STATIC_PARAMS)).thenReturn(mockedService);
        when(mockedService.getRequirements()).thenReturn(Set.of());
        GraphTraversal.Admin<?, ?> traversal = getGValueWithChildTraversal();
        assertEquals(Map.of("foo", "bar", "fizz", "fuzz", "abc", 123),
                ((CallStepPlaceholder<?, ?>) traversal.getSteps().get(1)).asConcreteStep().getMergedParams());
    }
    
    @Test
    public void getServiceShouldPinVariables() {
        when(mockedRegistry.get(TEST_SERVICE, true, STATIC_PARAMS)).thenReturn(mockedService);
        GraphTraversal.Admin<Object, Object> traversal = getGValueTraversal();
        CallStepPlaceholder<?, ?> callStep = (CallStepPlaceholder<?, ?>) traversal.getSteps().get(0);
        assertNotNull(callStep.service());
        assertEquals(TEST_SERVICE, callStep.getServiceName());
        verifyVariables(traversal, Set.of("params"), Set.of());
    }

    @Test
    public void getServiceGValueSafeShouldNotPinVariables() {
        when(mockedRegistry.get(TEST_SERVICE, true, STATIC_PARAMS)).thenReturn(mockedService);
        GraphTraversal.Admin<Object, Object> traversal = getGValueTraversal();
        CallStepPlaceholder<?, ?> callStep = (CallStepPlaceholder<?, ?>) traversal.getSteps().get(0);
        assertNotNull(callStep.serviceGValueSafe());
        assertEquals(TEST_SERVICE, callStep.getServiceName());
        verifyVariables(traversal, Set.of(), Set.of("params"));
    }

    @Test
    public void getServiceFromConcreteStep() {
        when(mockedRegistry.get(TEST_SERVICE, true, STATIC_PARAMS)).thenReturn(mockedService);
        GraphTraversal.Admin<Object, Object> traversal = getGValueTraversal();
        CallStep<?, ?> callStep = ((CallStepPlaceholder<?, ?>) traversal.getSteps().get(0)).asConcreteStep();
        assertNotNull(callStep.service());
        assertEquals(TEST_SERVICE, callStep.getServiceName());
    }
    
    @Test
    public void getGValuesShouldReturnAllGValues() {
        GraphTraversal.Admin<Object, Object> traversal = getGValueTraversal();
        Collection<GValue<?>> gValues = ((CallStepPlaceholder<?, ?>) traversal.getSteps().get(0)).getGValues();
        assertEquals(1, gValues.size());
        assertEquals("params", gValues.iterator().next().getName());
    }

    @Test
    public void getGValuesNoneShouldUseCallStepInsteadOfPlaceholder() {
        GraphTraversal.Admin<Object, Object> traversal = __.call(TEST_SERVICE, STATIC_PARAMS).asAdmin();
        assertTrue(traversal.getSteps().get(0) instanceof CallStep);
        assertEquals(STATIC_PARAMS, ((CallStep) traversal.getSteps().get(0)).getMergedParams());
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

    private GraphTraversal.Admin<Object, Object> getGValueTraversal() {
        MapConfiguration mapConfiguration = new MapConfiguration(Map.of("service-registry", mockedRegistry));
        GraphTraversalSource g = traversal().with(GraphFactoryTest.MockGraph.open(mapConfiguration));
        return g.call(TEST_SERVICE, GValue.of("params", STATIC_PARAMS)).asAdmin();
    }

    private GraphTraversal.Admin<?,?> getGValueWithChildTraversal() {
        when(mockedService.getRequirements()).thenReturn(Set.of());
        MapConfiguration mapConfiguration = new MapConfiguration(Map.of("service-registry", mockedRegistry));
        GraphTraversalSource g = traversal().with(GraphFactoryTest.MockGraph.open(mapConfiguration));
        return g.V().call(TEST_SERVICE, GValue.of("params", STATIC_PARAMS), 
                (Traversal) __.inject(Map.of("abc", 123))).asAdmin();
    }
    
}
