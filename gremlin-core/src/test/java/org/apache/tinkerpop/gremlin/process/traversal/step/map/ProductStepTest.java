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

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.StepTest;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class ProductStepTest extends StepTest {
    @Override
    protected List<Traversal> getTraversals() {
        return Arrays.asList(
                __.product(Collections.emptyList()),
                __.product(Collections.emptySet()),
                __.product(__.V().fold()));
    }

    @Test
    public void testReturnTypes() {
        assertEquals(Collections.emptyList(), __.__(Collections.emptyList()).product(Collections.emptyList()).next());
        assertEquals(Collections.emptyList(), __.__(Collections.emptyList()).product(Arrays.asList(1, 2, 3)).next());
        assertEquals(Collections.emptyList(), __.__(Arrays.asList(1, 2, 3)).product(Collections.emptyList()).next());

        assertEquals(Arrays.asList(Arrays.asList(1,1), Arrays.asList(1,2), Arrays.asList(1,3)), __.__(Arrays.asList(1)).product(Arrays.asList(1, 2, 3)).next());
        assertEquals(Arrays.asList(Arrays.asList(1,1), Arrays.asList(2,1), Arrays.asList(3,1)), __.__(Arrays.asList(1, 2, 3)).product(Arrays.asList(1)).next());

        assertEquals(Arrays.asList(Arrays.asList(5L,7L), Arrays.asList(5L,10L), Arrays.asList(8L,7L), Arrays.asList(8L,10L)),
                    __.__(new long[] {5L, 8L}).product(new long[] {7L, 10L}).next());
        assertEquals(Arrays.asList(Arrays.asList(5L,7L), Arrays.asList(5L,10L), Arrays.asList(8L,7L), Arrays.asList(8L,10L)),
                    __.__(new long[] {5L, 8L}).product(__.constant(new long[] {7L, 10L})).next());

        assertEquals(Arrays.asList(Arrays.asList(7L,8L), Arrays.asList(7L,11L), Arrays.asList(3L,8L), Arrays.asList(3L,11L)),
                    __.__(1).constant(new Long[] {7L, 3L}).product(new Long[] {8L, 11L}).next());
        assertEquals(Arrays.asList(Arrays.asList(7L,8L), Arrays.asList(7L,11L), Arrays.asList(1L,8L), Arrays.asList(1L,11L)),
                    __.__(1).constant(new Long[] {7L, 1L}).product(__.constant(new Long[] {8L, 11L})).next());

        assertEquals(Arrays.asList(Arrays.asList(5.5,5.5), Arrays.asList(5.5,10.5), Arrays.asList(8.0,5.5), Arrays.asList(8.0,10.5)),
                    __.__(new double[] {5.5, 8.0}).product(new double[] {5.5, 10.5}).next());
        assertEquals(Arrays.asList(Arrays.asList(5.5,5.5), Arrays.asList(5.5,10.5), Arrays.asList(8.0,5.5), Arrays.asList(8.0,10.5)),
                    __.__(new double[] {5.5, 8.0}).product(__.constant(new double[] {5.5, 10.5})).next());

        final Set<Integer> ten = new HashSet<>();
        ten.add(10);
        final Set<Integer> eleven = new HashSet<>();
        eleven.add(11);
        assertEquals(Arrays.asList(Arrays.asList(10, 11)), __.__(ten).product(eleven).next());
        assertEquals(Arrays.asList(Arrays.asList(10, 11)), __.__(ten).product(__.constant(eleven)).next());
    }
}
