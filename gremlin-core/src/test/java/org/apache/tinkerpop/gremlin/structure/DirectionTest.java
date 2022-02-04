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
package org.apache.tinkerpop.gremlin.structure;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class DirectionTest {
    @Test
    public void shouldReturnOppositeOfIn() {
        assertEquals(Direction.OUT, Direction.IN.opposite());
    }

    @Test
    public void shouldReturnOppositeOfOut() {
        assertEquals(Direction.IN, Direction.OUT.opposite());
    }

    @Test
    public void shouldReturnOppositeOfBoth() {
        assertEquals(Direction.BOTH, Direction.BOTH.opposite());
    }

    @Test
    public void shouldGetDirectionFromName() {
        assertEquals(Direction.IN, Direction.directionValueOf("IN"));
        assertEquals(Direction.OUT, Direction.directionValueOf("OUT"));
        assertEquals(Direction.IN, Direction.directionValueOf("to"));
        assertEquals(Direction.OUT, Direction.directionValueOf("from"));
    }
}
