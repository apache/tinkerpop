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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GraphHiddenTest {

    @Test
    public void shouldHandleSystemKeyManipulationCorrectly() {
        String key = "key";
        assertFalse(Graph.Hidden.isHidden(key));
        assertTrue(Graph.Hidden.isHidden(Graph.Hidden.hide(key)));
        assertEquals(Graph.Hidden.hide("").concat(key), Graph.Hidden.hide(key));
        for (int i = 0; i < 10; i++) {
            key = Graph.Hidden.hide(key);
        }
        assertTrue(Graph.Hidden.isHidden(key));
        assertEquals(Graph.Hidden.hide("key"), key);
        assertEquals("key", Graph.Hidden.unHide(key));
        for (int i = 0; i < 10; i++) {
            key = Graph.Hidden.unHide(key);
        }
        assertFalse(Graph.Hidden.isHidden(key));
        assertEquals("key", key);
    }
}
