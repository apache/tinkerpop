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
package org.apache.tinkerpop.gremlin.util.ser.binary;

import org.apache.tinkerpop.gremlin.structure.io.pdt.CompositePDTAdapter;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Test-only adapter registered via META-INF/services for SPI auto-wiring validation.
 */
public class TestPointAdapter implements CompositePDTAdapter<TestPointAdapter.TestPoint> {

    public static class TestPoint {
        public final int x;
        public final int y;

        public TestPoint(final int x, final int y) {
            this.x = x;
            this.y = y;
        }
    }

    @Override
    public String typeName() {
        return "test.Point";
    }

    @Override
    public Class<TestPoint> targetClass() {
        return TestPoint.class;
    }

    @Override
    public Map<String, Object> toFields(final TestPoint obj) {
        final Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("x", obj.x);
        fields.put("y", obj.y);
        return fields;
    }

    @Override
    public TestPoint fromFields(final Map<String, Object> fields) {
        return new TestPoint(((Number) fields.get("x")).intValue(), ((Number) fields.get("y")).intValue());
    }
}
