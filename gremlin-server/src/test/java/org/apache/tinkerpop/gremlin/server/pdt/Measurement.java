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
package org.apache.tinkerpop.gremlin.server.pdt;

import org.apache.tinkerpop.gremlin.structure.io.pdt.ProviderDefined;

/**
 * A composite test fixture containing a primitive PDT field ({@link Uint32}).
 * Used to exercise primitive-nested-in-composite round-trip in integration tests.
 */
@ProviderDefined(name = "Measurement")
public class Measurement {
    public String unit;
    public Uint32 quantity;

    public Measurement() {}

    public Measurement(final String unit, final Uint32 quantity) {
        this.unit = unit;
        this.quantity = quantity;
    }
}
