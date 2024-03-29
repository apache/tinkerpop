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

package org.apache.tinkerpop.gremlin.structure.io.binary;

public class Marker {
    private final byte value;

    public static Marker END_OF_STREAM = new Marker((byte)0);

    private Marker(final byte value) {
        this.value = value;
    }

    public byte getValue() {
        return value;
    }

    public static Marker of(final byte value) {
        if (value != 0) {
            throw new IllegalArgumentException();
        }
        return END_OF_STREAM;
    }
}
