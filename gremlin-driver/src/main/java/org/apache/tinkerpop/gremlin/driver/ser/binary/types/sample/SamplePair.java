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
package org.apache.tinkerpop.gremlin.driver.ser.binary.types.sample;

/**
 * A sample custom data type, trying to demonstrate the possibility to have arbitrary complex types and null
 * values without loosing type information.
 */
class SamplePair<K, V> {
    private final K key;
    private final V value;
    private final Info info;

    class Info {

    }

    SamplePair(K key, V value, SamplePair.Info info) {
        this.key = key;
        this.value = value;
        this.info = info;
    }

    public K getKey() {
        return key;
    }

    public V getValue() {
        return value;
    }
}
