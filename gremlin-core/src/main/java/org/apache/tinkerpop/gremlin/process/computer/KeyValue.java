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
package org.apache.tinkerpop.gremlin.process.computer;

import java.io.Serializable;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class KeyValue<K, V> implements Serializable {

    private final K key;
    private final V value;
    private static final String EMPTY_STRING = "";
    private static final String TAB = "\t";
    private static final String NULL = "null";

    public KeyValue(final K key, final V value) {
        this.key = key;
        this.value = value;
    }

    public K getKey() {
        return this.key;
    }

    public V getValue() {
        return this.value;
    }

    public String toString() {
        if (this.key instanceof MapReduce.NullObject && this.value instanceof MapReduce.NullObject) {
            return EMPTY_STRING;
        } else if (this.key instanceof MapReduce.NullObject) {
            return makeString(this.value);
        } else if (this.value instanceof MapReduce.NullObject) {
            return makeString(this.key);
        } else {
            return makeString(this.key) + TAB + makeString(this.value);
        }
    }

    private static final String makeString(final Object object) {
        return null == object ? NULL : object.toString();
    }
}
