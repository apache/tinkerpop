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

package org.apache.tinkerpop.gremlin.process.traversal.traverser.util;

import org.apache.commons.lang.mutable.MutableShort;

import java.io.Serializable;

/**
 * Class to track a count associated with a Label
 */
public class LabelledCounter implements Serializable {

    private String label;

    private MutableShort count;

    public LabelledCounter(String label, short initialCount) {
        if (label == null) {
            throw new NullPointerException("Label is null");
        }
        this.label = label;
        this.count = new MutableShort(initialCount);
    }

    public boolean hasLabel(String label){
        return this.label.equals(label);
    }

    public int count() {
        return this.count.intValue();
    }

    public void increment() {
        this.count.increment();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof LabelledCounter)) return false;

        LabelledCounter that = (LabelledCounter) o;

        if (!label.equals(that.label)) return false;
        return count.equals(that.count);
    }

    @Override
    public int hashCode() {
        int result = label.hashCode();
        result = 31 * result + count.hashCode();
        return result;
    }
}
