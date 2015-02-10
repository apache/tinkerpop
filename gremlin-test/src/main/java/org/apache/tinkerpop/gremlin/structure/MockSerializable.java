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

import java.io.Serializable;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class MockSerializable implements Serializable {
    private String testField;

    private MockSerializable() {
    }

    public MockSerializable(final String testField) {
        this.testField = testField;
    }

    public String getTestField() {
        return this.testField;
    }

    public void setTestField(final String testField) {
        this.testField = testField;
    }

    @Override
    public boolean equals(Object oth) {
        if (this == oth) return true;
        else if (oth == null) return false;
        else if (!getClass().isInstance(oth)) return false;
        MockSerializable m = (MockSerializable) oth;
        if (testField == null) {
            return (m.testField == null);
        } else return testField.equals(m.testField);
    }
}