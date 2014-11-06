package com.tinkerpop.gremlin.structure;

import java.io.Serializable;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class MockSerializable implements Serializable {
    private String testField;

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