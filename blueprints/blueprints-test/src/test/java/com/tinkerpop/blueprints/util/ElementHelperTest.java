package com.tinkerpop.blueprints.util;

import com.tinkerpop.blueprints.Property;
import org.junit.Test;

import static junit.framework.Assert.fail;
import static org.junit.Assert.assertEquals;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ElementHelperTest {

    @Test
    public void shouldValidatePropertyAndNotAllowNullValue() {
        try {
            ElementHelper.validateProperty("test", null);
            fail("Should fail as property value cannot be null");
        } catch (IllegalArgumentException iae) {
            assertEquals(Property.Exceptions.propertyValueCanNotBeNull().getMessage(), iae.getMessage());
        }
    }

    @Test
    public void shouldValidatePropertyAndNotAllowNullKey() {
        try {
            ElementHelper.validateProperty(null, "test");
            fail("Should fail as property key cannot be null");
        } catch (IllegalArgumentException iae) {
            assertEquals(Property.Exceptions.propertyKeyCanNotBeNull().getMessage(), iae.getMessage());
        }
    }

    @Test
    public void shouldValidatePropertyAndNotAllowEmptyKey() {
        try {
            ElementHelper.validateProperty("", "test");
            fail("Should fail as property key cannot be empty");
        } catch (IllegalArgumentException iae) {
            assertEquals(Property.Exceptions.propertyKeyCanNotBeEmpty().getMessage(), iae.getMessage());
        }
    }

    @Test
    public void shouldValidatePropertyAndNotAllowIdKey() {
        try {
            ElementHelper.validateProperty(Property.Key.ID, "test");
            fail(String.format("Should fail as property key %s is reserved", Property.Key.ID));
        } catch (IllegalArgumentException iae) {
            assertEquals(Property.Exceptions.propertyKeyIdIsReserved().getMessage(), iae.getMessage());
        }
    }

    @Test
    public void shouldValidatePropertyAndAllowLabelKey() {
        try {
            ElementHelper.validateProperty(Property.Key.LABEL, "test");
            fail(String.format("Should fail as property key %s is reserved", Property.Key.LABEL));
        } catch (IllegalArgumentException iae) {
            assertEquals(Property.Exceptions.propertyKeyLabelIsReserved().getMessage(), iae.getMessage());
        }
    }
}
