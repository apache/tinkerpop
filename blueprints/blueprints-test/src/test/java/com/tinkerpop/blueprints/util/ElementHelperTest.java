package com.tinkerpop.blueprints.util;

import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Property;
import org.junit.Test;

import static junit.framework.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

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

    @Test
    public void shouldHaveValidProperty() {
        ElementHelper.validateProperty("key", "value");
    }

    @Test
    public void shouldAllowEvenNumberOfKeyValues() {
        try {
            ElementHelper.legalKeyValues("key", "test", "no-value-for-this-one");
            fail("Should fail as there is an odd number of key-values");
        } catch (IllegalArgumentException iae) {
            assertEquals(Element.Exceptions.providedKeyValuesMustBeAMultipleOfTwo().getMessage(), iae.getMessage());
        }
    }

    @Test
    public void shouldNotAllowEvenNumberOfKeyValuesAndInvalidKeys() {
        try {
            ElementHelper.legalKeyValues("key", "test", "value-for-this-one", 1, 1, "none");
            fail("Should fail as there is an even number of key-values, but a bad key");
        } catch (IllegalArgumentException iae) {
            assertEquals(Element.Exceptions.providedKeyValuesMustHaveALegalKeyOnEvenIndices().getMessage(), iae.getMessage());
        }
    }

    @Test
    public void shouldAllowEvenNumberOfKeyValuesAndValidKeys() {
        ElementHelper.legalKeyValues("key", "test", "value-for-this-one", 1, Property.Key.hidden("1"), "none");
    }

    @Test
    public void shouldFindTheIdValueAlone() {
        assertEquals(123l, ElementHelper.getIdValue(Property.Key.ID, 123l).get());
    }

    @Test
    public void shouldFindTheIdValueInSet() {
        assertEquals(123l, ElementHelper.getIdValue("test", 321, Property.Key.ID, 123l, "testagain", "that").get());
    }

    @Test
    public void shouldNotFindAnIdValue() {
        assertFalse(ElementHelper.getIdValue("test", 321, "xyz", 123l, "testagain", "that").isPresent());
    }

    @Test
    public void shouldFindTheLabelValueAlone() {
        assertEquals("friend", ElementHelper.getLabelValue(Property.Key.LABEL, "friend").get());
    }

    @Test
    public void shouldFindTheLabelValueInSet() {
        assertEquals("friend", ElementHelper.getLabelValue("test", 321, Property.Key.LABEL, "friend", "testagain", "that").get());
    }

    @Test
    public void shouldNotFindAnLabelValue() {
        assertFalse(ElementHelper.getLabelValue("test", 321, "xyz", "friend", "testagain", "that").isPresent());
    }

    @Test(expected = ClassCastException.class)
    public void shouldErrorIfLabelIsNotString() {
        assertFalse(ElementHelper.getLabelValue("test", 321, Property.Key.LABEL, 4545, "testagain", "that").isPresent());
    }
}
