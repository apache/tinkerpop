package com.tinkerpop.blueprints.util;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.Vertex;
import org.junit.Test;

import static junit.framework.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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

    @Test
    public void shouldAttachKeyValuesButNotLabelsOrId() {
        final Element mockElement = mock(Element.class);
        ElementHelper.attachKeyValues(mockElement, "test", 123, Property.Key.ID, 321, Property.Key.LABEL, "friends");
        verify(mockElement, times(1)).setProperty("test", 123);
        verify(mockElement, times(0)).setProperty(Property.Key.ID, 321);
        verify(mockElement, times(0)).setProperty(Property.Key.LABEL, "friends");
    }

    @Test(expected = ClassCastException.class)
    public void shouldFailTryingToAttachNonStringKey() {
        final Element mockElement = mock(Element.class);
        ElementHelper.attachKeyValues(mockElement, "test", 123, 321, "test");
    }

    @Test
    public void shouldFailTryingToAttachKeysToNullElement() {
        try {
            ElementHelper.attachKeyValues(null, "test", 123, 321, "test");
            fail("Should throw exception since the element argument is null");
        } catch (IllegalArgumentException iae) {
            assertEquals(Graph.Exceptions.argumentCanNotBeNull("element").getMessage(), iae.getMessage());
        }
    }

    @Test
    public void shouldFailElementAreEqualTestBecauseFirstArgumentIsNull() {
        try {
            ElementHelper.areEqual((Element) null, "some object");
            fail("Should throw exception since the first argument is null");
        } catch (IllegalArgumentException iae) {
            assertEquals(Graph.Exceptions.argumentCanNotBeNull("a").getMessage(), iae.getMessage());
        }
    }

    @Test
    public void shouldFailElementAreEqualTestBecauseSecondArgumentIsNull() {
        final Element mockElement = mock(Element.class);
        try {
            ElementHelper.areEqual(mockElement, null);
            fail("Should throw exception since the second argument is null");
        } catch (IllegalArgumentException iae) {
            assertEquals(Graph.Exceptions.argumentCanNotBeNull("b").getMessage(), iae.getMessage());
        }
    }

    @Test
    public void shouldDetermineElementsAreEqualAsTheyAreSameObject() {
        final Element mockElement = mock(Element.class);
        assertTrue(ElementHelper.areEqual(mockElement, mockElement));
    }

    @Test
    public void shouldDetermineElementsAreNotEqualAsAIsVertexAndBIsEdge() {
        final Element mockVertex = mock(Vertex.class);
        final Element mockEdge = mock(Edge.class);
        assertFalse(ElementHelper.areEqual(mockVertex, mockEdge));
    }

    @Test
    public void shouldDetermineElementsAreNotEqualAsBIsVertexAndAIsEdge() {
        final Element mockVertex = mock(Vertex.class);
        final Element mockEdge = mock(Edge.class);
        assertFalse(ElementHelper.areEqual(mockEdge, mockVertex));
    }

    @Test
    public void shouldDetermineVerticesAreEqual() {
        final Element mockVertexA = mock(Vertex.class);
        final Element mockVertexB = mock(Vertex.class);
        when(mockVertexA.getId()).thenReturn("1");
        when(mockVertexB.getId()).thenReturn("1");
        assertTrue(ElementHelper.areEqual(mockVertexA, mockVertexB));
    }

    @Test
    public void shouldDetermineVerticesAreNotEqual() {
        final Element mockVertexA = mock(Vertex.class);
        final Element mockVertexB = mock(Vertex.class);
        when(mockVertexA.getId()).thenReturn("1");
        when(mockVertexB.getId()).thenReturn("2");
        assertFalse(ElementHelper.areEqual(mockVertexA, mockVertexB));
    }

    @Test
    public void shouldDetermineEdgesAreEqual() {
        final Element mockEdgeA = mock(Edge.class);
        final Element mockEdgeB = mock(Edge.class);
        when(mockEdgeA.getId()).thenReturn("1");
        when(mockEdgeB.getId()).thenReturn("1");
        assertTrue(ElementHelper.areEqual(mockEdgeA, mockEdgeB));
    }

    @Test
    public void shouldDetermineEdgesAreNotEqual() {
        final Element mockEdgeA = mock(Edge.class);
        final Element mockEdgeB = mock(Edge.class);
        when(mockEdgeA.getId()).thenReturn("1");
        when(mockEdgeB.getId()).thenReturn("2");
        assertFalse(ElementHelper.areEqual(mockEdgeA, mockEdgeB));
    }

    @Test
    public void shouldFailPropertyAreEqualTestBecauseFirstArgumentIsNull() {
        try {
            ElementHelper.areEqual((Property) null, "some object");
            fail("Should throw exception since the first argument is null");
        } catch (IllegalArgumentException iae) {
            assertEquals(Graph.Exceptions.argumentCanNotBeNull("a").getMessage(), iae.getMessage());
        }
    }

    @Test
    public void shouldFailPropertyAreEqualTestBecauseSecondArgumentIsNull() {
        final Property mockProperty = mock(Property.class);
        try {
            ElementHelper.areEqual(mockProperty, null);
            fail("Should throw exception since the second argument is null");
        } catch (IllegalArgumentException iae) {
            assertEquals(Graph.Exceptions.argumentCanNotBeNull("b").getMessage(), iae.getMessage());
        }
    }

    @Test
    public void shouldFailPropertyAreEqualTestBecauseSecondArgumentIsNotProperty() {
        final Property mockProperty = mock(Property.class);
        assertFalse(ElementHelper.areEqual(mockProperty, "i'm a string"));
    }

    @Test
    public void shouldDeterminePropertiesAreEqualAsTheyAreSameObject() {
        final Property mockProperty = mock(Property.class);
        assertTrue(ElementHelper.areEqual(mockProperty, mockProperty));
    }

    @Test
    public void shouldDeterminePropertiesAreEqualAsTheyAreBothEmpty() {
        final Property mockPropertyA = mock(Property.class);
        final Property mockPropertyB = mock(Property.class);
        when(mockPropertyA.isPresent()).thenReturn(false);
        when(mockPropertyB.isPresent()).thenReturn(false);
        assertTrue(ElementHelper.areEqual(mockPropertyA, mockPropertyB));
    }

    @Test
    public void shouldDeterminePropertiesAreNotEqualAsAIsEmptyAndBIsNot() {
        final Property mockPropertyA = mock(Property.class);
        final Property mockPropertyB = mock(Property.class);
        when(mockPropertyA.isPresent()).thenReturn(false);
        when(mockPropertyB.isPresent()).thenReturn(true);
        assertFalse(ElementHelper.areEqual(mockPropertyA, mockPropertyB));
    }

    @Test
    public void shouldDeterminePropertiesAreNotEqualAsBIsEmptyAndAIsNot() {
        final Property mockPropertyA = mock(Property.class);
        final Property mockPropertyB = mock(Property.class);
        when(mockPropertyA.isPresent()).thenReturn(true);
        when(mockPropertyB.isPresent()).thenReturn(false);
        assertFalse(ElementHelper.areEqual(mockPropertyA, mockPropertyB));
    }

}
