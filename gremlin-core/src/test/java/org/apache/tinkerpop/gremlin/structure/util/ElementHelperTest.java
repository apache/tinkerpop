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
package org.apache.tinkerpop.gremlin.structure.util;

import org.apache.tinkerpop.gremlin.AssertHelper;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.javatuples.Pair;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ElementHelperTest {
    @Test
    public void shouldBeUtilityClass() throws Exception {
        AssertHelper.assertIsUtilityClass(ElementHelper.class);
    }

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
    public void shouldValidatePropertyAndNotAllowHiddenKey() {
        final String key = Graph.Hidden.hide("key");
        try {
            ElementHelper.validateProperty(key, "test");
            fail("Should fail as property key cannot be hidden");
        } catch (IllegalArgumentException iae) {
            assertEquals(Property.Exceptions.propertyKeyCanNotBeAHiddenKey(key).getMessage(), iae.getMessage());
        }
    }

    @Test
    public void shouldHaveValidProperty() {
        ElementHelper.validateProperty("aKey", "value");
    }

    @Test
    public void shouldAllowEvenNumberOfKeyValues() {
        try {
            ElementHelper.legalPropertyKeyValueArray("aKey", "test", "no-value-for-this-one");
            fail("Should fail as there is an odd number of key-values");
        } catch (IllegalArgumentException iae) {
            assertEquals(Element.Exceptions.providedKeyValuesMustBeAMultipleOfTwo().getMessage(), iae.getMessage());
        }
    }

    @Test
    public void shouldNotAllowEvenNumberOfKeyValuesAndInvalidKeys() {
        try {
            ElementHelper.legalPropertyKeyValueArray("aKey", "test", "value-for-this-one", 1, 1, "none");
            fail("Should fail as there is an even number of key-values, but a bad key");
        } catch (IllegalArgumentException iae) {
            assertEquals(Element.Exceptions.providedKeyValuesMustHaveALegalKeyOnEvenIndices().getMessage(), iae.getMessage());
        }
    }

    @Test
    public void shouldAllowEvenNumberOfKeyValuesAndValidKeys() {
        ElementHelper.legalPropertyKeyValueArray("aKey", "test", "value-for-this-one", 1, "1", "none");
    }

    @Test
    public void shouldNotAllowEvenNumberOfKeyValuesAndInvalidValues() {
        try {
            ElementHelper.legalPropertyKeyValueArray("aKey", "test", "value-for-this-one", 1, "1", null);
        } catch (IllegalArgumentException iae) {
            assertEquals(Property.Exceptions.propertyValueCanNotBeNull().getMessage(), iae.getMessage());
        }
    }

    @Test
    public void shouldFindTheIdValueAlone() {
        assertEquals(123l, ElementHelper.getIdValue(T.id, 123l).get());
    }

    @Test
    public void shouldFindTheIdValueInSet() {
        assertEquals(123l, ElementHelper.getIdValue("test", 321, T.id, 123l, "testagain", "that").get());
    }

    @Test
    public void shouldNotFindAnIdValue() {
        assertFalse(ElementHelper.getIdValue("test", 321, "xyz", 123l, "testagain", "that").isPresent());
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotFindAnIdValueBecauseItIsNull() {
        ElementHelper.getIdValue("test", 321, T.id, null, "testagain", "that");
    }

    @Test
    public void shouldFindTheLabelValueAlone() {
        assertEquals("friend", ElementHelper.getLabelValue(T.label, "friend").get());
    }

    @Test
    public void shouldFindTheLabelValueInSet() {
        assertEquals("friend", ElementHelper.getLabelValue("test", 321, T.label, "friend", "testagain", "that").get());
    }

    @Test
    public void shouldNotFindTheLabelValue() {
        assertFalse(ElementHelper.getLabelValue("test", 321, "xyz", "friend", "testagain", "that").isPresent());
    }

    @Test(expected = ClassCastException.class)
    public void shouldNotFindTheLabelBecauseItIsNotString() {
        ElementHelper.getLabelValue("test", 321, T.label, 4545, "testagain", "that");
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailCauseNullLabelsAreNotAllowed() {
        ElementHelper.getLabelValue("test", 321, T.label, null, "testagain", "that");
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailCauseSystemLabelsAreNotAllowed() {
        ElementHelper.getLabelValue("test", 321, T.label, Graph.Hidden.hide("systemLabel"), "testagain", "that");
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailCauseEmptyLabelsAreNotAllowed() {
        ElementHelper.getLabelValue("test", 321, T.label, "", "testagain", "that");
    }

    @Test
    public void shouldAttachPropertiesButNotLabelsOrId() {
        final Element mockElement = mock(Element.class);
        ElementHelper.attachProperties(mockElement, "test", 123, T.id, 321, T.label, "friends");
        verify(mockElement, times(1)).property("test", 123);
        verify(mockElement, times(0)).property(T.id.getAccessor(), 321);
        verify(mockElement, times(0)).property(T.label.getAccessor(), "friends");
    }

    @Test(expected = ClassCastException.class)
    public void shouldFailTryingToAttachPropertiesNonStringKey() {
        final Element mockElement = mock(Element.class);
        ElementHelper.attachProperties(mockElement, "test", 123, 321, "test");
    }

    @Test
    public void shouldFailTryingToAttachPropertiesToNullElement() {
        try {
            ElementHelper.attachProperties(null, "test", 123, 321, "test");
            fail("Should throw exception since the element argument is null");
        } catch (IllegalArgumentException iae) {
            assertTrue(Graph.Exceptions.argumentCanNotBeNull("vertex").getMessage().equals(iae.getMessage()) || Graph.Exceptions.argumentCanNotBeNull("element").getMessage().equals(iae.getMessage()));
        }
    }

    @Test
    public void shouldAttachPropertiesWithCardinalityButNotLabelsOrId() {
        final Vertex mockElement = mock(Vertex.class);
        ElementHelper.attachProperties(mockElement, VertexProperty.Cardinality.single, "test", 123, T.id, 321, T.label, "friends");
        verify(mockElement, times(1)).property(VertexProperty.Cardinality.single, "test", 123);
        verify(mockElement, times(0)).property(VertexProperty.Cardinality.single, T.id.getAccessor(), 321);
        verify(mockElement, times(0)).property(VertexProperty.Cardinality.single, T.label.getAccessor(), "friends");
    }

    @Test(expected = ClassCastException.class)
    public void shouldFailTryingToAttachPropertiesWithCardinalityNonStringKey() {
        final Element mockElement = mock(Vertex.class);
        ElementHelper.attachProperties(mockElement, VertexProperty.Cardinality.single, "test", 123, 321, "test");
    }

    @Test
    public void shouldFailTryingToAttachPropertiesWithCardinalityToNullElement() {
        try {
            ElementHelper.attachProperties((Vertex) null, VertexProperty.Cardinality.single, "test", 123, 321, "test");
            fail("Should throw exception since the element argument is null");
        } catch (IllegalArgumentException iae) {
            assertEquals(Graph.Exceptions.argumentCanNotBeNull("vertex").getMessage(), iae.getMessage());
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
        when(mockVertexA.id()).thenReturn("1");
        when(mockVertexB.id()).thenReturn("1");
        assertTrue(ElementHelper.areEqual(mockVertexA, mockVertexB));
    }

    @Test
    public void shouldDetermineVerticesAreNotEqual() {
        final Element mockVertexA = mock(Vertex.class);
        final Element mockVertexB = mock(Vertex.class);
        when(mockVertexA.id()).thenReturn("1");
        when(mockVertexB.id()).thenReturn("2");
        assertFalse(ElementHelper.areEqual(mockVertexA, mockVertexB));
    }

    @Test
    public void shouldDetermineElementsAreEqualWhenBothNull() {
        assertTrue(ElementHelper.areEqual((Element) null, null));
    }

    @Test
    public void shouldDetermineElementsAreNotEqualBecauseFirstArgumentIsNull() {
        Element v = mock(Element.class);
        assertFalse(ElementHelper.areEqual((Element) null, v));
    }

    @Test
    public void shouldDetermineElementsAreNotEqualBecauseSecondArgumentIsNull() {
        Element v = mock(Element.class);
        assertFalse(ElementHelper.areEqual(v, null));
    }

    @Test
    public void shouldDetermineEdgesAreEqual() {
        final Element mockEdgeA = mock(Edge.class);
        final Element mockEdgeB = mock(Edge.class);
        when(mockEdgeA.id()).thenReturn("1");
        when(mockEdgeB.id()).thenReturn("1");
        assertTrue(ElementHelper.areEqual(mockEdgeA, mockEdgeB));
    }

    @Test
    public void shouldDetermineEdgesAreNotEqual() {
        final Element mockEdgeA = mock(Edge.class);
        final Element mockEdgeB = mock(Edge.class);
        when(mockEdgeA.id()).thenReturn("1");
        when(mockEdgeB.id()).thenReturn("2");
        assertFalse(ElementHelper.areEqual(mockEdgeA, mockEdgeB));
    }

    @Test
    public void shouldDeterminePropertiesAreEqualBecauseBothAreNull() {
        assertTrue(ElementHelper.areEqual((Property) null, null));
    }

    @Test
    public void shouldDeterminePropertiesAreNotEqualBecauseFirstArgumentIsNull() {
        assertFalse(ElementHelper.areEqual((Property) null, "some object"));
    }

    @Test
    public void shouldDeterminePropertiesAreNotEqualTestBecauseSecondArgumentIsNull() {
        final Property mockProperty = mock(Property.class);
        assertFalse(ElementHelper.areEqual(mockProperty, null));
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

    @Test
    public void shouldDeterminePropertiesAreEqual() {
        final Property mockPropertyA = mock(Property.class);
        final Property mockPropertyB = mock(Property.class);
        final Element mockElement = mock(Element.class);
        when(mockPropertyA.isPresent()).thenReturn(true);
        when(mockPropertyB.isPresent()).thenReturn(true);
        when(mockPropertyA.element()).thenReturn(mockElement);
        when(mockPropertyB.element()).thenReturn(mockElement);
        when(mockPropertyA.key()).thenReturn("k");
        when(mockPropertyB.key()).thenReturn("k");
        when(mockPropertyA.value()).thenReturn("v");
        when(mockPropertyB.value()).thenReturn("v");

        assertTrue(ElementHelper.areEqual(mockPropertyA, mockPropertyB));
    }

    @Test
    public void shouldDeterminePropertiesAreEqualWhenElementsAreDifferent() {
        final Property mockPropertyA = mock(Property.class);
        final Property mockPropertyB = mock(Property.class);
        final Element mockElement = mock(Element.class);
        final Element mockElementDifferent = mock(Element.class);
        when(mockPropertyA.isPresent()).thenReturn(true);
        when(mockPropertyB.isPresent()).thenReturn(true);
        when(mockPropertyA.element()).thenReturn(mockElement);
        when(mockPropertyB.element()).thenReturn(mockElementDifferent);
        when(mockPropertyA.key()).thenReturn("k");
        when(mockPropertyB.key()).thenReturn("k");
        when(mockPropertyA.value()).thenReturn("v");
        when(mockPropertyB.value()).thenReturn("v");

        assertTrue(ElementHelper.areEqual(mockPropertyA, mockPropertyB));
    }

    @Test
    public void shouldDeterminePropertiesAreNotEqualWhenKeysAreDifferent() {
        final Property mockPropertyA = mock(Property.class);
        final Property mockPropertyB = mock(Property.class);
        final Element mockElement = mock(Element.class);
        when(mockPropertyA.isPresent()).thenReturn(true);
        when(mockPropertyB.isPresent()).thenReturn(true);
        when(mockPropertyA.element()).thenReturn(mockElement);
        when(mockPropertyB.element()).thenReturn(mockElement);
        when(mockPropertyA.key()).thenReturn("k");
        when(mockPropertyB.key()).thenReturn("k1");
        when(mockPropertyA.value()).thenReturn("v");
        when(mockPropertyB.value()).thenReturn("v");

        assertFalse(ElementHelper.areEqual(mockPropertyA, mockPropertyB));
    }

    @Test
    public void shouldDeterminePropertiesAreNotEqualWhenValuesAreDifferent() {
        final Property mockPropertyA = mock(Property.class);
        final Property mockPropertyB = mock(Property.class);
        final Element mockElement = mock(Element.class);
        when(mockPropertyA.isPresent()).thenReturn(true);
        when(mockPropertyB.isPresent()).thenReturn(true);
        when(mockPropertyA.element()).thenReturn(mockElement);
        when(mockPropertyB.element()).thenReturn(mockElement);
        when(mockPropertyA.key()).thenReturn("k");
        when(mockPropertyB.key()).thenReturn("k");
        when(mockPropertyA.value()).thenReturn("v");
        when(mockPropertyB.value()).thenReturn("v1");

        assertFalse(ElementHelper.areEqual(mockPropertyA, mockPropertyB));
    }

    @Test
    public void shouldDetermineAbsentPropertiesEqual() {
        Property<?> p1 = mock(Property.class);
        Property<?> p2 = mock(Property.class);

        when(p1.isPresent()).thenReturn(false);
        when(p2.isPresent()).thenReturn(false);

        assertTrue(ElementHelper.areEqual(p1, p2));
    }

    @Test
    public void shouldDetermineAbsentPropertyNotEqualToPresent() {
        Property<?> p1 = mock(Property.class);
        Property<?> p2 = mock(Property.class);

        when(p1.isPresent()).thenReturn(false);
        when(p2.isPresent()).thenReturn(true);

        assertFalse(ElementHelper.areEqual(p1, p2));
    }

    @Test
    public void shouldDeterminePresentPropertyNotEqualToAbsent() {
        Property<?> p1 = mock(Property.class);
        Property<?> p2 = mock(Property.class);

        when(p1.isPresent()).thenReturn(true);
        when(p2.isPresent()).thenReturn(false);

        assertFalse(ElementHelper.areEqual(p1, p2));
    }

    @Test
    public void shouldExtractKeys() {
        final Set<String> keys = ElementHelper.getKeys("test1", "something", "test2", "something else");
        assertTrue(keys.contains("test1"));
        assertTrue(keys.contains("test2"));
    }

    @Test
    public void shouldGetPairs() {
        final List<Pair<String, Object>> pairs = ElementHelper.asPairs("1", "this", "2", 6l, "3", "other", "4", 1);
        assertEquals(4, pairs.size());
        assertEquals("1", pairs.get(0).getValue0());
        assertEquals("this", pairs.get(0).getValue1());
        assertEquals("2", pairs.get(1).getValue0());
        assertEquals(6l, pairs.get(1).getValue1());
        assertEquals("3", pairs.get(2).getValue0());
        assertEquals("other", pairs.get(2).getValue1());
        assertEquals("4", pairs.get(3).getValue0());
        assertEquals(1, pairs.get(3).getValue1());
    }

    @Test
    public void shouldGetMap() {
        final Map<String, Object> map = ElementHelper.asMap("1", "this", "2", 6l, "3", "other", "4", 1);
        assertEquals(4, map.size());
        assertEquals("this", map.get("1"));
        assertEquals(6l, map.get("2"));
        assertEquals("other", map.get("3"));
        assertEquals(1, map.get("4"));
    }

    @Test
    public void shouldRemoveAKey() {
        final Optional<Object[]> kvs = ElementHelper.remove("2", "1", "this", "2", 6l, "3", "other", "4", 1);
        assertEquals(6, kvs.get().length);
        assertTrue(Stream.of(kvs.get()).noneMatch(kv -> kv.equals("2") || kv.equals(6l)));
    }

    @Test
    public void shouldRemoveAKeyAndReturnEmpty() {
        final Optional<Object[]> kvs = ElementHelper.remove("1", "1", "this");
        assertEquals(Optional.empty(), kvs);
    }

    @Test
    public void shouldRemoveAccessor() {
        final Optional<Object[]> kvs = ElementHelper.remove(T.id, "1", "this", T.id, 6l, "3", "other", "4", 1);
        assertEquals(6, kvs.get().length);
        assertTrue(Stream.of(kvs.get()).noneMatch(kv -> kv.equals("2") || kv.equals(6l)));
    }

    @Test
    public void shouldRemoveAccessorAndReturnEmpty() {
        final Optional<Object[]> kvs = ElementHelper.remove(T.id, T.id, "this");
        assertEquals(Optional.empty(), kvs);
    }

    @Test
    public void shouldUpsertKeyValueByAddingIt() {
        final Object[] oldKvs = new Object[]{"k", "v"};
        final Object[] newKvs = ElementHelper.upsert(oldKvs, "k1", "v1");
        assertEquals(4, newKvs.length);
        assertEquals("k1", newKvs[2]);
        assertEquals("v1", newKvs[3]);
    }

    @Test
    public void shouldUpsertKeyValueByUpdatingIt() {
        final Object[] oldKvs = new Object[]{"k", "v", "k1", "v0"};
        final Object[] newKvs = ElementHelper.upsert(oldKvs, "k1", "v1");
        assertEquals(4, newKvs.length);
        assertEquals("v1", newKvs[3]);
    }

    @Test
    public void shouldReplaceKey() {
        final Object[] oldKvs = new Object[]{"k", "v", "k1", "v0"};
        final Object[] newKvs = ElementHelper.replaceKey(oldKvs, "k", "k2");
        assertEquals(4, newKvs.length);
        assertEquals("k2", newKvs[0]);
        assertEquals("v", newKvs[1]);
        assertEquals("k1", newKvs[2]);
        assertEquals("v0", newKvs[3]);
    }
}
