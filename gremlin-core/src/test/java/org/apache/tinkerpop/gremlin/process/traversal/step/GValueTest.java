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
package org.apache.tinkerpop.gremlin.process.traversal.step;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class GValueTest {

    @Test
    public void shouldReturnAnExistingGValue() {
        final GValue<Integer> gValue = GValue.of(123);
        final Object returnedGValue = GValue.of(gValue);
        assertEquals(gValue, returnedGValue);
        assertSame(gValue, returnedGValue);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldReturnAnExistInTypedGValue() {
        final Object gValue = GValue.of("x", 123);
        GValue.of("x", gValue);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectVariableNamesStartingWithUnderscore() {
        GValue.of("_invalid", "value");
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectNestedGValues() {
        final GValue inner = GValue.of("inner", "value");
        GValue.of("outer", inner);
    }

    @Test
    public void shouldMaintainTypeInformation() {
        final GValue<List<String>> listValue = GValue.of("list", Arrays.asList("a", "b"));
        assertEquals(listValue.get(), Arrays.asList("a", "b"));
        assertTrue(listValue.get() instanceof List);
    }

    @Test
    public void shouldCreateGValueFromValue() {
        final GValue<Integer> gValue = GValue.of(123);
        assertEquals(123, gValue.get().intValue());
        assertThat(gValue.isVariable(), is(false));
    }

    @Test
    public void shouldCreateGValueFromNameAndValue() {
        final GValue<Integer> gValue = GValue.of("varName", 123);
        assertEquals(123, gValue.get().intValue());
        assertEquals("varName", gValue.getName());
        assertThat(gValue.isVariable(), is(true));
    }

    @Test
    public void shouldCreateGValueFromString() {
        final GValue<String> gValue = GValue.ofString(null, "test");
        assertEquals("test", gValue.get());
        assertThat(gValue.isVariable(), is(false));
    }

    @Test
    public void shouldCreateGValueFromStringWithName() {
        final GValue<String> gValue = GValue.ofString("varName", "test");
        assertEquals("test", gValue.get());
        assertEquals("varName", gValue.getName());
        assertThat(gValue.isVariable(), is(true));
    }

    @Test
    public void shouldCreateGValueFromInteger() {
        final GValue<Integer> gValue = GValue.ofInteger(123);
        assertEquals(123, gValue.get().intValue());
        assertThat(gValue.isVariable(), is(false));
    }

    @Test
    public void shouldCreateGValueFromIntegerWithName() {
        final GValue<Integer> gValue = GValue.ofInteger("varName", 123);
        assertEquals(123, gValue.get().intValue());
        assertEquals("varName", gValue.getName());
        assertThat(gValue.isVariable(), is(true));
    }

    @Test
    public void shouldCreateGValueFromBoolean() {
        final GValue<Boolean> gValue = GValue.ofBoolean(true);
        assertEquals(true, gValue.get());
        assertThat(gValue.isVariable(), is(false));
    }

    @Test
    public void shouldCreateGValueFromBooleanWithName() {
        final GValue<Boolean> gValue = GValue.ofBoolean("varName", true);
        assertEquals(true, gValue.get());
        assertEquals("varName", gValue.getName());
        assertThat(gValue.isVariable(), is(true));
    }

    @Test
    public void shouldCreateGValueFromDouble() {
        final GValue<Double> gValue = GValue.ofDouble(123.45);
        assertEquals(123.45, gValue.get(), 0.0);
        assertThat(gValue.isVariable(), is(false));
    }

    @Test
    public void shouldCreateGValueFromDoubleWithName() {
        final GValue<Double> gValue = GValue.ofDouble("varName", 123.45);
        assertEquals(123.45, gValue.get(), 0.0);
        assertEquals("varName", gValue.getName());
        assertThat(gValue.isVariable(), is(true));
    }

    @Test
    public void shouldCreateGValueFromBigInteger() {
        final GValue<BigInteger> gValue = GValue.ofBigInteger(BigInteger.ONE);
        assertEquals(BigInteger.ONE, gValue.get());
        assertThat(gValue.isVariable(), is(false));
    }

    @Test
    public void shouldCreateGValueFromBigIntegerWithName() {
        final GValue<BigInteger> gValue = GValue.ofBigInteger("varName", BigInteger.ONE);
        assertEquals(BigInteger.ONE, gValue.get());
        assertEquals("varName", gValue.getName());
        assertThat(gValue.isVariable(), is(true));
    }

    @Test
    public void shouldCreateGValueFromBigDecimal() {
        final GValue<BigDecimal> gValue = GValue.ofBigDecimal(BigDecimal.ONE);
        assertEquals(BigDecimal.ONE, gValue.get());
        assertThat(gValue.isVariable(), is(false));
    }

    @Test
    public void shouldCreateGValueFromBigDecimalWithName() {
        final GValue<BigDecimal> gValue = GValue.ofBigDecimal("varName", BigDecimal.ONE);
        assertEquals(BigDecimal.ONE, gValue.get());
        assertEquals("varName", gValue.getName());
        assertThat(gValue.isVariable(), is(true));
    }

    @Test
    public void shouldCreateGValueFromLong() {
        final GValue<Long> gValue = GValue.ofLong(123L);
        assertEquals(123L, gValue.get().longValue());
        assertThat(gValue.isVariable(), is(false));
    }

    @Test
    public void shouldCreateGValueFromLongWithName() {
        final GValue<Long> gValue = GValue.ofLong("varName", 123L);
        assertEquals(123L, gValue.get().longValue());
        assertEquals("varName", gValue.getName());
        assertThat(gValue.isVariable(), is(true));
    }

    @Test
    public void shouldCreateGValueFromMap() {
        final Map<String, String> map = new HashMap<String,String>() {{
            put("key", "value");
        }};
        final GValue<Map> gValue = GValue.ofMap(map);
        assertEquals(map, gValue.get());
        assertThat(gValue.isVariable(), is(false));
    }

    @Test
    public void shouldCreateGValueFromMapWithName() {
        final Map<String, String> map = new HashMap<String,String>() {{
            put("key", "value");
        }};
        final GValue<Map<?,?>> gValue = GValue.ofMap("varName", map);
        assertEquals(map, gValue.get());
        assertEquals("varName", gValue.getName());
        assertThat(gValue.isVariable(), is(true));
    }

    @Test
    public void shouldCreateGValueFromList() {
        final List<String> list = Arrays.asList("value1", "value2");
        final GValue<List<String>> gValue = GValue.ofList(list);
        assertEquals(list, gValue.get());
        assertThat(gValue.isVariable(), is(false));
    }

    @Test
    public void shouldCreateGValueFromListWithName() {
        final List<String> list = Arrays.asList("value1", "value2");
        final GValue<List<String>> gValue = GValue.ofList("varName", list);
        assertEquals(list, gValue.get());
        assertEquals("varName", gValue.getName());
        assertThat(gValue.isVariable(), is(true));
    }

    @Test
    public void shouldCreateGValueFromSet() {
        final Set<String> set = new HashSet<>(Arrays.asList("value1", "value2"));
        final GValue<Set> gValue = GValue.ofSet(set);
        assertEquals(set, gValue.get());
        assertThat(gValue.isVariable(), is(false));
    }

    @Test
    public void shouldCreateGValueFromSetWithName() {
        final Set<String> set = new HashSet<>(Arrays.asList("value1", "value2"));
        final GValue<Set> gValue = GValue.ofSet("varName", set);
        assertEquals(set, gValue.get());
        assertEquals("varName", gValue.getName());
        assertThat(gValue.isVariable(), is(true));
    }

    @Test
    public void shouldCreateGValueFromVertex() {
        final Vertex vertex = mock(Vertex.class);
        final GValue<Vertex> gValue = GValue.ofVertex(vertex);
        assertEquals(vertex, gValue.get());
        assertThat(gValue.isVariable(), is(false));
    }

    @Test
    public void shouldCreateGValueFromVertexWithName() {
        final Vertex vertex = mock(Vertex.class);
        final GValue<Vertex> gValue = GValue.ofVertex("varName", vertex);
        assertEquals(vertex, gValue.get());
        assertEquals("varName", gValue.getName());
        assertThat(gValue.isVariable(), is(true));
    }

    @Test
    public void shouldHaveForMatchingType() {
        final GValue<Integer> gValue = GValue.of(123);
        assertThat(GValue.valueInstanceOf(gValue, Integer.class), is(true));
    }

    @Test
    public void valueInstanceOfShouldReturnFalseForNonMatchingType() {
        final GValue<Integer> gValue = GValue.of(123);
        assertThat(GValue.valueInstanceOf(gValue, String.class), is(false));
    }

    @Test
    public void valueInstanceOfShouldReturnFalseForNonGValueObject() {
        final String nonGValue = "test";
        assertThat(GValue.valueInstanceOf(nonGValue, String.class), is(false));
    }

    @Test
    public void valueInstanceOfShouldReturnFalseForNullObject() {
        assertThat(GValue.valueInstanceOf(null, String.class), is(false));
    }

    @Test
    public void getShouldReturnGValueValue() {
        final GValue<Integer> gValue = GValue.of(123);
        assertThat(GValue.getFrom(gValue), is(123));
    }

    @Test
    public void getShouldReturnObjectAsIs() {
        final String value = "test";
        assertThat(GValue.getFrom(value), is(value));
    }

    @Test
    public void getShouldReturnNullForNullInput() {
        assertThat(GValue.getFrom(null), is((Object) null));
    }

    @Test
    public void shouldConvertObjectArrayToGValues() {
        final Object[] input = {1, "string", true};
        final GValue<?>[] expected = {GValue.of(1), GValue.of("string"), GValue.of(true)};
        final GValue<?>[] result = GValue.ensureGValues(input);
        assertArrayEquals(expected, result);
    }

    @Test
    public void shouldPreserveExistingGValuesInArray() {
        final GValue<Integer> gValue = GValue.of(123);
        final Object[] input = {gValue, "string"};
        final GValue<?>[] expected = {gValue, GValue.of("string")};
        final GValue<?>[] result = GValue.ensureGValues(input);
        assertArrayEquals(expected, result);
    }

    @Test
    public void shouldHandleEmptyArray() {
        final Object[] input = {};
        final GValue<?>[] expected = {};
        final GValue<?>[] result = GValue.ensureGValues(input);
        assertArrayEquals(expected, result);
    }

    @Test
    public void shouldHandleArrayWithNullValues() {
        final Object[] input = {null, "string"};
        final GValue<?>[] expected = {GValue.of(null), GValue.of("string")};
        final GValue<?>[] result = GValue.ensureGValues(input);
        assertArrayEquals(expected, result);
    }

    @Test
    public void shouldResolveGValuesToValues() {
        final GValue<?>[] input = {GValue.of(1), GValue.of("string"), GValue.of(true)};
        final Object[] expected = {1, "string", true};
        final Object[] result = GValue.resolveToValues(input);
        assertArrayEquals(expected, result);
    }

    @Test
    public void shouldHandleEmptyGValuesArray() {
        final GValue<?>[] input = {};
        final Object[] expected = {};
        final Object[] result = GValue.resolveToValues(input);
        assertArrayEquals(expected, result);
    }

    @Test
    public void shouldHandleGValuesArrayWithNullValues() {
        final GValue<?>[] input = {GValue.of(null), GValue.of("string")};
        final Object[] expected = {null, "string"};
        final Object[] result = GValue.resolveToValues(input);
        assertArrayEquals(expected, result);
    }

    @Test
    public void shouldHandleMixedTypeGValuesArray() {
        final GValue<?>[] input = {GValue.of(1), GValue.of("string"), GValue.of(true), GValue.of(123.45)};
        final Object[] expected = {1, "string", true, 123.45};
        final Object[] result = GValue.resolveToValues(input);
        assertArrayEquals(expected, result);
    }

    @Test
    public void equalsShouldReturnTrueForSameObject() {
        final GValue<Integer> gValue = GValue.of(123);
        assertEquals(true, gValue.equals(gValue));
    }

    @Test
    public void equalsShouldReturnFalseForNull() {
        final GValue<Integer> gValue = GValue.of(123);
        assertEquals(false, gValue.equals(null));
    }

    @Test
    public void equalsShouldReturnFalseForDifferentClass() {
        final GValue<Integer> gValue = GValue.of(123);
        assertEquals(false, gValue.equals("string"));
    }

    @Test
    public void equalsShouldReturnTrueForEqualGValues() {
        final GValue<Integer> gValue1 = GValue.of(123);
        final GValue<Integer> gValue2 = GValue.of(123);
        assertEquals(true, gValue1.equals(gValue2));
    }

    @Test
    public void equalsShouldReturnFalseForDifferentNames() {
        final GValue<Integer> gValue1 = GValue.of("name1", 123);
        final GValue<Integer> gValue2 = GValue.of("name2", 123);
        assertEquals(false, gValue1.equals(gValue2));
    }

    @Test
    public void equalsShouldReturnFalseForDifferentTypes() {
        final GValue<Integer> gValue1 = GValue.of(123);
        final GValue<String> gValue2 = GValue.of("123");
        assertEquals(false, gValue1.equals(gValue2));
    }

    @Test
    public void equalsShouldReturnFalseForDifferentValues() {
        final GValue<Integer> gValue1 = GValue.of(123);
        final GValue<Integer> gValue2 = GValue.of(456);
        assertEquals(false, gValue1.equals(gValue2));
    }

    @Test
    public void valueOfShouldReturnGValueValue() {
        final GValue<Integer> gValue = GValue.of(123);
        assertEquals(123, GValue.valueOf(gValue));
    }

    @Test
    public void valueOfShouldReturnObjectAsIs() {
        final String value = "test";
        assertEquals("test", GValue.valueOf(value));
    }

    @Test
    public void valueOfShouldReturnNullForNullInput() {
        assertNull(GValue.valueOf(null));
        assertNull(null);
    }
    
    @Test
    public void cloneShouldHandleNullValue() throws CloneNotSupportedException {
        final GValue<String> original = GValue.of("varName", null);
        final GValue<String> cloned = original.clone();
        
        assertBasicCloneProperties(original, cloned);
        assertNull(cloned.get());
    }

    @Test
    public void cloneShouldHandleNullValueWithoutName() throws CloneNotSupportedException {
        final GValue<String> original = GValue.of(null);
        final GValue<String> cloned = original.clone();
        
        assertBasicCloneProperties(original, cloned);
        assertNull(cloned.getName());
        assertNull(cloned.get());
    }

    @Test
    public void cloneShouldDeepCloneSerializableList() throws CloneNotSupportedException {
        final List<String> originalList = new ArrayList<>(Arrays.asList("item1", "item2"));
        final GValue<List<String>> original = GValue.of("listVar", originalList);
        final GValue<List<String>> cloned = original.clone();
        
        assertDeepClone(original, cloned);
        
        // Modify original list to verify deep clone
        originalList.add("item3");
        assertEquals(2, cloned.get().size()); // Cloned list should not be affected
        assertEquals(3, original.get().size()); // Original list should be affected
    }

    @Test
    public void cloneShouldDeepCloneSerializableMap() throws CloneNotSupportedException {
        final Map<String, String> originalMap = new HashMap<>();
        originalMap.put("key1", "value1");
        originalMap.put("key2", "value2");
        
        final GValue<Map<String, String>> original = GValue.of("mapVar", originalMap);
        final GValue<Map<String, String>> cloned = original.clone();
        
        assertDeepClone(original, cloned);
        
        // Modify original map to verify deep clone
        originalMap.put("key3", "value3");
        assertEquals(2, cloned.get().size()); // Cloned map should not be affected
        assertEquals(3, original.get().size()); // Original map should be affected
    }

    @Test
    public void cloneShouldDeepCloneSerializableSet() throws CloneNotSupportedException {
        final Set<String> originalSet = new HashSet<>(Arrays.asList("item1", "item2"));
        final GValue<Set<String>> original = GValue.of("setVar", originalSet);
        final GValue<Set<String>> cloned = original.clone();
        
        assertDeepClone(original, cloned);
        
        // Modify original set to verify deep clone
        originalSet.add("item3");
        assertEquals(2, cloned.get().size()); // Cloned set should not be affected
        assertEquals(3, original.get().size()); // Original set should be affected
    }

    @Test
    public void cloneShouldDeepClonePrimitiveInt() throws CloneNotSupportedException {
        final GValue<Integer> original = GValue.of("intVar", 42);
        assertImmutableClone(original, original.clone());
    }

    @Test
    public void cloneShouldDeepCloneString() throws CloneNotSupportedException {
        final GValue<String> original = GValue.of("stringVar", "test value");
        assertImmutableClone(original, original.clone());
    }

    @Test
    public void cloneShouldDeepCloneBigInteger() throws CloneNotSupportedException {
        final GValue<BigInteger> original = GValue.of("bigIntVar", new BigInteger("12345678901234567890"));
        assertImmutableClone(original, original.clone());
    }

    @Test
    public void cloneShouldDeepCloneBigDecimal() throws CloneNotSupportedException {
        final GValue<BigDecimal> original = GValue.of("bigDecVar", new BigDecimal("123.456789"));
        assertImmutableClone(original, original.clone());
    }

    @Test
    public void cloneShouldShallowCloneNonSerializableValue() throws CloneNotSupportedException {
        final GValue<NonSerializableClass> original = GValue.of("nonSerVar", new NonSerializableClass("test"));
        assertShallowClone(original, original.clone());
    }

    @Test
    public void cloneShouldFallbackToShallowCloneOnSerializationFailure() throws CloneNotSupportedException {
        final GValue<FailingSerializableClass> original = GValue.of("failVar", new FailingSerializableClass("test"));
        assertShallowClone(original, original.clone());
    }

    @Test
    public void cloneShouldHandleComplexNestedSerializableStructures() throws CloneNotSupportedException {
        final Map<String, List<String>> complexMap = new HashMap<>();
        complexMap.put("list1", new ArrayList<>(Arrays.asList("a", "b")));
        complexMap.put("list2", new ArrayList<>(Arrays.asList("c", "d")));
        
        final GValue<Map<String, List<String>>> original = GValue.of("complexVar", complexMap);
        final GValue<Map<String, List<String>>> cloned = original.clone();
        
        assertNotSame(original, cloned);
        assertEquals(original.getName(), cloned.getName());
        assertDeepClone(original, cloned);
        
        // Verify deep cloning of nested structures
        original.get().get("list1").add("e");
        assertEquals(2, cloned.get().get("list1").size()); // Cloned nested list should not be affected
        assertEquals(3, original.get().get("list1").size()); // Original nested list should be affected
    }
    
    private <T> void assertBasicCloneProperties(final GValue<T> original, final GValue<T> cloned) {
        assertNotSame(original, cloned);
        assertEquals(original.getName(), cloned.getName());
        assertEquals(original, cloned);
        assertEquals(original.isVariable(), cloned.isVariable());
    }

    private <T> void assertDeepClone(final GValue<T> original, final GValue<T> cloned) {
        assertBasicCloneProperties(original, cloned);
        assertEquals(original.get(), cloned.get());
        assertNotSame(original.get(), cloned.get());
    }

    private <T> void assertShallowClone(final GValue<T> original, final GValue<T> cloned) {
        assertBasicCloneProperties(original, cloned);
        assertSame(original.get(), cloned.get());
    }

    private <T> void assertImmutableClone(final GValue<T> original, final GValue<T> cloned) {
        assertBasicCloneProperties(original, cloned);
        assertEquals(original.get(), cloned.get());
    }
    
    private static class NonSerializableClass {
        private final String value;
        
        public NonSerializableClass(final String value) {
            this.value = value;
        }
        
        public String getValue() {
            return value;
        }
        
        @Override
        public boolean equals(final Object o) {
            return EqualsBuilder.reflectionEquals(this, o);
        }
        
        @Override
        public int hashCode() {
            return HashCodeBuilder.reflectionHashCode(this);
        }
    }
    
    private static class FailingSerializableClass implements Serializable {
        private static final long serialVersionUID = 1L;
        private final String value;
        
        public FailingSerializableClass(final String value) {
            this.value = value;
        }
        
        public String getValue() {
            return this.value;
        }
        
        // This will cause serialization to fail
        private void writeObject(final java.io.ObjectOutputStream out) throws java.io.IOException {
            throw new java.io.IOException("Intentional serialization failure for testing");
        }
        
        @Override
        public boolean equals(final Object o) {
            return EqualsBuilder.reflectionEquals(this, o);
        }
        
        @Override
        public int hashCode() {
            return HashCodeBuilder.reflectionHashCode(this);
        }
    }
}