/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.object.reflect;

import org.apache.tinkerpop.gremlin.object.model.DefaultValue;
import org.apache.tinkerpop.gremlin.object.structure.Element;
import org.javatuples.Pair;

import java.lang.reflect.Field;
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import lombok.SneakyThrows;

/**
 * {@link Primitives} helps us work with properties whose type is a primitive, or a wrapper thereof.
 * It also handles conversions between various formats of time-based properties, and default values
 * for various primitive types.
 *
 * <p>
 * It is important to know which properties are treated as primitive by the graph system, so that
 * the object graph can pass them as-is to the traversal. If a graph system defines custom primitive
 * types, they may register them here using the {@link #registerPrimitiveClass} method.
 *
 * @author Karthick Sankarachary (http://github.com/karthicks)
 */
@SuppressWarnings({"rawtypes", "PMD.AvoidUsingShortType"})
public final class Primitives {

  /**
   * If false, then primitive fields that are {@code PrimaryKey} or {@code OrderingKey} cannot be
   * stored using the primitive default values.
   */
  public static boolean allowDefaultKeys = true;

  /**
   * What is the primitive value for the primitive classes that we know of?
   */
  private static final Map<Class, Object> PRIMITIVE_DEFAULTS = new HashMap<>();

  /**
   * How can one parse a primitive value from the string specified in {@link DefaultValue#value()}?
   */
  private static final Map<Class, Function<String, ?>> STRING_CONVERTERS = new HashMap<>();

  /**
   * How do we go from one type of time class to another?
   */
  private static final Map<Pair<Class, Class>, Function<Object, Object>> TIME_CONVERTERS =
      new HashMap<>();

  static {
    // Register the known primitive types and default values thereof.
    registerPrimitiveClass(boolean.class, false);
    registerPrimitiveClass(byte.class, 0xB);
    registerPrimitiveClass(char.class, '\u0000');
    registerPrimitiveClass(double.class, 0D);
    registerPrimitiveClass(float.class, 0F);
    registerPrimitiveClass(int.class, 0);
    registerPrimitiveClass(long.class, 0L);
    registerPrimitiveClass(short.class, 0);
    registerPrimitiveClass(Boolean.class, false);
    registerPrimitiveClass(Byte.class, 0xB);
    registerPrimitiveClass(Character.class, '\u0000');
    registerPrimitiveClass(Double.class, 0D);
    registerPrimitiveClass(Float.class, 0F);
    registerPrimitiveClass(Integer.class, 0);
    registerPrimitiveClass(Long.class, 0L);
    registerPrimitiveClass(Short.class, 0);
    registerPrimitiveClass(Void.class);
    registerPrimitiveClass(String.class, "");
    registerPrimitiveClass(Date.class, new Date(0));
    registerPrimitiveClass(Instant.class, Instant.ofEpochMilli(0));

    // Register the string parsers for the known primitive types.
    registerStringConverters(byte.class, Byte::parseByte);
    registerStringConverters(char.class, string -> string.charAt(0));
    registerStringConverters(double.class, Double::parseDouble);
    registerStringConverters(float.class, Float::parseFloat);
    registerStringConverters(int.class, Integer::parseInt);
    registerStringConverters(long.class, Long::parseLong);
    registerStringConverters(short.class, Short::parseShort);
    registerStringConverters(Byte.class, Byte::parseByte);
    registerStringConverters(Character.class, string -> string.charAt(0));
    registerStringConverters(Double.class, Double::parseDouble);
    registerStringConverters(Float.class, Float::parseFloat);
    registerStringConverters(Integer.class, Integer::parseInt);
    registerStringConverters(Long.class, Long::parseLong);
    registerStringConverters(Short.class, Short::parseShort);
    registerStringConverters(String.class, Function.identity());

    // Register the time converters for the know time types.
    registerTimeConverters(Date.class, Date.class, Function.identity());
    registerTimeConverters(Date.class, Instant.class, value -> toInstant((Date) value));
    registerTimeConverters(Instant.class, Date.class, value -> toDate((Instant) value));
    registerTimeConverters(Instant.class, Instant.class, Function.identity());
    registerTimeConverters(Long.class, Date.class, value -> toDate((Long) value));
    registerTimeConverters(Long.class, Instant.class, value -> toInstant((Long) value));
    registerTimeConverters(Date.class, Long.class, value -> toEpoch((Date) value));
    registerTimeConverters(Instant.class, Long.class, value -> toEpoch((Instant) value));
  }

  private Primitives() {}

  public static void registerPrimitiveClass(Class primitiveClass) {
    registerPrimitiveClass(primitiveClass, null);
  }

  @SneakyThrows
  public static void registerPrimitiveClass(Class primitiveClass, Object primitiveDefault) {
    PRIMITIVE_DEFAULTS.put(primitiveClass, primitiveDefault);
  }

  @SuppressWarnings("unchecked")
  public static <P> void registerStringConverters(Class<P> primitiveClass,
      Function<String, P> stringConverter) {
    STRING_CONVERTERS.put(primitiveClass, stringConverter);
  }

  @SuppressWarnings("unchecked")
  public static void registerTimeConverters(Class sourceTimeClass, Class targetTimeClass,
      Function<Object, Object> timeConverter) {
    TIME_CONVERTERS.put(new Pair(sourceTimeClass, targetTimeClass), timeConverter);
  }

  public static boolean isPrimitive(Field field) {
    return isPrimitive(field.getType());
  }


  public static boolean isPrimitive(Class clazz) {
    return clazz.isEnum() || clazz.isPrimitive() || PRIMITIVE_DEFAULTS.containsKey(clazz);
  }

  public static boolean isPrimitiveDefault(Field field, Object value) {
    return value == null || (isPrimitive(field) && PRIMITIVE_DEFAULTS.get(field.getType())
        .equals(value));
  }

  public static Object asPrimitiveType(Field field, String defaultValue) {
    Function<String, ?> stringConverter = STRING_CONVERTERS.get(field.getType());
    return (stringConverter != null) ? stringConverter.apply(defaultValue) : null;
  }

  /**
   * The given object is considered to be "missing", in the sense that it does not have a value, if
   * it is {@code null} or it's a default primitive value, and {@link #allowDefaultKeys} is false.
   */
  public static boolean isMissing(Object object) {
    if (object == null) {
      return true;
    }
    if (allowDefaultKeys) {
      return false;
    }
    Class objectClass = object.getClass();
    Object defaultValue = PRIMITIVE_DEFAULTS.get(objectClass);
    return object.equals(defaultValue);
  }

  public static boolean isTimeType(Class clazz) {
    return clazz.equals(Date.class) || clazz.equals(Instant.class) || clazz.equals(Long.class);
  }

  public static Date toDate(long epoch) {
    return toDate(epoch, TimeUnit.MILLISECONDS);
  }

  public static Date toDate(long epoch, TimeUnit timeUnit) {
    return new Date(timeUnit.toMillis(epoch));
  }

  public static long toEpoch(Date date) {
    return date.getTime();
  }

  public static Instant toInstant(long epoch) {
    return Instant.ofEpochMilli(epoch);
  }

  public static long toEpoch(Instant instant) {
    return instant.toEpochMilli();
  }

  public static Instant toInstant(Date date) {
    return date.toInstant();
  }

  public static Date toDate(Instant instant) {
    return Date.from(instant);
  }

  /**
   * Convert the given time value to an instance of the target time type.
   */
  @SuppressWarnings("unchecked")
  public static <T> T toTimeType(Object timeValue, Class<T> targetTimeType) {
    Class valueClass = timeValue.getClass();
    Function<Object, Object> timeConverter =
        TIME_CONVERTERS.get(new Pair(valueClass, targetTimeType));
    if (timeConverter == null) {
      throw Element.Exceptions.invalidTimeType(targetTimeType, timeValue);
    }
    return (T) timeConverter.apply(timeValue);
  }
}
