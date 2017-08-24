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
package org.apache.tinkerpop.gremlin.object.vertices;

import org.apache.tinkerpop.gremlin.object.model.Alias;
import org.apache.tinkerpop.gremlin.object.model.OrderingKey;
import org.apache.tinkerpop.gremlin.object.model.PropertyValue;
import org.apache.tinkerpop.gremlin.object.structure.Element;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

/**
 * The {@link Location} class represents the "locations" vertex property, whose value is kept in the
 * {@link #name} field that is annotated with {@link PropertyValue}. The rest of the fields become
 * its meta-properties.
 *
 * @author Karthick Sankarachary (http://github.com/karthicks)
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Alias(label = "location")
@Accessors(fluent = true, chain = true)
@EqualsAndHashCode(of = {}, callSuper = true)
public class Location extends Element {

  @OrderingKey
  @PropertyValue
  private String name;
  @OrderingKey
  private Instant startTime;
  private Instant endTime;

  public static Location of(String name, int startYear) {
    return of(name, year(startYear));
  }

  public static Location of(String name, Instant startTime) {
    return of(name, startTime, null);
  }

  public static Location of(String name, int startYear, int endYear) {
    return of(name, year(startYear), year(endYear));
  }

  public static Location of(String name, Instant startTime, Instant endTime) {
    return Location.builder().name(name).startTime(startTime).endTime(endTime).build();
  }

  private static Map<Integer, Instant> years = new HashMap<>();

  public static Instant year(int value) {
    Instant instant = years.get(value);
    if (instant != null) {
      return instant;
    }
    instant = LocalDate.parse(String.format("%s-01-01", value)).atStartOfDay()
        .toInstant(ZoneOffset.UTC);
    years.put(value, instant);
    return instant;
  }
}
