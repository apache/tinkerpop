/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

// Type-checked by test/unit/enum-types-test.js, never executed. Each @ts-expect-error is itself an error if the line
// it guards compiles, so this file compiles cleanly only while the enum objects are typed with exactly their members.
import { Cardinality, Direction, GType, Order, T } from '../../lib/index.js';
import type { EnumValue } from '../../lib/process/traversal.js';

export const valid: EnumValue[] = [
  Order.desc,
  Cardinality.list,
  Direction.OUT,
  Direction.from_,
  Direction.to,
  GType.string,
  T.id,
];

// @ts-expect-error misspelled member
export const misspelled = Order.dsc;
// @ts-expect-error lowercase Direction members are not part of the API
export const lowercaseDirection = Direction.out;
// @ts-expect-error unknown member
export const unknown = T.name;
