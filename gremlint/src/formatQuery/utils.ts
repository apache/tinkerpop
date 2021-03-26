/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

export const last = <T>(array: T[]): T | undefined => array.slice(-1)[0];

export const pipe = (...fns: ((value: any) => any)[]) => (value: any) => fns.reduce((value, fn) => fn(value), value);

export const spaces = (numberOfSpaces: number): string => Array(numberOfSpaces + 1).join(' ');

export const eq = (a: unknown) => (b: unknown): boolean => a === b;

export const neq = (a: unknown) => (b: unknown): boolean => a !== b;

export const sum = (a: number, b: number): number => a + b;

export const count = (array: any): number => array?.length ?? 0;

export const choose = (
  getCondition: (...params: any[]) => any,
  getThen: (...params: any[]) => any,
  getElse: (...params: any[]) => any,
) => (...params: any[]) => {
  return getCondition(...params) ? getThen(...params) : getElse(...params);
};
