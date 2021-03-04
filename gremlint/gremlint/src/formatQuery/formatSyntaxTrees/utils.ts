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

import { GremlintInternalConfig } from '../types';

export const withIndentation = (localIndentation: number) => (
  config: GremlintInternalConfig,
): GremlintInternalConfig => ({
  ...config,
  localIndentation,
});

export const withZeroIndentation = withIndentation(0);

export const withIncreasedIndentation = (indentationIncrease: number) => (
  config: GremlintInternalConfig,
): GremlintInternalConfig => ({
  ...config,
  localIndentation: config.localIndentation + indentationIncrease,
});

export const withDotInfo = ({
  shouldStartWithDot,
  shouldEndWithDot,
}: {
  shouldStartWithDot: boolean;
  shouldEndWithDot: boolean;
}) => (config: GremlintInternalConfig): GremlintInternalConfig => ({
  ...config,
  shouldStartWithDot,
  shouldEndWithDot,
});

export const withZeroDotInfo = (config: GremlintInternalConfig): GremlintInternalConfig => ({
  ...config,
  shouldStartWithDot: false,
  shouldEndWithDot: false,
});

export const withNoEndDotInfo = (config: GremlintInternalConfig): GremlintInternalConfig => ({
  ...config,
  shouldEndWithDot: false,
});

export const withHorizontalPosition = (horizontalPosition: number) => (
  config: GremlintInternalConfig,
): GremlintInternalConfig => ({
  ...config,
  horizontalPosition,
});

export const withIncreasedHorizontalPosition = (horizontalPositionIncrease: number) => (
  config: GremlintInternalConfig,
): GremlintInternalConfig => ({
  ...config,
  horizontalPosition: config.horizontalPosition + horizontalPositionIncrease,
});
