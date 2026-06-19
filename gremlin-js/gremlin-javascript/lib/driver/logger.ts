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

/** The set of log levels emitted by the driver, ordered from least to most severe. */
export type LogLevel = 'debug' | 'info' | 'warn' | 'error';

/**
 * A logger object with one method per {@link LogLevel}. This shape is compatible with the
 * global `console` as well as common logging libraries.
 */
export interface LoggerObject {
  debug(message: string, ...args: any[]): void;
  info(message: string, ...args: any[]): void;
  warn(message: string, ...args: any[]): void;
  error(message: string, ...args: any[]): void;
}

/**
 * A logger callback that receives the level alongside the message and any extra arguments.
 */
export type LoggerCallback = (level: LogLevel, message: string, ...args: any[]) => void;

/**
 * The `logger` connection option. It may be either a {@link LoggerObject} (e.g. the global
 * `console`) or a {@link LoggerCallback}. Logging is disabled when no logger is provided.
 */
export type Logger = LoggerObject | LoggerCallback;

/**
 * Normalizes the user-supplied {@link Logger} option into a single callback. When no logger is
 * provided, a no-op callback is returned so call sites do not need to null-check.
 *
 * @param {Logger} [logger] The user-supplied logger option.
 * @returns {LoggerCallback} A callback that dispatches to the configured logger.
 */
export function normalizeLogger(logger?: Logger): LoggerCallback {
  if (logger === undefined || logger === null) {
    return () => {};
  }

  if (typeof logger === 'function') {
    return logger;
  }

  if (typeof logger === 'object') {
    return (level: LogLevel, message: string, ...args: any[]) => {
      const fn = (logger as LoggerObject)[level];
      if (typeof fn === 'function') {
        fn.call(logger, message, ...args);
      }
    };
  }

  throw new TypeError('logger must be a function, a logger object, or undefined');
}
