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

/**
 * @author Igor Ostapenko
 */
'use strict';

/*
 * Deserialization error general constructor.
 */
const des_error = ({ des, args, msg }) => {
  let buffer = args[0];
  let buffer_tail = '';
  if (buffer instanceof Buffer) {
    if (buffer.length > 32)
      buffer_tail = '...';
    buffer = buffer.slice(0, 32).toString('hex');
  }
  const fullyQualifiedFormat = args[1];
  const nullable = args[2];

  return new Error(`${des}.deserialize(buffer=${buffer}${buffer_tail}, fullyQualifiedFormat=${fullyQualifiedFormat}, nullable=${nullable}): ${msg}.`);
};

module.exports = {
  des_error,
};
