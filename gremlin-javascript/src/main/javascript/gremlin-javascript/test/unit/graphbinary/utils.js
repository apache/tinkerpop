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

const util = require('util');

const ser_title = ({ i, v }) =>
  `should be able to handle case #${i}: ${util.inspect(v, { depth: 1, breakLength: Infinity })}`;

const des_title = ({ i, b }) =>
  `should be able to handle case #${i}: ${b ? Buffer.from(b).toString('hex') : b}`;

const cbuf_title = ({ i, v }) =>
  `should be able to handle case #${i}: ${util.inspect(v, { depth: 1, breakLength: Infinity })}`;

module.exports = {
  ser_title,
  des_title,
  cbuf_title,
};
