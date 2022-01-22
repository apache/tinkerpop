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

const ioc = {};

ioc.DataType = require('./internals/DataType');
ioc.utils = require('./internals/utils');

ioc.intSerializer = new (require('./internals/IntSerializer'))(ioc);
ioc.stringSerializer = new (require('./internals/StringSerializer'))(ioc);
ioc.mapSerializer = new (require('./internals/MapSerializer'))(ioc);
ioc.uuidSerializer = new (require('./internals/UuidSerializer'))(ioc);
ioc.bytecodeSerializer = new (require('./internals/BytecodeSerializer'))(ioc);

ioc.graphBinaryReader = new (require('./internals/GraphBinaryReader.js'))(ioc);
ioc.graphBinaryWriter = new (require('./internals/GraphBinaryWriter'))(ioc);

ioc.anySerializer = new (require('./internals/AnySerializer'))(ioc);

module.exports = ioc;
