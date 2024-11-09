﻿#region License

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#endregion

using System.Collections.Generic;
using System.Globalization;
using System.Text.Json;

namespace Gremlin.Net.Structure.IO.GraphSON
{
    internal abstract class NumberConverter<T> : IGraphSONDeserializer, IGraphSONSerializer where T : notnull
    {
        protected abstract string GraphSONTypeName { get; }
        protected virtual string Prefix => "g";
        protected virtual bool StringifyValue => false;

        public dynamic Objectify(JsonElement graphsonObject, GraphSONReader reader)
        {
            return FromJsonElement(graphsonObject);
        }

        protected abstract dynamic FromJsonElement(JsonElement graphson);

        public Dictionary<string, dynamic> Dictify(dynamic objectData, GraphSONWriter writer)
        {
            T number = objectData;
            var value = ConvertInvalidNumber(number);
            if (StringifyValue)
            {
                value = string.Format(CultureInfo.InvariantCulture, "{0}", value);
            }
            return GraphSONUtil.ToTypedValue(GraphSONTypeName, value, Prefix);
        }

        protected virtual object ConvertInvalidNumber(T number) => number;
    }
}