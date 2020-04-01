#region License

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
using System.Linq;
using System.Text.Json;

namespace Gremlin.Net.Structure.IO.GraphSON
{
    internal class Path3Deserializer : IGraphSONDeserializer
    {
        public dynamic Objectify(JsonElement graphsonObject, GraphSONReader reader)
        {
            // "labels" is a object[] where each item is ISet<object>
            var labelProperty = (object[])reader.ToObject(graphsonObject.GetProperty("labels"));
            var labels = labelProperty
                .Select(x => new HashSet<string>(((ISet<object>)x).Cast<string>()))
                .ToList<ISet<string>>();
            // "objects" is an object[]
            object[] objects = reader.ToObject(graphsonObject.GetProperty("objects"));
            return new Path(labels, objects);
        }
    }
}