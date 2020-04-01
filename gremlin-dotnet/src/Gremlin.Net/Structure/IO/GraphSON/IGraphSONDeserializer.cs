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

using System.Text.Json;

namespace Gremlin.Net.Structure.IO.GraphSON
{
    /// <summary>
    ///     Supports deserializing GraphSON into an object.
    /// </summary>
    public interface IGraphSONDeserializer
    {
        /// <summary>
        ///     Deserializes GraphSON to an object.
        /// </summary>
        /// <param name="graphsonObject">The GraphSON object to objectify.</param>
        /// <param name="reader">A <see cref="GraphSONReader" /> that can be used to objectify properties of the GraphSON object.</param>
        /// <returns>The deserialized object.</returns>
        dynamic Objectify(JsonElement graphsonObject, GraphSONReader reader);
    }
}