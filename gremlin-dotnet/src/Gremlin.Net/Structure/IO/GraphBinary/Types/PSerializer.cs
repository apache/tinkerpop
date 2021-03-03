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

using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Gremlin.Net.Process.Traversal;

namespace Gremlin.Net.Structure.IO.GraphBinary.Types
{
    /// <summary>
    /// A <see cref="P"/> serializer.
    /// </summary>
    public class PSerializer : SimpleTypeSerializer<P>
    {
        /// <summary>
        ///     Initializes a new instance of the <see cref="PSerializer" /> class.
        /// </summary>
        public PSerializer(DataType typeOfP) : base(typeOfP)
        {
        }

        /// <inheritdoc />
        protected override async Task WriteValueAsync(P value, Stream stream, GraphBinaryWriter writer)
        {
            ICollection args = value.Value is ICollection ? value.Value : new List<object> {value.Value};

            var argsLength = value.Other == null ? args.Count : args.Count + 1;
            
            await writer.WriteValueAsync(value.OperatorName, stream, false).ConfigureAwait(false);
            await writer.WriteValueAsync(argsLength, stream, false).ConfigureAwait(false);

            foreach (var arg in args)
            {
                await writer.WriteAsync(arg, stream).ConfigureAwait(false);
            }

            if (value.Other != null)
            {
                await writer.WriteAsync(value.Other, stream).ConfigureAwait(false);
            }
        }

        /// <inheritdoc />
        protected override async Task<P> ReadValueAsync(Stream stream, GraphBinaryReader reader)
        {
            var operatorName = (string) await reader.ReadValueAsync<string>(stream, false).ConfigureAwait(false);
            var argsLength = (int) await reader.ReadValueAsync<int>(stream, false).ConfigureAwait(false);

            var args = new object[argsLength];
            for (var i = 0; i < argsLength; i++)
            {
                args[i] = await reader.ReadAsync(stream).ConfigureAwait(false);
            }

            if (operatorName == "and" || operatorName == "or")
            {
                
                return new P(operatorName, (P) args[0], (P) args[1]);
            }

            if (operatorName == "not")
            {
                return new P(operatorName, (P) args[0]);
            }

            if (argsLength == 1)
            {
                if (DataType == DataType.TextP)
                {
                    return new TextP(operatorName, (string) args[0]);
                }
                return new P(operatorName, args[0]);
            }
            return new P(operatorName, args);
        }
    }
}