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
using System.IO;
using System.Threading.Tasks;
using Gremlin.Net.Process.Traversal;

namespace Gremlin.Net.Structure.IO.GraphBinary.Types
{
    /// <summary>
    /// A <see cref="Bytecode"/> serializer.
    /// </summary>
    public class BytecodeSerializer : SimpleTypeSerializer<Bytecode>
    {
        /// <summary>
        ///     Initializes a new instance of the <see cref="BytecodeSerializer" /> class.
        /// </summary>
        public BytecodeSerializer() : base(DataType.Bytecode)
        {
        }

        /// <inheritdoc />
        protected override async Task WriteValueAsync(Bytecode value, Stream stream, GraphBinaryWriter writer)
        {
            await WriteInstructionsAsync(value.StepInstructions, stream, writer).ConfigureAwait(false);
            await WriteInstructionsAsync(value.SourceInstructions, stream, writer).ConfigureAwait(false);
        }

        private static async Task WriteInstructionsAsync(IReadOnlyCollection<Instruction> instructions, Stream stream,
            GraphBinaryWriter writer)
        {
            await writer.WriteValueAsync(instructions.Count, stream, false).ConfigureAwait(false);

            foreach (var instruction in instructions)
            {
                await writer.WriteValueAsync(instruction.OperatorName, stream, false).ConfigureAwait(false);
                await WriteArgumentsAsync(instruction.Arguments, stream, writer).ConfigureAwait(false);
            }
        }

        private static async Task WriteArgumentsAsync(object[] arguments, Stream stream, GraphBinaryWriter writer)
        {
            await writer.WriteValueAsync(arguments.Length, stream, false).ConfigureAwait(false);

            foreach (var value in arguments)
            {
                await writer.WriteAsync(value, stream).ConfigureAwait(false);
            }
        }

        /// <inheritdoc />
        protected override async Task<Bytecode> ReadValueAsync(Stream stream, GraphBinaryReader reader)
        {
            var result = new Bytecode();

            var stepsLength = (int) await reader.ReadValueAsync<int>(stream, false).ConfigureAwait(false);
            for (var i = 0; i < stepsLength; i++)
            {
                result.AddStep((string) await reader.ReadValueAsync<string>(stream, false).ConfigureAwait(false),
                    await ReadArgumentsAsync(stream, reader).ConfigureAwait(false));
            }
            
            var sourcesLength = await stream.ReadIntAsync().ConfigureAwait(false);
            for (var i = 0; i < sourcesLength; i++)
            {
                result.AddSource((string) await reader.ReadValueAsync<string>(stream, false).ConfigureAwait(false),
                    await ReadArgumentsAsync(stream, reader).ConfigureAwait(false));
            }
            
            return result;
        }

        private static async Task<object[]> ReadArgumentsAsync(Stream stream, GraphBinaryReader reader)
        {
            var valuesLength = (int) await reader.ReadValueAsync<int>(stream, false).ConfigureAwait(false);
            var values = new object[valuesLength];
            for (var i = 0; i < valuesLength; i++)
            {
                values[i] = await reader.ReadAsync(stream).ConfigureAwait(false);
            }
            return values;
        }
    }
}