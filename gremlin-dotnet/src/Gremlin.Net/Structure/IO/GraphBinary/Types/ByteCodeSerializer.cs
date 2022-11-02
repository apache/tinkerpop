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
using System.Threading;
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
        protected override async Task WriteValueAsync(Bytecode value, Stream stream, GraphBinaryWriter writer,
            CancellationToken cancellationToken = default)
        {
            await WriteInstructionsAsync(value.StepInstructions, stream, writer, cancellationToken)
                .ConfigureAwait(false);
            await WriteInstructionsAsync(value.SourceInstructions, stream, writer, cancellationToken)
                .ConfigureAwait(false);
        }

        private static async Task WriteInstructionsAsync(IReadOnlyCollection<Instruction> instructions, Stream stream,
            GraphBinaryWriter writer, CancellationToken cancellationToken)
        {
            await writer.WriteNonNullableValueAsync(instructions.Count, stream, cancellationToken).ConfigureAwait(false);

            foreach (var instruction in instructions)
            {
                await writer.WriteNonNullableValueAsync(instruction.OperatorName, stream, cancellationToken)
                    .ConfigureAwait(false);
                await WriteArgumentsAsync(instruction.Arguments, stream, writer, cancellationToken)
                    .ConfigureAwait(false);
            }
        }

        private static async Task WriteArgumentsAsync(object?[] arguments, Stream stream, GraphBinaryWriter writer,
            CancellationToken cancellationToken)
        {
            await writer.WriteNonNullableValueAsync(arguments.Length, stream, cancellationToken).ConfigureAwait(false);

            foreach (var value in arguments)
            {
                await writer.WriteAsync(value, stream, cancellationToken).ConfigureAwait(false);
            }
        }

        /// <inheritdoc />
        protected override async Task<Bytecode> ReadValueAsync(Stream stream, GraphBinaryReader reader,
            CancellationToken cancellationToken = default)
        {
            var result = new Bytecode();

            var stepsLength = (int) await reader.ReadNonNullableValueAsync<int>(stream, cancellationToken).ConfigureAwait(false);
            for (var i = 0; i < stepsLength; i++)
            {
                result.AddStep(
                    (string)await reader.ReadNonNullableValueAsync<string>(stream, cancellationToken).ConfigureAwait(false),
                    await ReadArgumentsAsync(stream, reader, cancellationToken).ConfigureAwait(false));
            }
            
            var sourcesLength = await stream.ReadIntAsync(cancellationToken).ConfigureAwait(false);
            for (var i = 0; i < sourcesLength; i++)
            {
                result.AddSource(
                    (string)await reader.ReadNonNullableValueAsync<string>(stream, cancellationToken).ConfigureAwait(false),
                    await ReadArgumentsAsync(stream, reader, cancellationToken).ConfigureAwait(false));
            }
            
            return result;
        }

        private static async Task<object?[]> ReadArgumentsAsync(Stream stream, GraphBinaryReader reader,
            CancellationToken cancellationToken)
        {
            var valuesLength =
                (int)await reader.ReadNonNullableValueAsync<int>(stream, cancellationToken).ConfigureAwait(false);
            var values = new object?[valuesLength];
            for (var i = 0; i < valuesLength; i++)
            {
                values[i] = await reader.ReadAsync(stream, cancellationToken).ConfigureAwait(false);
            }
            return values;
        }
    }
}