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
using Gremlin.Net.Process.Traversal;

namespace Gremlin.Net.Structure.IO.GraphSON
{
    internal class BytecodeSerializer : IGraphSONSerializer
    {
        public Dictionary<string, dynamic> Dictify(dynamic bytecodeObj, GraphSONWriter writer)
        {
            Bytecode bytecode = bytecodeObj;

            var valueDict = new Dictionary<string, IEnumerable<IEnumerable<dynamic>>>();
            if (bytecode.SourceInstructions.Count > 0)
                valueDict["source"] = DictifyInstructions(bytecode.SourceInstructions, writer);
            if (bytecode.StepInstructions.Count > 0)
                valueDict["step"] = DictifyInstructions(bytecode.StepInstructions, writer);

            return GraphSONUtil.ToTypedValue(nameof(Bytecode), valueDict);
        }

        private IEnumerable<IEnumerable<dynamic>> DictifyInstructions(IEnumerable<Instruction> instructions,
            GraphSONWriter writer)
        {
            return instructions.Select(instruction => DictifyInstruction(instruction, writer));
        }

        private IEnumerable<dynamic> DictifyInstruction(Instruction instruction, GraphSONWriter writer)
        {
            var result = new List<dynamic> {instruction.OperatorName};
            result.AddRange(instruction.Arguments.Select(arg => writer.ToDict(arg)));
            return result;
        }
    }
}