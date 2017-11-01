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

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Gremlin.Net.Process.Traversal
{
    /// <summary>
    ///     A language agnostic representation of <see cref="ITraversal" /> mutations.
    /// </summary>
    /// <remarks>
    ///     Bytecode is simply a list of ordered instructions.
    ///     Bytecode can be serialized between environments and machines by way of a GraphSON representation.
    ///     Thus, Gremlin.Net can create bytecode in C# and ship it to Gremlin-Java for evaluation in Java.
    /// </remarks>
    public class Bytecode
    {
        private static readonly object[] EmptyArray = new object[0];

        /// <summary>
        ///     Initializes a new instance of the <see cref="Bytecode" /> class.
        /// </summary>
        public Bytecode()
        {
        }

        /// <summary>
        ///     Initializes a new instance of the <see cref="Bytecode" /> class.
        /// </summary>
        /// <param name="byteCode">Already existing <see cref="Bytecode" /> that should be cloned.</param>
        public Bytecode(Bytecode byteCode)
        {
            SourceInstructions = new List<Instruction>(byteCode.SourceInstructions);
            StepInstructions = new List<Instruction>(byteCode.StepInstructions);
        }

        /// <summary>
        ///     Gets the traversal source instructions.
        /// </summary>
        public List<Instruction> SourceInstructions { get; } = new List<Instruction>();

        /// <summary>
        ///     Gets the <see cref="ITraversal" /> instructions.
        /// </summary>
        public List<Instruction> StepInstructions { get; } = new List<Instruction>();

        /// <summary>
        ///     Add a traversal source instruction to the bytecode.
        /// </summary>
        /// <param name="sourceName">The traversal source method name (e.g. withSack()).</param>
        /// <param name="args">The traversal source method arguments.</param>
        public void AddSource(string sourceName, params object[] args)
        {
            SourceInstructions.Add(new Instruction(sourceName, FlattenArguments(args)));
            Bindings.Clear();
        }

        /// <summary>
        ///     Adds a <see cref="ITraversal" /> instruction to the bytecode.
        /// </summary>
        /// <param name="stepName">The traversal method name (e.g. out()).</param>
        /// <param name="args">The traversal method arguments.</param>
        public void AddStep(string stepName, params object[] args)
        {
            StepInstructions.Add(new Instruction(stepName, FlattenArguments(args)));
            Bindings.Clear();
        }

        private object[] FlattenArguments(object[] arguments)
        {
            if (arguments.Length == 0)
                return EmptyArray;
            var flatArguments = new List<object>();
            foreach (var arg in arguments)
            {
                if (arg is object[] objects)
                {
                    flatArguments.AddRange(objects.Select(nestObject => ConvertArgument(nestObject, true)));
                }
                else
                {
                    flatArguments.Add(ConvertArgument(arg, true));
                }
            }
            return flatArguments.ToArray();
        }

        private object ConvertArgument(object argument, bool searchBindings)
        {
            if (searchBindings)
            {
                var variable = Bindings.GetBoundVariable(argument);
                if (variable != null)
                    return new Binding(variable, ConvertArgument(argument, false));
            }
            if (IsDictionaryType(argument.GetType()))
            {
                var dict = new Dictionary<object, object>();
                foreach (DictionaryEntry item in (IDictionary)argument)
                {
                    dict[ConvertArgument(item.Key, true)] = ConvertArgument(item.Value, true);
                }
                return dict;
            }
            if (IsListType(argument.GetType()))
            {
                var list = new List<object>(((IList) argument).Count);
                list.AddRange(from object item in (IList) argument select ConvertArgument(item, true));
                return list;
            }
            if (IsHashSetType(argument.GetType()))
            {
                var set = new HashSet<object>();
                foreach (var item in (IEnumerable)argument)
                {
                    set.Add(ConvertArgument(item, true));
                }
                return set;
            }
            return argument;
        }

        private bool IsDictionaryType(Type type)
        {
            return type.IsConstructedGenericType && type.GetGenericTypeDefinition() == typeof(Dictionary<,>);
        }

        private bool IsListType(Type type)
        {
            return type.IsConstructedGenericType && type.GetGenericTypeDefinition() == typeof(List<>);
        }

        private bool IsHashSetType(Type type)
        {
            return type.IsConstructedGenericType && type.GetGenericTypeDefinition() == typeof(HashSet<>);
        }
    }
}