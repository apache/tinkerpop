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
using System.Globalization;
using System.Numerics;
using System.Text;
using System.Threading;
using Gremlin.Net.Process.Traversal.Strategy;
using Gremlin.Net.Process.Traversal.Strategy.Decoration;
using Gremlin.Net.Structure;

namespace Gremlin.Net.Process.Traversal
{
    /// <summary>
    ///     Builds a gremlin-lang compatible string representation of a traversal,
    ///     along with a map of named parameters.
    /// </summary>
    public class GremlinLang : ICloneable, IEquatable<GremlinLang>
    {
        private static readonly object[] EmptyArray = Array.Empty<object>();

        private StringBuilder _gremlin = new();
        private Dictionary<string, object> _parameters = new();
        private static int _paramCount;
        private List<OptionsStrategy> _optionsStrategies = new();

        /// <summary>
        ///     Initializes a new instance of the <see cref="GremlinLang" /> class.
        /// </summary>
        public GremlinLang()
        {
        }

        /// <summary>
        ///     Adds a traversal source instruction to the GremlinLang.
        /// </summary>
        /// <param name="sourceName">The traversal source method name (e.g. withSack()).</param>
        /// <param name="arguments">The traversal source method arguments.</param>
        public void AddSource(string sourceName, params object?[] arguments)
        {
            if (sourceName == "withStrategies" && arguments.Length != 0)
            {
                var args = BuildStrategyArgs(arguments);
                if (args.Length != 0)
                {
                    _gremlin.Append('.').Append("withStrategies").Append('(').Append(args).Append(')');
                }
                return;
            }

            AddToGremlin(sourceName, arguments);
        }

        /// <summary>
        ///     Adds a traversal step instruction to the GremlinLang.
        /// </summary>
        /// <param name="stepName">The traversal method name (e.g. out()).</param>
        /// <param name="arguments">The traversal method arguments.</param>
        public void AddStep(string stepName, params object?[] arguments)
        {
            AddToGremlin(stepName, arguments);
        }

        /// <summary>
        ///     Sets the alias for the traversal source.
        /// </summary>
        /// <param name="g">The alias to set.</param>
        public void AddG(string g)
        {
            _parameters["g"] = g;
        }

        /// <summary>
        ///     Gets the gremlin-lang compatible string representation prefixed with "g".
        /// </summary>
        /// <returns>The gremlin-lang string.</returns>
        public string GetGremlin()
        {
            return GetGremlin("g");
        }

        /// <summary>
        ///     Gets the gremlin-lang compatible string representation with the specified prefix.
        /// </summary>
        /// <param name="prefix">The prefix to use (e.g. "g" or "__").</param>
        /// <returns>The gremlin-lang string.</returns>
        public string GetGremlin(string prefix)
        {
            var gremlinStr = _gremlin.ToString();
            // special handling for CardinalityValueTraversal
            if (gremlinStr.Length != 0 && gremlinStr[0] != '.')
            {
                return gremlinStr;
            }
            return prefix + gremlinStr;
        }

        /// <summary>
        ///     Gets the parameters used in the traversal.
        /// </summary>
        public Dictionary<string, object> Parameters => _parameters;

        /// <summary>
        ///     Gets the list of extracted OptionsStrategy instances.
        /// </summary>
        public List<OptionsStrategy> OptionsStrategies => _optionsStrategies;

        /// <summary>
        ///     Gets a value indicating whether this GremlinLang has no content.
        /// </summary>
        public bool IsEmpty => _gremlin.Length == 0;
        
        /// <summary>
        ///     Gets or sets the raw gremlin string content (without the "g" prefix).
        ///     This is intended for test infrastructure use only, such as prepending
        ///     source steps like withSideEffect after traversal construction.
        /// </summary>
        internal string Gremlin
        {
            get => _gremlin.ToString();
            set
            {
                _gremlin.Clear();
                _gremlin.Append(value);
            }
        }

        /// <summary>
        ///     Resets the static parameter counter. Intended for test determinism.
        /// </summary>
        public static void ResetCounter()
        {
            Interlocked.Exchange(ref _paramCount, 0);
        }

        private void AddToGremlin(string name, object?[] arguments)
        {
            var flattenedArguments = FlattenArguments(arguments);

            // special handling for CardinalityValueTraversal
            if (name == "CardinalityValueTraversal")
            {
                _gremlin.Append("Cardinality.").Append(flattenedArguments[0])
                    .Append('(').Append(flattenedArguments[1]).Append(')');
                return;
            }

            _gremlin.Append('.').Append(name).Append('(');

            for (int i = 0; i < flattenedArguments.Length; i++)
            {
                if (i != 0)
                {
                    _gremlin.Append(',');
                }
                _gremlin.Append(ArgAsString(flattenedArguments[i]));
            }

            _gremlin.Append(')');
        }

        private string ArgAsString(object? arg)
        {
            if (arg == null)
                return "null";

            if (arg is string s)
                return $"\"{EscapeJava(s)}\"";

            if (arg is bool b)
                return b ? "true" : "false";

            if (arg is byte byteVal)
                return $"{byteVal}B";
            if (arg is sbyte sbyteVal)
                return $"{sbyteVal}B";
            if (arg is short shortVal)
                return $"{shortVal}S";
            if (arg is int intVal)
                return intVal.ToString(CultureInfo.InvariantCulture);
            if (arg is long longVal)
                return $"{longVal}L";

            if (arg is BigInteger bigIntVal)
                return $"{bigIntVal}N";

            if (arg is float floatVal)
            {
                if (float.IsNaN(floatVal))
                    return "NaN";
                if (float.IsPositiveInfinity(floatVal))
                    return "+Infinity";
                if (float.IsNegativeInfinity(floatVal))
                    return "-Infinity";
                return $"{FormatFloatingPoint(floatVal.ToString(CultureInfo.InvariantCulture))}F";
            }

            if (arg is double doubleVal)
            {
                if (double.IsNaN(doubleVal))
                    return "NaN";
                if (double.IsPositiveInfinity(doubleVal))
                    return "+Infinity";
                if (double.IsNegativeInfinity(doubleVal))
                    return "-Infinity";
                return $"{FormatFloatingPoint(doubleVal.ToString(CultureInfo.InvariantCulture))}D";
            }

            if (arg is decimal decimalVal)
                return $"{decimalVal.ToString(CultureInfo.InvariantCulture)}M";

            if (arg is DateTimeOffset dto)
                return $"datetime(\"{dto.ToUniversalTime():yyyy-MM-ddTHH:mm:ss.fffZ}\")";

            if (arg is Guid guid)
                return $"UUID(\"{guid}\")";

            if (arg is EnumWrapper enumWrapper)
                return $"{enumWrapper.EnumName}.{enumWrapper.EnumValue}";

            if (arg is Vertex vertex)
                return ArgAsString(vertex.Id);

            if (arg is P p)
                return AsString(p);

            if (arg is GremlinLang gl)
            {
                foreach (var kvp in gl._parameters)
                {
                    _parameters[kvp.Key] = kvp.Value;
                }
                return gl.GetGremlin("__");
            }

            if (arg is ITraversal traversal)
            {
                var traversalGl = traversal.GremlinLang;
                foreach (var kvp in traversalGl._parameters)
                {
                    _parameters[kvp.Key] = kvp.Value;
                }
                return traversalGl.GetGremlin("__");
            }

            if (arg is IGValue gValue)
            {
                var key = gValue.Name;

                if (key == null)
                {
                    return ArgAsString(gValue.ObjectValue);
                }

                if (!IsValidIdentifier(key))
                {
                    throw new ArgumentException($"Invalid parameter name [{key}].");
                }

                if (_parameters.ContainsKey(key))
                {
                    if (!Equals(_parameters[key], gValue.ObjectValue))
                    {
                        throw new ArgumentException($"Parameter with name [{key}] already defined.");
                    }
                }
                else
                {
                    _parameters[key] = gValue.ObjectValue!;
                }
                return key;
            }

            if (arg is CardinalityValue cv)
                return $"Cardinality.{cv.Cardinality!.EnumValue}({ArgAsString(cv.Value)})";

            if (arg is IDictionary dict)
                return AsString(dict);

            if (arg is ISet<object?> objSet)
                return AsStringSet(objSet);

            // Check for generic HashSet types
            if (IsHashSetType(arg.GetType()))
                return AsStringSetEnumerable((IEnumerable)arg);

            if (arg is IList list)
                return AsStringList(list);

            if (arg is Array arr)
                return AsStringList(arr);

            if (arg is Type type)
                return type.Name;

            return AsParameter(arg);
        }

        private string AsParameter(object arg)
        {
            var paramName = $"_{Interlocked.Increment(ref _paramCount) - 1}";
            _parameters[paramName] = arg;
            return paramName;
        }

        private string AsString(P p)
        {
            var sb = new StringBuilder();

            if (p is TextP)
            {
                // Check for connective (and/or) on TextP
                if (p.OperatorName == "and" || p.OperatorName == "or")
                {
                    // Value is the left P, Other is the right P
                    sb.Append(ArgAsString(p.Value));
                    sb.Append('.').Append(p.OperatorName).Append('(');
                    sb.Append(ArgAsString(p.Other));
                    sb.Append(')');
                }
                else
                {
                    sb.Append(p.OperatorName).Append('(');
                    sb.Append(ArgAsString(p.Value));
                    sb.Append(')');
                }
            }
            else if (p.OperatorName == "and" || p.OperatorName == "or")
            {
                // Connective P: Value is the left P, Other is the right P
                sb.Append(ArgAsString(p.Value));
                sb.Append('.').Append(p.OperatorName).Append('(');
                sb.Append(ArgAsString(p.Other));
                sb.Append(')');
            }
            else if (p.OperatorName == "not")
            {
                sb.Append("P.not(");
                sb.Append(ArgAsString(p.Value));
                sb.Append(')');
            }
            else
            {
                // Regular P: render without "P." prefix to match Java/Go/Python behavior
                sb.Append(p.OperatorName).Append('(');

                // For predicates like between/inside, the value is an object[] with 2 elements
                if (p.Value is object[] valArr)
                {
                    if (valArr.Length > 1 && p.OperatorName != "between" && p.OperatorName != "inside")
                    {
                        sb.Append('[');
                        for (int i = 0; i < valArr.Length; i++)
                        {
                            if (i > 0) sb.Append(',');
                            sb.Append(ArgAsString(valArr[i]));
                        }
                        sb.Append(']');
                    }
                    else
                    {
                        for (int i = 0; i < valArr.Length; i++)
                        {
                            if (i > 0) sb.Append(',');
                            sb.Append(ArgAsString(valArr[i]));
                        }
                    }
                }
                else if (p.Value is IList listVal && (p.OperatorName == "within" || p.OperatorName == "without"))
                {
                    // within/without with a list value - render as [e1,e2,...]
                    if (listVal.Count == 0)
                    {
                        // empty within() - no brackets
                    }
                    else
                    {
                        sb.Append('[');
                        for (int i = 0; i < listVal.Count; i++)
                        {
                            if (i > 0) sb.Append(',');
                            sb.Append(ArgAsString(listVal[i]));
                        }
                        sb.Append(']');
                    }
                }
                else
                {
                    sb.Append(ArgAsString(p.Value));
                }

                sb.Append(')');
            }

            return sb.ToString();
        }

        private string AsString(IDictionary dict)
        {
            var sb = new StringBuilder("[");
            int size = dict.Count;

            if (size == 0)
            {
                sb.Append(':');
            }
            else
            {
                foreach (DictionaryEntry entry in dict)
                {
                    var key = ArgAsString(entry.Key);
                    // special handling for enum keys
                    if (entry.Key is EnumWrapper && key.Contains("."))
                    {
                        key = $"({key})";
                    }
                    sb.Append(key).Append(':').Append(ArgAsString(entry.Value));
                    if (--size > 0)
                    {
                        sb.Append(',');
                    }
                }
            }

            sb.Append(']');
            return sb.ToString();
        }

        private string AsStringSet(ISet<object?> set)
        {
            var sb = new StringBuilder("{");
            var first = true;
            foreach (var item in set)
            {
                if (!first) sb.Append(',');
                sb.Append(ArgAsString(item));
                first = false;
            }
            sb.Append('}');
            return sb.ToString();
        }

        private string AsStringSetEnumerable(IEnumerable set)
        {
            var sb = new StringBuilder("{");
            var first = true;
            foreach (var item in set)
            {
                if (!first) sb.Append(',');
                sb.Append(ArgAsString(item));
                first = false;
            }
            sb.Append('}');
            return sb.ToString();
        }

        private string AsStringList(IEnumerable list)
        {
            var sb = new StringBuilder("[");
            var first = true;
            foreach (var item in list)
            {
                if (!first) sb.Append(',');
                sb.Append(ArgAsString(item));
                first = false;
            }
            sb.Append(']');
            return sb.ToString();
        }

        private string BuildStrategyArgs(object?[] arguments)
        {
            var sb = new StringBuilder();
            var count = 0;
            foreach (var arg in arguments)
            {
                if (arg is OptionsStrategy optionsStrategy)
                {
                    _optionsStrategies.Add(optionsStrategy);
                    continue;
                }

                if (count > 0)
                    sb.Append(',');

                if (arg is AbstractTraversalStrategy strategy)
                {
                    var strategyName = strategy.StrategyName;
                    var configuration = strategy.Configuration;

                    if (configuration.Count == 0)
                    {
                        sb.Append(strategyName);
                    }
                    else
                    {
                        sb.Append("new ").Append(strategyName).Append('(');
                        var first = true;
                        foreach (var kvp in configuration)
                        {
                            if (kvp.Key == "strategy")
                                continue;

                            if (!first)
                                sb.Append(',');

                            sb.Append(kvp.Key).Append(':');

                            if (kvp.Value is ITraversal traversalVal)
                            {
                                sb.Append(ArgAsString(traversalVal.GremlinLang));
                            }
                            else
                            {
                                sb.Append(ArgAsString(kvp.Value));
                            }
                            first = false;
                        }
                        sb.Append(')');
                    }
                    count++;
                }
            }

            return sb.ToString();
        }

        private object?[] FlattenArguments(object?[] arguments)
        {
            if (arguments == null || arguments.Length == 0)
                return EmptyArray;

            var flatArguments = new List<object?>(arguments.Length);
            foreach (var arg in arguments)
            {
                if (arg is object[] objects)
                {
                    foreach (var nestObject in objects)
                    {
                        flatArguments.Add(ConvertArgument(nestObject));
                    }
                }
                else
                {
                    flatArguments.Add(ConvertArgument(arg));
                }
            }
            return flatArguments.ToArray();
        }

        private object? ConvertArgument(object? argument)
        {
            if (argument == null)
                return null;

            if (argument is ITraversal traversal)
            {
                if (!traversal.IsAnonymous)
                {
                    throw new InvalidOperationException(
                        $"The child traversal of {traversal} was not spawned anonymously - use the __ class rather than a TraversalSource to construct the child traversal");
                }
                return traversal.GremlinLang;
            }

            if (IsDictionaryType(argument.GetType()))
            {
                var dict = new Dictionary<object, object?>();
                foreach (DictionaryEntry item in (IDictionary)argument)
                {
                    dict[ConvertArgument(item.Key)!] = ConvertArgument(item.Value);
                }
                return dict;
            }

            if (IsListType(argument.GetType()))
            {
                var list = new List<object?>(((IList)argument).Count);
                foreach (var item in (IList)argument)
                {
                    list.Add(ConvertArgument(item));
                }
                return list;
            }

            if (IsHashSetType(argument.GetType()))
            {
                var set = new HashSet<object?>();
                foreach (var item in (IEnumerable)argument)
                {
                    set.Add(ConvertArgument(item));
                }
                return set;
            }

            return argument;
        }

        private static bool IsDictionaryType(Type type)
        {
            return type.IsConstructedGenericType && type.GetGenericTypeDefinition() == typeof(Dictionary<,>);
        }

        private static bool IsListType(Type type)
        {
            return type.IsConstructedGenericType && type.GetGenericTypeDefinition() == typeof(List<>);
        }

        private static bool IsHashSetType(Type type)
        {
            return type.IsConstructedGenericType && type.GetGenericTypeDefinition() == typeof(HashSet<>);
        }

        private static string EscapeJava(string value)
        {
            var sb = new StringBuilder(value.Length);
            foreach (var c in value)
            {
                switch (c)
                {
                    case '\\':
                        sb.Append("\\\\");
                        break;
                    case '"':
                        sb.Append("\\\"");
                        break;
                    case '\n':
                        sb.Append("\\n");
                        break;
                    case '\r':
                        sb.Append("\\r");
                        break;
                    case '\t':
                        sb.Append("\\t");
                        break;
                    case '\b':
                        sb.Append("\\b");
                        break;
                    case '\f':
                        sb.Append("\\f");
                        break;
                    default:
                        sb.Append(c);
                        break;
                }
            }
            return sb.ToString();
        }

        /// <summary>
        ///     Ensures a floating-point string representation always contains a decimal point,
        ///     matching Java's Double/Float toString() behavior (e.g., "5.0" not "5").
        /// </summary>
        private static string FormatFloatingPoint(string value)
        {
            if (value.IndexOf('.') < 0 && value.IndexOf('E') < 0 && value.IndexOf('e') < 0)
            {
                return value + ".0";
            }
            return value;
        }

        private static bool IsValidIdentifier(string name)
        {
            if (string.IsNullOrEmpty(name))
                return false;
            if (!char.IsLetter(name[0]))
                return false;
            for (int i = 1; i < name.Length; i++)
            {
                if (!char.IsLetterOrDigit(name[i]))
                    return false;
            }
            return true;
        }

        /// <summary>
        ///     Creates a deep copy of this <see cref="GremlinLang" /> instance.
        /// </summary>
        /// <returns>A new <see cref="GremlinLang" /> with copied state.</returns>
        public GremlinLang Clone()
        {
            var clone = new GremlinLang
            {
                _gremlin = new StringBuilder(_gremlin.ToString()),
                _parameters = new Dictionary<string, object>(_parameters),
                _optionsStrategies = new List<OptionsStrategy>(_optionsStrategies)
            };
            return clone;
        }

        /// <inheritdoc />
        object ICloneable.Clone() => Clone();

        /// <inheritdoc />
        public bool Equals(GremlinLang? other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return _gremlin.ToString() == other._gremlin.ToString() &&
                   DictionaryEquals(_parameters, other._parameters);
        }

        /// <inheritdoc />
        public override bool Equals(object? obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;
            return Equals((GremlinLang)obj);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked
            {
                var hash = _gremlin.ToString().GetHashCode() * 397;
                foreach (var kvp in _parameters)
                {
                    hash ^= (kvp.Key.GetHashCode() * 31) + (kvp.Value?.GetHashCode() ?? 0);
                }
                return hash;
            }
        }

        /// <inheritdoc />
        public override string ToString()
        {
            return _gremlin.ToString();
        }

        private static bool DictionaryEquals(Dictionary<string, object> a, Dictionary<string, object> b)
        {
            if (a.Count != b.Count) return false;
            foreach (var kvp in a)
            {
                if (!b.TryGetValue(kvp.Key, out var value)) return false;
                if (!Equals(kvp.Value, value)) return false;
            }
            return true;
        }
    }
}
