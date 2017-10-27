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
using System.Collections.Generic;
using System.Reflection;
using System.Text.RegularExpressions;
using Gremlin.Net.Process.Traversal;

namespace Gremlin.Net.IntegrationTest.Gherkin.TraversalEvaluation
{
    public class TraversalParser
    {
        private static readonly IDictionary<string, Func<GraphTraversalSource, ITraversal>> FixedTranslations = 
            new Dictionary<string, Func<GraphTraversalSource, ITraversal>>
            {
                { "g.V().fold().count(Scope.local)", g => g.V().Fold<object>().Count(Scope.Local)}
            };
        
        private static readonly Regex RegexInteger = new Regex(@"\d+", RegexOptions.Compiled);
        private static readonly Regex RegexDouble = new Regex(@"\d+\.\d+", RegexOptions.Compiled);
        private static readonly Regex RegexFloat =
            new Regex(@"\d+\.\d+f", RegexOptions.Compiled | RegexOptions.IgnoreCase);
        private static readonly Regex RegexLong = new Regex(@"\d+l", RegexOptions.Compiled | RegexOptions.IgnoreCase);

        internal static ITraversal GetTraversal(string traversalText, GraphTraversalSource g)
        {
            Func<GraphTraversalSource, ITraversal> traversalBuilder;
            if (!FixedTranslations.TryGetValue(traversalText, out traversalBuilder))
            {
                return BuildFromMethods(traversalText, g);
            }
            return traversalBuilder(g);
        }

        private static ITraversal BuildFromMethods(string traversalText, GraphTraversalSource g)
        {
            var parts = ParseTraversal(traversalText);
            if (parts[0].Name != "g")
            {
                throw BuildException(traversalText);
            }
            ITraversal traversal;
            switch (parts[1].Name)
            {
                case "V":
                    //TODO: support V() parameters
                    traversal = g.V();
                    break;
                case "E":
                    traversal = g.E();
                    break;
                default:
                    throw BuildException(traversalText);
            }
            for (var i = 2; i < parts.Count; i++)
            {
                var token = parts[i];
                var name = GetCsharpName(token.Name);
                var method = traversal.GetType().GetMethod(name);
                if (method == null)
                {
                    throw new InvalidOperationException($"Traversal method '{parts[i]}' not found for testing");
                }
                var parameterValues = BuildParameters(method, token, out var genericParameters);
                method = BuildGenericMethod(method, genericParameters, parameterValues);
                traversal = (ITraversal) method.Invoke(traversal, parameterValues);
            }
            return traversal;
        }

        private static MethodInfo BuildGenericMethod(MethodInfo method, IDictionary<string, Type> genericParameters,
                                                     object[] parameterValues)
        {
            if (!method.IsGenericMethod)
            {
                return method;
            }
            var genericArgs = method.GetGenericArguments();
            var types = new Type[genericArgs.Length];
            for (var i = 0; i < genericArgs.Length; i++)
            {
                var name = genericArgs[i].Name;
                Type type;
                if (!genericParameters.TryGetValue(name, out type))
                {
                    // Try to infer it from the name based on modern graph
                    type = ModernGraphTypeInformation.GetTypeArguments(method, parameterValues);
                }
                if (type == null)
                {
                    throw new InvalidOperationException(
                        $"Can not build traversal to test as '{method.Name}()' method is generic and type '{name}'" +
                        $" can not be inferred");
                }
                types[i] = type;
            }
            return method.MakeGenericMethod(types);
        }

        private static object[] BuildParameters(MethodInfo method, Token token,
                                                out IDictionary<string, Type> genericParameterTypes)
        {
            var paramsInfo = method.GetParameters();
            var parameters = new object[paramsInfo.Length];
            genericParameterTypes = new Dictionary<string, Type>();
            for (var i = 0; i < paramsInfo.Length; i++)
            {
                var info = paramsInfo[i];
                object value = null;
                if (token.Parameters.Count > i)
                {
                    var tokenParameter = token.Parameters[i];
                    value =  tokenParameter.GetValue();
                    if (info.ParameterType.IsGenericParameter)
                    {
                        // We've provided a value for parameter of a generic type, we can infer the
                        // type of the generic argument based on the parameter.
                        // For example, in the case of `Constant<E2>(E2 value)`
                        // if we have the type of value we have the type of E2. 
                        genericParameterTypes.Add(info.ParameterType.Name, tokenParameter.GetParameterType());
                    }
                    else if (info.ParameterType == typeof(object[]))
                    {
                        value = new [] {value};
                    }
                }
                parameters[i] = value ?? GetDefault(info.ParameterType);
            }
            return parameters;
        }

        public static object GetDefault(Type type)
        {
            return type.GetTypeInfo().IsValueType ? Activator.CreateInstance(type) : null;
        }

        private static string GetCsharpName(string part)
        {
            // Transform to PascalCasing and remove the parenthesis
            return char.ToUpper(part[0]) + part.Substring(1);
        }

        private static Exception BuildException(string traversalText)
        {
            return new InvalidOperationException($"Can not build a traversal to test from '{traversalText}'");
        }

        internal static IList<Token> ParseTraversal(string traversalText)
        {
            var index = 0;
            return ParseTokens(traversalText, ref index);
        }

        private static IList<Token> ParseTokens(string text, ref int i)
        {
            var result = new List<Token>();
            var startIndex = i;
            var parsing = ParsingPart.Name;
            string name = null;
            var parameters = new List<ITokenParameter>();
            while (i < text.Length)
            {
                switch (text[i])
                {
                    case '.':
                        if (name == null)
                        {
                            name = text.Substring(startIndex, i - startIndex);
                        }
                        startIndex = i + 1;
                        result.Add(new Token(name, parameters));
                        name = null;
                        parameters = new List<ITokenParameter>();
                        parsing = ParsingPart.Name;
                        break;
                    case '(':
                    {
                        name = text.Substring(startIndex, i - startIndex);
                        i++;
                        parsing = ParsingPart.StartParameters;
                        var param = ParseParameter(text, ref i);
                        if (param == null)
                        {
                            parsing = ParsingPart.EndParameters;
                        }
                        else
                        {
                            parameters.Add(param);
                        }
                        break;
                    }
                    case ',' when text[i+1] != ' ':
                    case ' ' when text[i+1] != ' ':
                    {
                        if (parsing != ParsingPart.StartParameters)
                        {
                            throw new InvalidOperationException(
                                "Can not parse space or comma chars outside parameters");
                        }
                        i++;
                        var param = ParseParameter(text, ref i);
                        if (param == null)
                        {
                            parsing = ParsingPart.EndParameters;
                        }
                        else
                        {
                            parameters.Add(param);
                        }
                        break;
                    }
                    case ')' when parsing != ParsingPart.StartParameters:
                        // The traversal already ended
                        i--;
                        if (name != null)
                        {
                            result.Add(new Token(name, parameters));
                        }
                        return result;
                    case ')':
                        parsing = ParsingPart.EndParameters;
                        break;
                }
                i++;
            }
            if (name != null)
            {
                result.Add(new Token(name, parameters));
            }
            return result;
        }

        private static ITokenParameter ParseParameter(string text, ref int i)
        {
            var firstChar = text[i];
            if (firstChar == ')')
            {
                return null;
            }
            if (firstChar == '"')
            {
                return StringParameter.Parse(text, ref i);
            }
            if (char.IsDigit(firstChar))
            {
                return ParseNumber(text, ref i);
            }
            if (text.Substring(i, 3).StartsWith("__."))
            {
                return new StaticTraversalParameter(ParseTokens(text, ref i));
            }
            if (text.Substring(i, 2).StartsWith("T."))
            {
                return new TraversalTokenParameter(ParseTokens(text, ref i));
            }
            if (text.Substring(i, 2).StartsWith("P."))
            {
                return new TraversalPredicateParameter(ParseTokens(text, ref i));
            }
            return null;
        }
        
        private static ITokenParameter ParseNumber(string text, ref int i)
        {
            var match = RegexLong.Match(text.Substring(i));
            if (match.Success)
            {
                i += match.Value.Length - 1;
                return NumericParameter.Create(Convert.ToInt64(match.Value.Substring(0, match.Value.Length-1)));
            }
            match = RegexFloat.Match(text.Substring(i));
            if (match.Success)
            {
                i += match.Value.Length - 1;
                return NumericParameter.Create(Convert.ToSingle(match.Value.Substring(0, match.Value.Length-1)));
            }
            match = RegexDouble.Match(text.Substring(i));
            if (match.Success)
            {
                i += match.Value.Length;
                return NumericParameter.Create(Convert.ToSingle(match.Value));
            }
            match = RegexInteger.Match(text.Substring(i));
            if (!match.Success)
            {
                throw new InvalidOperationException(
                    $"Could not parse numeric value from the beginning of {text.Substring(i)}");
            }
            i += match.Value.Length;
            return NumericParameter.Create(Convert.ToInt32(match.Value));
        }
        
        private enum ParsingPart
        {
            Name,
            StartParameters,
            EndParameters
        }
    }
}