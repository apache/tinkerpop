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
using System.Globalization;
using System.Linq;
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
                { "g.V().fold().count(Scope.local)", g => g.V().Fold().Count(Scope.Local)}
            };

        private static readonly Regex RegexNumeric =
            new Regex(@"\d+(\.\d+)?(?:l|f)?", RegexOptions.Compiled | RegexOptions.IgnoreCase);

        private static readonly Regex RegexEnum = new Regex(@"\w+\.\w+", RegexOptions.Compiled);

        private static readonly Regex RegexIO = new Regex(@"IO.\w+", RegexOptions.Compiled);

        private static readonly Regex RegexParam = new Regex(@"\w+", RegexOptions.Compiled);
        
        private static readonly HashSet<Type> NumericTypes = new HashSet<Type>
        {
            typeof(int), typeof(long), typeof(double), typeof(float), typeof(short), typeof(decimal), typeof(byte)
        };

        internal static ITraversal GetTraversal(string traversalText, GraphTraversalSource g,
                                                IDictionary<string, object> contextParameterValues)
        {
            if (!FixedTranslations.TryGetValue(traversalText, out var traversalBuilder))
            {
                var tokens = ParseTraversal(traversalText);
                return GetTraversalFromTokens(tokens, g, contextParameterValues, traversalText);
            }
            return traversalBuilder(g);
        }

        internal static ITraversal GetTraversalFromTokens(IList<Token> tokens, GraphTraversalSource g,
                                                          IDictionary<string, object> contextParameterValues,
                                                          string traversalText)
        {
            object instance;
            Type instanceType;
            if (tokens[0].Name == "g")
            {
                instance = g;
                instanceType = g.GetType();
            }
            else if (tokens[0].Name == "__")
            {
                instance = null;
                instanceType = typeof(__);
            }
            else
            {
                throw BuildException(traversalText);
            }
            for (var i = 1; i < tokens.Count; i++)
            {
                var token = tokens[i];
                token.SetContextParameterValues(contextParameterValues);
                var name = GetCsharpName(token.Name);
                var methods = instanceType.GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static)
                    .Where(m => m.Name == name).ToList();
                var method = GetClosestMethod(methods, token.Parameters);
                if (method == null)
                {
                    throw new InvalidOperationException($"Traversal method '{tokens[i].Name}' not found for testing");
                }
                var parameterValues = BuildParameters(method, token, out var genericParameters);
                method = BuildGenericMethod(method, genericParameters, parameterValues);
                instance = method.Invoke(instance, parameterValues);
                instanceType = instance.GetType();
            }
            return (ITraversal) instance;
        }

        /// <summary>
        /// Find the method that supports the amount of parameters provided
        /// </summary>
        private static MethodInfo GetClosestMethod(IList<MethodInfo> methods, IList<ITokenParameter> tokenParameters)
        {
            if (methods.Count == 0)
            {
                return null;
            }
            if (methods.Count == 1)
            {
                return methods[0];
            }
            var ordered = methods.OrderBy(m => m.GetParameters().Length);
            if (tokenParameters.Count == 0)
            {
                return ordered.First();
            }
            MethodInfo lastMethod = null;
            var compatibleMethods = new Dictionary<int, MethodInfo>();
            foreach (var method in ordered)
            {
                lastMethod = method;
                var methodParameters = method.GetParameters();
                var requiredParameters = methodParameters.Length;
                if (requiredParameters > 0 && IsParamsArray(methodParameters.Last()))
                {
                    // Params array can be not provided
                    requiredParameters--;
                }
                if (tokenParameters.Count < requiredParameters)
                {
                    continue;
                }
                var matched = true;
                var exactMatches = 0;
                for (var i = 0; i < tokenParameters.Count; i++)
                {
                    if (methodParameters.Length <= i)
                    {
                        // The method contains less parameters (and no params array) than provided
                        matched = false;
                        break;
                    }
                    var methodParameter = methodParameters[i];
                    var tokenParameterType = tokenParameters[i].GetParameterType();
                    // Match either the same parameter type
                    matched = methodParameter.ParameterType == tokenParameterType;
                    if (matched)
                    {
                        exactMatches++;
                    }
                    else if (IsParamsArray(methodParameter))
                    {
                        matched = methodParameter.ParameterType == typeof(object[]) ||
                                  methodParameter.ParameterType.GetElementType() == tokenParameterType;
                        // The method has params array, no further parameters are going to be defined
                        break;
                    }
                    else
                    {
                        if (IsNumeric(methodParameter.ParameterType) && IsNumeric(tokenParameterType))
                        {
                            // Acount for implicit conversion of numeric values as an exact match 
                            exactMatches++;
                        }
                        else if (!methodParameter.ParameterType.GetTypeInfo().IsAssignableFrom(tokenParameterType))
                        {
                            // Not a match
                            break;
                        }
                        // Is assignable to the parameter type
                        matched = true;
                    }
                }
                if (matched)
                {
                    compatibleMethods[exactMatches] = method;
                }
            }
            // Attempt to use the method with the higher number of matches or the last one
            return compatibleMethods.OrderByDescending(kv => kv.Key).Select(kv => kv.Value).FirstOrDefault() ??
                   lastMethod;
        }

        private static bool IsNumeric(Type t) => NumericTypes.Contains(t);

        private static bool IsParamsArray(ParameterInfo methodParameter)
        {
            return methodParameter.IsDefined(typeof(ParamArrayAttribute), false);
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
                    type = ModernGraphTypeInformation.GetTypeArguments(method, parameterValues, i);
                }
                if (type == null)
                {
                    throw new InvalidOperationException(
                        $"Can not build traversal to test as '{method.Name}()' method is generic and type '{name}'" +
                         " can not be inferred");
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
                    else if (IsParamsArray(info) && info.ParameterType.GetElementType().IsGenericParameter)
                    {
                        // Its a method where the type parameter comes from an params Array
                        // e.g., Inject<S>(params S[] value)
                        genericParameterTypes.Add(info.ParameterType.GetElementType().Name,
                            tokenParameter.GetParameterType());
                    }

                    if (info.ParameterType != tokenParameter.GetParameterType() && IsNumeric(info.ParameterType) &&
                        IsNumeric(tokenParameter.GetParameterType()))
                    {
                        // Numeric conversion
                        value = Convert.ChangeType(value, info.ParameterType);
                    }
                }

                if (IsParamsArray(info))
                {
                    // For `params type[] value` we should provide an empty array
                    if (value == null)
                    {
                        // An empty array
                        value = Array.CreateInstance(info.ParameterType.GetElementType(), 0);
                    }
                    else if (!value.GetType().IsArray)
                    {
                        // An array with the parameter values
                        // No more method parameters after this one
                        var elementType = info.ParameterType.GetElementType();

                        if (elementType.IsGenericParameter)
                        {
                            // The Array element type is generic, so we use type of the value to specify it
                            elementType = value.GetType();
                        }

                        var arr = Array.CreateInstance(elementType, token.Parameters.Count - i);
                        arr.SetValue(value, 0);
                        for (var j = 1; j < token.Parameters.Count - i; j++)
                        {
                            arr.SetValue(token.Parameters[i + j].GetValue(), j);
                        }
                        value = arr;
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

        internal static string GetCsharpName(string part)
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
            // Parser issue: quotes are not normalized
            text = text.Replace("\\\"", "\"");
            var result = new List<Token>();
            var startIndex = i;
            var parsing = ParsingPart.Name;
            var parameters = new List<ITokenParameter>();
            string name = null;
            while (i < text.Length)
            {
                switch (text[i])
                {
                    case '.':
                        if (parsing == ParsingPart.Name)
                        {
                            // The previous token was an object property, not a method
                            result.Add(new Token(text.Substring(startIndex, i - startIndex)));
                        }
                        startIndex = i + 1;
                        parameters = new List<ITokenParameter>();
                        parsing = ParsingPart.Name;
                        break;
                    case '(':
                    {
                        name = text.Substring(startIndex, i - startIndex);
                        parsing = ParsingPart.StartParameters;
                        // Start parsing from the next index
                        i++;
                        var param = ParseParameter(text, ref i);
                        if (param == null)
                        {
                            // The next character was a ')', empty params
                            // Evaluate the current position
                            continue;
                        }
                        parameters.Add(param);
                        break;
                    }
                    case ',' when parsing == ParsingPart.StartParameters && text.Length > i + 1 && text[i+1] != ' ':
                    case ' ' when parsing == ParsingPart.StartParameters && text.Length > i + 1 && text[i+1] != ' ' &&
                                  text[i+1] != ')':
                    {
                        i++;
                        var param = ParseParameter(text, ref i);
                        if (param == null)
                        {
                            // The next character was a ')', empty params
                            // Evaluate the current position
                            continue;
                        }
                        parameters.Add(param);
                        break;
                    }
                    case ',' when parsing != ParsingPart.StartParameters:
                    case ')' when parsing != ParsingPart.StartParameters:
                        // The current nested object already ended
                        if (parsing == ParsingPart.Name)
                        {
                            // The previous token was an object property, not a method and finished
                            result.Add(new Token(text.Substring(startIndex, i - startIndex)));
                        }
                        i--;
                        return result;
                    case ')':
                        parsing = ParsingPart.EndParameters;
                        result.Add(new Token(name, parameters));
                        break;
                }
                i++;
            }
            if (parsing == ParsingPart.Name)
            {
                // The previous token was an object property, not a method and finished
                result.Add(new Token(text.Substring(startIndex, i - startIndex)));
            }
            return result;
        }

        private static ITokenParameter ParseParameter(string text, ref int i)
        {
            var firstChar = text[i];
            while (char.IsWhiteSpace(firstChar))
            {
                firstChar = text[++i];
            }
            if (firstChar == ')')
            {
                return null;
            }
            if (firstChar == '"' || firstChar == '\'')
            {
                return StringParameter.Parse(text, firstChar, ref i);
            }
            if (char.IsDigit(firstChar))
            {
                return ParseNumber(text, ref i);
            }
            if (text.Length >= i + 3 && text.Substring(i, 3) == "__.")
            {
                var startIndex = i;
                var tokens = ParseTokens(text, ref i);
                return new StaticTraversalParameter(tokens, text.Substring(startIndex, i - startIndex));
            }
            if (text.Length >= i + 6 && text.Substring(i, 6) == "TextP.")
            {
                return new TextPParameter(ParseTokens(text, ref i));
            }
            if (text.Substring(i, 2).StartsWith("P."))
            {
                return new PParameter(ParseTokens(text, ref i));
            }
            var parameterText = text.Substring(i, text.IndexOf(')', i) - i);
            var separatorIndex = parameterText.IndexOf(',');
            if (separatorIndex >= 0)
            {
                parameterText = parameterText.Substring(0, separatorIndex);
            }
            parameterText = parameterText.Trim();
            if (parameterText == "")
            {
                return null;
            }
            if (parameterText == "true" || parameterText == "false")
            {
                i += parameterText.Length - 1;
                return LiteralParameter.Create(Convert.ToBoolean(parameterText));
            }
            if (RegexIO.IsMatch(parameterText))
            {
                i += parameterText.Length - 1;
                return new IOParameter(parameterText);
            }
            if (RegexEnum.IsMatch(parameterText))
            {
                i += parameterText.Length - 1;
                return new TraversalEnumParameter(parameterText);
            }
            if (RegexParam.IsMatch(parameterText))
            {
                i += parameterText.Length - 1;
                return new ContextBasedParameter(parameterText);
            }
            throw new NotSupportedException($"Parameter {parameterText} not supported");
        }
        
        private static ITokenParameter ParseNumber(string text, ref int i)
        {
            var match = RegexNumeric.Match(text, i);
            if (!match.Success)
            {
                throw new InvalidOperationException(
                    $"Could not parse numeric value from the beginning of {text.Substring(i)}");
            }
            var numericText = match.Value.ToUpper();
            i += match.Value.Length - 1;
            if (numericText.EndsWith("L"))
            {
                return LiteralParameter.Create(Convert.ToInt64(match.Value.Substring(0, match.Value.Length - 1)));
            }
            if (numericText.EndsWith("F"))
            {
                return LiteralParameter.Create(Convert.ToSingle(match.Value.Substring(0, match.Value.Length - 1),
                    CultureInfo.InvariantCulture));
            }
            if (match.Groups[1].Value != "")
            {
                // Captured text with the decimal separator
                return LiteralParameter.Create(Convert.ToDecimal(match.Value, CultureInfo.InvariantCulture));
            }
            return LiteralParameter.Create(Convert.ToInt32(match.Value));
        }
        
        private enum ParsingPart
        {
            Name,
            StartParameters,
            EndParameters
        }
    }
}
