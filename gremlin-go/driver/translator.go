/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/

package gremlingo

import (
	"fmt"
	"reflect"
)

type Translator interface {
	GetTraversalSource() string
	GetTargetLanguage() string
	Of(traversalSource string)
	Translate(bytecode *Bytecode) string
}

type translator struct {
	traversalSource string
}

func NewTranslator(traversalSource string) Translator {
	return &translator{
		traversalSource: traversalSource,
	}
}

func (t *translator) GetTargetLanguage() string {
	return "gremlin-groovy"
}

func (t *translator) GetTraversalSource() string {
	return t.traversalSource
}

func (t *translator) Of(traversalSource string) {
	t.traversalSource = traversalSource
}

func (t *translator) Translate(bytecode *Bytecode) string {
	script := t.traversalSource
	instructions := bytecode.stepInstructions

	for _, inst := range instructions {
		params := inst.arguments
		if t.traversalSource == "" {
		} else {
			script += "."
		}
		script += fmt.Sprintf("%s(", inst.operator)

		if len(params) > 0 {
			for i, param := range params {
				if i > 0 {
					script += ", "
				}

				switch param.(type) {
				case string:
					{
						script += fmt.Sprintf("\"%s\"", param)
					}
				case int, int32, int64:
					{
						script += fmt.Sprintf("%d", param)
					}
				case float32, float64:
					{
						script += fmt.Sprintf("%g", param)
					}
				case bool:
					{
						script += fmt.Sprintf("%t", param)
					}
				case []interface{}:
					{
						script += "["
						for j, p := range param.([]interface{}) {
							if j > 0 {
								script += ", "
							}
							switch p.(type) {
							case string:
								{
									script += fmt.Sprintf("\"%s\"", p)
								}
							case int, int32, int64:
								{
									script += fmt.Sprintf("%d", p)
								}
							case float32, float64:
								{
									script += fmt.Sprintf("%g", p)
								}
							case bool:
								{
									script += fmt.Sprintf("%t", p)
								}
							}
						}
						script += "]"
					}
				case interface{}:
					{
						switch param := param.(type) {
						case *Bytecode:
							{
								subT := NewTranslator("__")
								script += subT.Translate(param)

							}
						case instruction:
							{
								subT := NewTranslator("")
								script += subT.Translate(&Bytecode{
									stepInstructions: []instruction{
										param,
									},
								})
							}
						case Predicate:
							{
								v := reflect.ValueOf(param)
								if v.Kind() != reflect.Ptr {
									panic("Predicate must be a pointer to a struct")
								}

								v = v.Elem()
								if v.Kind() != reflect.Struct {
									panic("Predicate must be a pointer to a struct")
								}

								t := v.Type()
								for i := 0; i < t.NumField(); i++ {
									f := t.Field(i)

									if f.Name == "operator" {
										script += v.FieldByName(f.Name).String()
									}

									if f.Name == "values" {
										switch v.FieldByName(f.Name).Kind() {
										case reflect.Slice:
											{
												slice := v.FieldByName(f.Name).Slice(0, v.FieldByName(f.Name).Len())
												script += "("

												for j := 0; j < slice.Len(); j++ {
													if j > 0 {
														script += ", "
													}

													sliceItemType := slice.Index(j).Kind()
													if sliceItemType == reflect.Interface {
														switch slice.Index(j).Elem().Kind() {
														case reflect.String:
															{
																script += fmt.Sprintf("\"%s\"", slice.Index(j).Elem().String())
															}
														case reflect.Int, reflect.Int32, reflect.Int64:
															{
																script += fmt.Sprintf("%d", slice.Index(j).Elem().Int())
															}
														case reflect.Float32, reflect.Float64:
															{
																script += fmt.Sprintf("%g", slice.Index(j).Elem().Float())
															}
														case reflect.Bool:
															{
																script += fmt.Sprintf("%t", slice.Index(j).Elem().Bool())
															}
														}
													}

												}
												script += ")"

											}
										}
									}
								}

							}
						default:
							{
								script += fmt.Sprintf("%s", param)
							}
						}
					}
				}

			}

		}

		script += ")"
	}

	return script
}
