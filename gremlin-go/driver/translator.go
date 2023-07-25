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
)

type Translator interface {
	GetTraversalSource() *string
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

func (t *translator) GetTraversalSource() *string {
	return &t.traversalSource
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
				case int:
					{
						script += fmt.Sprintf("%d", param)
					}
				case interface{}:
					{
						switch param := param.(type) {
						case instruction:
							{
								subT := NewTranslator("")
								script += subT.Translate(&Bytecode{
									stepInstructions: []instruction{
										param,
									},
								})
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
