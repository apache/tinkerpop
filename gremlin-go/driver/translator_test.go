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

import "testing"

func Test_translator_Translate(t *testing.T) {
	type fields struct {
		traversalSource string
	}
	type args struct {
		bytecode *Bytecode
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   string
	}{
		{
			name:   "Should translate simple traversal",
			fields: fields{traversalSource: "g"},
			args: args{
				bytecode: &Bytecode{
					stepInstructions: []instruction{
						{
							operator:  "V",
							arguments: []interface{}{"1"},
						},
					},
				},
			},
			want: "g.V(\"1\")",
		},
		{
			name:   "Should translate traversal with multiple steps",
			fields: fields{traversalSource: "g"},
			args: args{
				bytecode: &Bytecode{
					stepInstructions: []instruction{
						{
							operator:  "V",
							arguments: []interface{}{"1"},
						},
						{
							operator:  "out",
							arguments: []interface{}{},
						},
					},
				},
			},
			want: "g.V(\"1\").out()",
		},
		{
			name:   "Should translate traversal with multiple steps and arguments",
			fields: fields{traversalSource: "g"},
			args: args{
				bytecode: &Bytecode{
					stepInstructions: []instruction{
						{
							operator:  "V",
							arguments: []interface{}{"1"},
						},
						{
							operator:  "out",
							arguments: []interface{}{"knows"},
						},
					},
				},
			},
			want: "g.V(\"1\").out(\"knows\")",
		},
		{
			name:   "Should translate traversal with multiple steps and multiple arguments",
			fields: fields{traversalSource: "g"},
			args: args{
				bytecode: &Bytecode{
					stepInstructions: []instruction{
						{
							operator:  "V",
							arguments: []interface{}{"1"},
						},
						{
							operator:  "out",
							arguments: []interface{}{"knows", "created"},
						},
					},
				},
			},
			want: "g.V(\"1\").out(\"knows\", \"created\")",
		},
		{
			name:   "Should translate traversal with multiple steps and multiple arguments of different types",
			fields: fields{traversalSource: "g"},
			args: args{
				bytecode: &Bytecode{
					stepInstructions: []instruction{
						{
							operator:  "V",
							arguments: []interface{}{"1"},
						},
						{
							operator:  "out",
							arguments: []interface{}{"knows", 1},
						},
					},
				},
			},
			want: "g.V(\"1\").out(\"knows\", 1)",
		},
		{
			name:   "Should translate traversal with multiple steps and multiple arguments of different types",
			fields: fields{traversalSource: "g"},
			args: args{
				bytecode: &Bytecode{
					stepInstructions: []instruction{
						{
							operator:  "V",
							arguments: []interface{}{"1"},
						},
						{
							operator:  "out",
							arguments: []interface{}{"knows", 1},
						},
						{
							operator:  "out",
							arguments: []interface{}{"created"},
						},
					},
				},
			},
			want: "g.V(\"1\").out(\"knows\", 1).out(\"created\")",
		},
		{
			name:   "Should translate traversal with multiple steps and multiple arguments of different types",
			fields: fields{traversalSource: "g"},
			args: args{
				bytecode: &Bytecode{
					stepInstructions: []instruction{
						{
							operator:  "V",
							arguments: []interface{}{"1"},
						},
						{
							operator:  "out",
							arguments: []interface{}{"knows", 1},
						},
						{
							operator:  "out",
							arguments: []interface{}{"created"},
						},
						{
							operator:  "out",
							arguments: []interface{}{},
						},
					},
				},
			},
			want: "g.V(\"1\").out(\"knows\", 1).out(\"created\").out()",
		},
		{
			name:   "Should translate traversal with multiple different steps and multiple arguments of different types",
			fields: fields{traversalSource: "g"},
			args: args{
				bytecode: &Bytecode{
					stepInstructions: []instruction{
						{
							operator:  "V",
							arguments: []interface{}{"1"},
						},
						{
							operator:  "inE",
							arguments: []interface{}{"knows", 1},
						},
						{
							operator:  "outV",
							arguments: []interface{}{"created"},
						},
						{
							operator:  "hasLabel",
							arguments: []interface{}{"person"},
						},
						{
							operator: "where",
							arguments: []interface{}{
								instruction{
									operator:  "eq",
									arguments: []interface{}{"name", "marko"},
								},
							},
						},
					},
				},
			},
			want: "g.V(\"1\").inE(\"knows\", 1).outV(\"created\").hasLabel(\"person\").where(eq(\"name\", \"marko\"))",
		},
		{
			name:   "Should translate traversal with multiple different steps and multiple arguments of different types",
			fields: fields{traversalSource: "g"},
			args: args{
				bytecode: &Bytecode{
					stepInstructions: []instruction{
						{
							operator: "V",
							arguments: []interface{}{
								"1",
								"2",
								"3",
							},
						},
						{
							operator:  "inE",
							arguments: []interface{}{"knows", 1},
						},
						{
							operator:  "outV",
							arguments: []interface{}{"created"},
						},
						{
							operator:  "hasLabel",
							arguments: []interface{}{"person"},
						},
						{
							operator: "where",
							arguments: []interface{}{
								instruction{
									operator: "or",
									arguments: []interface{}{
										instruction{
											operator:  "has",
											arguments: []interface{}{"name"},
										},
										instruction{
											operator:  "has",
											arguments: []interface{}{"age"},
										},
									},
								},
							},
						},
					},
				},
			},
			want: "g.V(\"1\", \"2\", \"3\").inE(\"knows\", 1).outV(\"created\").hasLabel(\"person\").where(or(has(\"name\"), has(\"age\")))",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := &translator{
				traversalSource: tt.fields.traversalSource,
			}
			if got := tr.Translate(tt.args.bytecode); got != tt.want {
				t.Errorf("translator.Translate() = %v, want %v", got, tt.want)
			}
		})
	}
}
