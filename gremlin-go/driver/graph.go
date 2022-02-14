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
	"errors"
	"fmt"
	"strings"
)

type Graph struct {
}

// Element is the base structure for both Vertex and Edge.
// The inherited identifier must be unique to the inheriting classes.
type Element struct {
	id    interface{}
	label string
}

// Vertex contains a single vertex which has a label and an id.
type Vertex struct {
	Element
}

// Edge links two Vertex structs along with its Property objects. An edge has both a direction and a label.
type Edge struct {
	Element
	outV Vertex
	inV  Vertex
}

// VertexProperty is similar to propery in that it denotes a key/value pair associated with a Vertex, but is different
// in that it also represents an entity that is an Element and can have properties of its own.
type VertexProperty struct {
	Element
	key    string // This is the label of vertex.
	value  interface{}
	vertex Vertex // Vertex that owns the property.
}

// Property denotes a key/value pair associated with an Edge. A property can be empty.
type Property struct {
	key     string
	value   interface{}
	element Element
}

// Path denotes a particular walk through a Graph as defined by a traversal.
// A list of labels and a list of objects is maintained in the path.
// The list of labels are the labels of the steps traversed, and the objects are the objects that are traversed.
// TODO: change labels to be []<set of string> after implementing set in AN-1022 and update the GetPathObject accordingly
type Path struct {
	labels  [][]string
	objects []interface{}
}

func (v *Vertex) String() string {
	return fmt.Sprintf("v[%s]", v.id)
}

func (e *Edge) String() string {
	return fmt.Sprintf("e[%s][%s-%s->%s]", e.id, e.outV.id, e.label, e.inV.id)
}

func (vp *VertexProperty) String() string {
	return fmt.Sprintf("vp[%s->%v]", vp.label, vp.value)
}

func (p *Property) String() string {
	return fmt.Sprintf("p[%s->%v]", p.key, p.value)
}

func (p *Path) String() string {
	return fmt.Sprintf("path[%s]", strings.Trim(strings.Join(strings.Fields(fmt.Sprint(p.objects)), ", "), "[]"))
}

// GetPathObject returns the value that corresponds to the key for the Path and error if the value is not present or cannot be retrieved.
func (p *Path) GetPathObject(key string) (interface{}, error) {
	if len(p.objects) != len(p.labels) {
		return nil, errors.New("Path is invalid because it does not contain an equal number of labels and objects.")
	}
	var objectList []interface{}
	var object interface{}
	for i := 0; i < len(p.labels); i++ {
		for j := 0; j < len(p.labels[i]); j++ {
			if p.labels[i][j] == key {
				if object == nil {
					object = p.objects[i]
				} else if objectList != nil {
					objectList = append(objectList, p.objects[i])
				} else {
					objectList = []interface{}{object, p.objects[i]}
				}
			}
		}
	}
	if objectList != nil {
		return objectList, nil
	} else if object != nil {
		return object, nil
	} else {
		return nil, errors.New(fmt.Sprintf("Path does not contain a label of '%s'.", key))
	}
}
