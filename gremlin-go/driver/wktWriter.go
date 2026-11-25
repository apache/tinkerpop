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
	"strconv"
	"strings"
)

// String returns the ISO WKT text of this point.
func (p *Point) String() string {
	return geometryWkt(p)
}

// String returns the ISO WKT text of this line string.
func (ls *LineString) String() string {
	return geometryWkt(ls)
}

// String returns the ISO WKT text of this polygon.
func (poly *Polygon) String() string {
	return geometryWkt(poly)
}

// String returns the ISO WKT text of this multi-point.
func (mp *MultiPoint) String() string {
	return geometryWkt(mp)
}

// String returns the ISO WKT text of this multi-line string.
func (mls *MultiLineString) String() string {
	return geometryWkt(mls)
}

// String returns the ISO WKT text of this multi-polygon.
func (mpoly *MultiPolygon) String() string {
	return geometryWkt(mpoly)
}

// String returns the ISO WKT text of this collection.
func (gc *GeometryCollection) String() string {
	return geometryWkt(gc)
}

// WKT returns the ISO WKT text of this point, the same text as String.
func (p *Point) WKT() string {
	return geometryWkt(p)
}

// WKT returns the ISO WKT text of this line string, the same text as String.
func (ls *LineString) WKT() string {
	return geometryWkt(ls)
}

// WKT returns the ISO WKT text of this polygon, the same text as String.
func (poly *Polygon) WKT() string {
	return geometryWkt(poly)
}

// WKT returns the ISO WKT text of this multi-point, the same text as String.
func (mp *MultiPoint) WKT() string {
	return geometryWkt(mp)
}

// WKT returns the ISO WKT text of this multi-line string, the same text as String.
func (mls *MultiLineString) WKT() string {
	return geometryWkt(mls)
}

// WKT returns the ISO WKT text of this multi-polygon, the same text as String.
func (mpoly *MultiPolygon) WKT() string {
	return geometryWkt(mpoly)
}

// WKT returns the ISO WKT text of this collection, the same text as String.
func (gc *GeometryCollection) WKT() string {
	return geometryWkt(gc)
}

// EWKT returns the ISO WKT text of this point with its SRID prefix.
func (p *Point) EWKT() string {
	return geometryEwkt(p)
}

// EWKT returns the ISO WKT text of this line string with its SRID prefix.
func (ls *LineString) EWKT() string {
	return geometryEwkt(ls)
}

// EWKT returns the ISO WKT text of this polygon with its SRID prefix.
func (poly *Polygon) EWKT() string {
	return geometryEwkt(poly)
}

// EWKT returns the ISO WKT text of this multi-point with its SRID prefix.
func (mp *MultiPoint) EWKT() string {
	return geometryEwkt(mp)
}

// EWKT returns the ISO WKT text of this multi-line string with its SRID prefix.
func (mls *MultiLineString) EWKT() string {
	return geometryEwkt(mls)
}

// EWKT returns the ISO WKT text of this multi-polygon with its SRID prefix.
func (mpoly *MultiPolygon) EWKT() string {
	return geometryEwkt(mpoly)
}

// EWKT returns the ISO WKT text of this collection with its SRID prefix.
func (gc *GeometryCollection) EWKT() string {
	return geometryEwkt(gc)
}

func geometryWkt(g Geometry) string {
	sb := &strings.Builder{}
	writeWktTagged(sb, g)
	return sb.String()
}

// geometryEwkt prefixes the WKT text of a geometry with its SRID. The prefix is always written,
// so a geometry of SRID 0 carries "SRID=0;".
func geometryEwkt(g Geometry) string {
	sb := &strings.Builder{}
	sb.WriteString("SRID=")
	sb.WriteString(strconv.FormatInt(int64(g.Srid()), 10))
	sb.WriteByte(';')
	writeWktTagged(sb, g)
	return sb.String()
}

// writeWktTagged appends a geometry with its type name, its Z tag and either the EMPTY keyword or
// a parenthesized body. The choice is structural, so WKT and WKB describe the same parts.
func writeWktTagged(sb *strings.Builder, g Geometry) {
	sb.WriteString(g.wktTypeName())
	if g.HasZ() {
		sb.WriteString(" Z")
	}
	if g.hasNoParts() {
		sb.WriteString(" EMPTY")
		return
	}
	if g.HasZ() {
		sb.WriteByte(' ')
	}
	sb.WriteByte('(')
	g.writeWktBody(sb)
	sb.WriteByte(')')
}

// formatWktCoordinate formats a coordinate as the shortest decimal that reads back as the same
// float64, in plain notation with no exponent. A negative zero keeps its sign, as "-0".
func formatWktCoordinate(value float64) string {
	return strconv.FormatFloat(value, 'f', -1, 64)
}

func (p *Point) wktTypeName() string {
	return "POINT"
}

func (p *Point) writeWktBody(sb *strings.Builder) {
	if p.empty {
		// An empty point has no coordinates; the writer emits the EMPTY keyword.
		return
	}
	sb.WriteString(formatWktCoordinate(p.x))
	sb.WriteByte(' ')
	sb.WriteString(formatWktCoordinate(p.y))
	if p.hasZ {
		sb.WriteByte(' ')
		sb.WriteString(formatWktCoordinate(p.z))
	}
}

func (ls *LineString) wktTypeName() string {
	return "LINESTRING"
}

func (ls *LineString) writeWktBody(sb *strings.Builder) {
	for i, p := range ls.points {
		if i > 0 {
			sb.WriteString(", ")
		}
		p.writeWktBody(sb)
	}
}

func (poly *Polygon) wktTypeName() string {
	return "POLYGON"
}

func (poly *Polygon) writeWktBody(sb *strings.Builder) {
	if poly.hasNoParts() {
		return
	}
	sb.WriteByte('(')
	poly.shellRing().writeWktBody(sb)
	sb.WriteByte(')')
	for _, hole := range poly.holes {
		sb.WriteString(", (")
		hole.writeWktBody(sb)
		sb.WriteByte(')')
	}
}

func (mp *MultiPoint) wktTypeName() string {
	return "MULTIPOINT"
}

func (mp *MultiPoint) writeWktBody(sb *strings.Builder) {
	for i, p := range mp.points {
		if i > 0 {
			sb.WriteString(", ")
		}
		writeWktPart(sb, p)
	}
}

func (mls *MultiLineString) wktTypeName() string {
	return "MULTILINESTRING"
}

func (mls *MultiLineString) writeWktBody(sb *strings.Builder) {
	for i, ls := range mls.lineStrings {
		if i > 0 {
			sb.WriteString(", ")
		}
		writeWktPart(sb, ls)
	}
}

func (mpoly *MultiPolygon) wktTypeName() string {
	return "MULTIPOLYGON"
}

func (mpoly *MultiPolygon) writeWktBody(sb *strings.Builder) {
	for i, poly := range mpoly.polygons {
		if i > 0 {
			sb.WriteString(", ")
		}
		writeWktPart(sb, poly)
	}
}

func (gc *GeometryCollection) wktTypeName() string {
	return "GEOMETRYCOLLECTION"
}

func (gc *GeometryCollection) writeWktBody(sb *strings.Builder) {
	for i, g := range gc.geometries {
		if i > 0 {
			sb.WriteString(", ")
		}
		writeWktTagged(sb, g)
	}
}

// writeWktPart appends one part of a multi-kind, which carries no type name of its own: either
// the EMPTY keyword or a parenthesized body.
func writeWktPart(sb *strings.Builder, g Geometry) {
	if g.hasNoParts() {
		sb.WriteString("EMPTY")
		return
	}
	sb.WriteByte('(')
	g.writeWktBody(sb)
	sb.WriteByte(')')
}
