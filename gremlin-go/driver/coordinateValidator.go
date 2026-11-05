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
	"math"
)

func validateX(x float64, srid int32) error {
	if math.IsNaN(x) || math.IsInf(x, 0) {
		return errors.New("x coordinate cannot be NaN or infinite")
	}
	if srid == sridGeographic && (x < -180.0 || x > 180.0) {
		return fmt.Errorf("GEOGRAPHIC longitude must be in [-180, 180], got: %v", x)
	}
	return nil
}

func validateY(y float64, srid int32) error {
	if math.IsNaN(y) || math.IsInf(y, 0) {
		return errors.New("y coordinate cannot be NaN or infinite")
	}
	if srid == sridGeographic && (y < -90.0 || y > 90.0) {
		return fmt.Errorf("GEOGRAPHIC latitude must be in [-90, 90], got: %v", y)
	}
	return nil
}

func validateZ(z float64) error {
	if math.IsNaN(z) {
		return errors.New("z coordinate cannot be NaN when provided;" +
			" use a 2D constructor for a 2D point")
	}
	if math.IsInf(z, 0) {
		return errors.New("z coordinate cannot be infinite")
	}
	return nil
}

func validateSameSrid(expected, actual int32, context string) error {
	if expected != actual {
		return fmt.Errorf("mixed SRIDs in %s: expected %d, got %d", context, expected, actual)
	}
	return nil
}

func validateSameHasZ(expected, actual bool, context string) error {
	if expected != actual {
		return fmt.Errorf("mixed z dimensions in %s: some coordinates have z, some do not", context)
	}
	return nil
}

// validateRing checks that a polygon ring is closed and holds at least 4 points. An empty ring is
// only valid as the whole polygon being empty, which the polygon constructors check.
func validateRing(ring *LineString, context string) error {
	if ring.hasNoParts() {
		return nil
	}
	if len(ring.points) < 4 {
		return fmt.Errorf("%s must have at least 4 points, got: %d", context, len(ring.points))
	}
	if !ring.points[0].Equal(ring.points[len(ring.points)-1]) {
		return fmt.Errorf("%s must be closed (first and last point must be equal)", context)
	}
	return nil
}

// validateVertex checks that a point can be a vertex of a line string or a polygon ring, which
// takes a real coordinate, so an empty point is rejected.
func validateVertex(point *Point, context string, index int) error {
	if point == nil {
		return nilPartError(context, index)
	}
	if point.empty {
		return fmt.Errorf("%s vertex at index %d cannot be an empty point;"+
			" a vertex must be a real coordinate", context, index)
	}
	return nil
}

// validateHoles checks the hole rings of a polygon against its shell.
func validateHoles(shell *LineString, holes []*LineString, srid int32, hasZ bool) error {
	for i, hole := range holes {
		if hole == nil {
			return nilPartError("polygon holes", i)
		}
		if hole.IsEmpty() {
			return fmt.Errorf("hole ring at index %d cannot be empty", i)
		}
		if shell.IsEmpty() {
			return errors.New("a polygon with an empty shell cannot have holes")
		}
		if err := validateSameSrid(srid, hole.srid, "polygon"); err != nil {
			return err
		}
		if err := validateSameHasZ(hasZ, hole.hasZ, "polygon"); err != nil {
			return err
		}
		if err := validateRing(hole, "hole ring"); err != nil {
			return err
		}
	}
	return nil
}

func validateNestingDepth(depth int) error {
	if depth > maxGeometryNestingDepth {
		return fmt.Errorf("collection nesting depth exceeds maximum of %d", maxGeometryNestingDepth)
	}
	return nil
}

// collectionDepth returns the collection nesting depth of a geometry. Every collection kind
// counts as one level plus the depth of its deepest part, and every other kind has depth 0.
func collectionDepth(g Geometry) int {
	switch v := g.(type) {
	case *GeometryCollection:
		maxChildDepth := 0
		for _, child := range v.geometries {
			if depth := collectionDepth(child); depth > maxChildDepth {
				maxChildDepth = depth
			}
		}
		return 1 + maxChildDepth
	case *MultiPoint, *MultiLineString, *MultiPolygon:
		// The parts of these kinds are never collections themselves.
		return 1
	default:
		return 0
	}
}
