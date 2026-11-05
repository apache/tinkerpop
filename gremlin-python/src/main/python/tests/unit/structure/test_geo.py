"""
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
"""

import io
import math
import struct
import pytest

from gremlin_python.structure.geo import Curve, Geometry, GeometryCollection, LineString, Model, \
    MultiCurve, MultiLineString, MultiPoint, MultiPolygon, MultiSurface, Point, Polygon, Surface


def ring(coordinates, model=Model.CARTESIAN):
    return LineString([Point(x, y, model=model) for x, y in coordinates])


def nested_collection(depth):
    """Returns a collection holding a point, wrapped in a further collection depth - 1 times."""
    geometry = GeometryCollection([Point(1, 2)])
    for _ in range(depth - 1):
        geometry = GeometryCollection([geometry])
    return geometry


def wkb_header(order, type_code):
    """Returns a WKB geometry header: the byte order marker, then the type code in that order."""
    marker = b'\x00' if order == '>' else b'\x01'
    return marker + struct.pack(order + 'I', type_code)


class OneByteStream(object):
    """A binary stream that hands back at most one byte per read, however much is asked for."""

    def __init__(self, data):
        self._data = data
        self._position = 0

    def read(self, size):
        chunk = self._data[self._position:self._position + min(size, 1)]
        self._position += len(chunk)
        return chunk


# POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (2 2, 2 4, 4 4, 2 2)) under GEOGRAPHIC
POLYGON_WITH_HOLE = Polygon(
    ring([(0, 0), (10, 0), (10, 10), (0, 10), (0, 0)], Model.GEOGRAPHIC),
    [ring([(2, 2), (2, 4), (4, 4), (2, 2)], Model.GEOGRAPHIC)])

# GEOMETRYCOLLECTION(POINT(1 2), GEOMETRYCOLLECTION(LINESTRING(0 0, 1 1)), POINT EMPTY)
NESTED_COLLECTION = GeometryCollection([
    Point(1, 2),
    GeometryCollection([ring([(0, 0), (1, 1)])]),
    Point.empty()])

# POLYGON((10 20, 30 20, 30 40, 10 40, 10 20)) under GEOGRAPHIC
POSTGIS_POLYGON = Polygon(
    ring([(10, 20), (30, 20), (30, 40), (10, 40), (10, 20)], Model.GEOGRAPHIC))


class TestGeometry(object):

    def test_abstract_types_cannot_be_instantiated(self):
        for geometry_type in [Geometry, Curve, Surface, MultiCurve, MultiSurface]:
            with pytest.raises(TypeError, match='abstract'):
                geometry_type()

    def test_point_holds_its_coordinates(self):
        point = Point(-122.3321, 47.6062, model=Model.GEOGRAPHIC)
        assert point.x == -122.3321
        assert point.y == 47.6062
        assert point.z is None
        assert point.has_z is False
        assert point.is_empty is False
        assert point.srid == 4326
        assert point.model == Model.GEOGRAPHIC

    def test_point_with_z(self):
        point = Point(1.5, 2.5, 3.5)
        assert point.z == 3.5
        assert point.has_z is True
        assert point.srid == 0
        assert point.model == Model.CARTESIAN

    def test_empty_point(self):
        point = Point.empty()
        assert point.is_empty is True
        assert point.has_z is False
        with pytest.raises(ValueError):
            point.x
        with pytest.raises(ValueError):
            point.y
        with pytest.raises(ValueError):
            point.z

    def test_point_rejects_nan_and_infinite_coordinates(self):
        for x in [math.nan, math.inf, -math.inf]:
            with pytest.raises(ValueError):
                Point(x, 2)
            with pytest.raises(ValueError):
                Point(1, x)
            with pytest.raises(ValueError):
                Point(1, 2, x)

    def test_geographic_coordinates_must_be_in_range(self):
        assert Point(180, -90, model=Model.GEOGRAPHIC).to_wkt() == 'POINT(180 -90)'
        assert Point(-180, 90, model=Model.GEOGRAPHIC).to_wkt() == 'POINT(-180 90)'
        with pytest.raises(ValueError, match='longitude'):
            Point(180.5, 0, model=Model.GEOGRAPHIC)
        with pytest.raises(ValueError, match='latitude'):
            Point(0, -90.5, model=Model.GEOGRAPHIC)

    def test_other_srid_reports_the_other_model(self):
        point = Point(1, 2, srid=3857)
        assert point.srid == 3857
        assert point.model == Model.OTHER

    def test_constructors_reject_the_other_model(self):
        with pytest.raises(ValueError, match=r'Model\.OTHER'):
            Point(1, 2, model=Model.OTHER)
        with pytest.raises(ValueError, match=r'Model\.OTHER'):
            Point.empty(Model.OTHER)
        with pytest.raises(ValueError, match=r'Model\.OTHER'):
            LineString.empty(Model.OTHER)
        with pytest.raises(ValueError, match=r'Model\.OTHER'):
            GeometryCollection.empty(Model.OTHER)

    def test_line_string_holds_no_points_or_at_least_two(self):
        assert LineString.empty().is_empty is True
        assert LineString.empty().points == ()
        with pytest.raises(ValueError, match='at least 2 points'):
            LineString([Point(1, 2)])
        with pytest.raises(ValueError, match='at least 2 points'):
            LineString([])
        assert len(LineString([Point(0, 0), Point(1, 1)]).points) == 2

    def test_line_string_vertices_cannot_be_empty_points(self):
        with pytest.raises(ValueError, match='empty point'):
            LineString([Point(0, 0), Point.empty()])

    def test_parts_must_be_a_collection_of_the_right_type(self):
        with pytest.raises(ValueError, match='LineString points cannot be None'):
            LineString(None)
        with pytest.raises(ValueError, match='MultiPoint points cannot be None'):
            MultiPoint(None)
        with pytest.raises(ValueError, match='element at index 1 cannot be None'):
            LineString([Point(0, 0), None])
        with pytest.raises(ValueError, match='element at index 1 must be a Point, got: str'):
            LineString([Point(0, 0), 'x'])
        with pytest.raises(ValueError,
                           match='element at index 1 must be a Point, got: LineString'):
            MultiPoint([Point(1, 2), LineString([Point(0, 0), Point(1, 1)])])

    def test_parts_must_share_a_z_dimension(self):
        with pytest.raises(ValueError, match='Mixed z dimensions'):
            LineString([Point(0, 0), Point(1, 1, 1)])
        with pytest.raises(ValueError, match='Mixed z dimensions'):
            MultiPoint([Point(0, 0), Point(1, 1, 1)])
        with pytest.raises(ValueError, match='Mixed z dimensions'):
            GeometryCollection([Point(0, 0), Point(1, 1, 1)])
        # a 2D multi-point header whose only part carries a z dimension
        mixed = wkb_header('>', 4) + struct.pack('>I', 1) \
            + wkb_header('>', 1001) + struct.pack('>ddd', 1.0, 2.0, 3.0)
        with pytest.raises(ValueError, match='Mixed Z dimensions in WKB'):
            Geometry.from_wkb(mixed, 0)

    def test_parts_must_share_an_srid(self):
        geographic = Point(1, 2, model=Model.GEOGRAPHIC)
        with pytest.raises(ValueError, match='Mixed SRIDs'):
            MultiPoint([Point(1, 2), geographic])
        with pytest.raises(ValueError, match='Mixed SRIDs'):
            GeometryCollection([Point(1, 2), geographic])
        with pytest.raises(ValueError, match='Mixed SRIDs'):
            MultiLineString([ring([(0, 0), (1, 1)]),
                             ring([(0, 0), (1, 1)], Model.GEOGRAPHIC)])

    def test_polygon_ring_rules(self):
        closed = ring([(0, 0), (1, 0), (1, 1), (0, 0)])
        assert Polygon(closed).exterior_ring == closed
        assert Polygon(closed).interior_rings == ()
        with pytest.raises(ValueError, match='at least 4 points'):
            Polygon(ring([(0, 0), (1, 0), (0, 0)]))
        with pytest.raises(ValueError, match='closed'):
            Polygon(ring([(0, 0), (1, 0), (1, 1), (0, 1)]))
        with pytest.raises(ValueError, match='Hole ring at index 0 cannot be empty'):
            Polygon(closed, [LineString.empty()])
        with pytest.raises(ValueError, match='empty shell'):
            Polygon(LineString.empty(), [closed])
        assert Polygon.empty().is_empty is True
        assert Polygon.empty().exterior_ring.is_empty is True
        # the reader applies the same minimum to a ring it reads from WKB
        short_ring = wkb_header('>', 3) + struct.pack('>I', 1) + struct.pack('>I', 3) \
            + struct.pack('>dddddd', 0.0, 0.0, 1.0, 0.0, 0.0, 0.0)
        with pytest.raises(ValueError, match='Polygon ring must have at least 4 points, got 3'):
            Geometry.from_wkb(short_ring, 0)

    def test_parts_accessors_hand_out_an_immutable_sequence(self):
        points = [Point(1, 2), Point(3, 4)]
        multi_point = MultiPoint(points)
        assert multi_point.geometries == tuple(points)
        with pytest.raises(TypeError):
            multi_point.geometries[0] = Point(5, 6)
        line_string = LineString(points)
        assert line_string.points == tuple(points)
        with pytest.raises(TypeError):
            line_string.points[0] = Point(5, 6)

    def test_collection_nesting_is_limited(self):
        assert nested_collection(32).is_empty is False
        with pytest.raises(ValueError, match='nesting depth'):
            nested_collection(33)
        # the reader refuses a deeply nested collection as it reads the headers
        collection_header = wkb_header('>', 7) + struct.pack('>I', 1)
        deeply_nested = collection_header * 5000 + wkb_header('>', 1) \
            + struct.pack('>dd', 1.0, 2.0)
        with pytest.raises(ValueError, match='nesting depth'):
            Geometry.from_wkb(deeply_nested, 0)

    def test_equal_values_are_equal_and_hash_alike(self):
        for left, right in [(Point(1, 2), Point(1, 2)),
                            (Point(1, 2, 3), Point(1, 2, 3)),
                            (LineString([Point(0, 0), Point(1, 1)]),
                             LineString([Point(0, 0), Point(1, 1)])),
                            (POLYGON_WITH_HOLE, Polygon(
                                ring([(0, 0), (10, 0), (10, 10), (0, 10), (0, 0)], Model.GEOGRAPHIC),
                                [ring([(2, 2), (2, 4), (4, 4), (2, 2)], Model.GEOGRAPHIC)])),
                            (MultiPoint([Point(1, 2)]), MultiPoint([Point(1, 2)])),
                            (NESTED_COLLECTION, GeometryCollection([
                                Point(1, 2),
                                GeometryCollection([ring([(0, 0), (1, 1)])]),
                                Point.empty()]))]:
            assert left == right
            assert hash(left) == hash(right)

    def test_negative_zero_equals_zero(self):
        assert Point(-0.0, 0.0) == Point(0.0, 0.0)
        assert hash(Point(-0.0, 0.0)) == hash(Point(0.0, 0.0))
        assert Point(1, 2, -0.0) == Point(1, 2, 0.0)

    def test_empty_points_of_one_srid_are_equal(self):
        assert Point.empty() == Point.empty()
        assert hash(Point.empty()) == hash(Point.empty())
        assert Point.empty(Model.GEOGRAPHIC) == Point.empty(Model.GEOGRAPHIC)
        assert Point.empty() != Point.empty(Model.GEOGRAPHIC)

    def test_values_that_differ_are_not_equal(self):
        assert Point(1, 2) != Point(1, 2, model=Model.GEOGRAPHIC)
        assert Point(1, 2) != Point(1, 2, 0)
        assert Point(1, 2) != Point(2, 1)
        assert Point(1, 2) != 'POINT(1 2)'
        # a multi-point and a collection holding the same point are different kinds
        assert MultiPoint([Point(1, 2)]) != GeometryCollection([Point(1, 2)])
        assert LineString([Point(0, 0), Point(1, 1)]) != \
               MultiPoint([Point(0, 0), Point(1, 1)])
        # an empty value keeps its z dimension, which sets it apart from an empty 2D value
        empty_point_with_z = Geometry.from_wkb(
            wkb_header('>', 1001) + struct.pack('>ddd', math.nan, math.nan, math.nan), 0)
        assert Point.empty() != empty_point_with_z
        empty_line_string_with_z = Geometry.from_wkb(
            wkb_header('>', 1002) + struct.pack('>I', 0), 0)
        assert empty_line_string_with_z.has_z is True
        assert LineString.empty() != empty_line_string_with_z

    def test_string_form_is_the_wkt(self):
        assert str(Point(-122.3321, 47.6062, model=Model.GEOGRAPHIC)) == 'POINT(-122.3321 47.6062)'
        assert str(Point(1, 2, 3)) == 'POINT Z (1 2 3)'
        assert str(Point.empty()) == 'POINT EMPTY'
        assert str(POLYGON_WITH_HOLE) == 'POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (2 2, 2 4, 4 4, 2 2))'
        assert str(NESTED_COLLECTION) == \
               'GEOMETRYCOLLECTION(POINT(1 2), GEOMETRYCOLLECTION(LINESTRING(0 0, 1 1)), POINT EMPTY)'
        assert Point(1, 2).to_ewkt() == 'SRID=0;POINT(1 2)'
        assert Point(1, 2, srid=3857).to_ewkt() == 'SRID=3857;POINT(1 2)'

    def test_empty_parts_are_written_as_the_empty_keyword(self):
        assert str(MultiPoint([Point.empty(), Point(1, 2)])) == 'MULTIPOINT(EMPTY, (1 2))'
        assert str(MultiLineString([LineString.empty()])) == 'MULTILINESTRING(EMPTY)'
        assert str(MultiPolygon([Polygon.empty()])) == 'MULTIPOLYGON(EMPTY)'
        assert str(GeometryCollection.empty()) == 'GEOMETRYCOLLECTION EMPTY'

    def test_wkt_writes_the_shortest_plain_decimal(self):
        expected = {
            0.1: '0.1',
            1e15: '1000000000000000',
            1e16: '10000000000000000',
            1e21: '1000000000000000000000',
            1e-5: '0.00001',
            1e-6: '0.000001',
            1e-7: '0.0000001',
            -0.0: '-0'
        }
        for value, text in expected.items():
            assert str(Point(value, 0)) == f'POINT({text} 0)'

    def test_wkb_output_is_big_endian_iso_wkb(self):
        point = Point(-122.3321, 47.6062, model=Model.GEOGRAPHIC)
        assert point.to_wkb() == bytes.fromhex('0000000001c05e9541205bc01a4047cd97f62b6ae8')

    def test_assigning_a_coordinate_fails(self):
        point = Point(1, 2)
        with pytest.raises(AttributeError):
            point.x = 5
        with pytest.raises(AttributeError):
            point.srid = 4326

    def test_geometries_can_be_set_members_and_dict_keys(self):
        points = {Point(1, 2), Point(1, 2), Point(3, 4)}
        assert len(points) == 2
        by_point = {Point(1, 2): 'first'}
        by_point[Point(1, 2)] = 'second'
        assert by_point == {Point(1, 2): 'second'}

    def test_srid_must_fit_in_a_signed_32_bit_integer(self):
        assert Point(1, 2, srid=2 ** 31 - 1).srid == 2 ** 31 - 1
        assert Point(1, 2, srid=-2 ** 31).srid == -2 ** 31
        with pytest.raises(ValueError, match='32 bit'):
            Point(1, 2, srid=2 ** 31)
        with pytest.raises(ValueError, match='32 bit'):
            Point(1, 2, srid=-2 ** 31 - 1)

    def test_a_model_and_an_srid_cannot_both_be_given(self):
        with pytest.raises(ValueError, match='not both'):
            Point(1, 2, model=Model.GEOGRAPHIC, srid=4326)

    def test_integer_coordinates_are_held_as_floats(self):
        point = Point(1, 2)
        assert point == Point(1.0, 2.0)
        assert isinstance(point.x, float)
        assert str(point) == 'POINT(1 2)'

    def test_a_bool_is_not_a_coordinate(self):
        with pytest.raises(ValueError, match='int or a float'):
            Point(True, 2)
        with pytest.raises(ValueError, match='int or a float'):
            Point(1, 2, True)
        with pytest.raises(ValueError, match='must be an int'):
            Point(1, 2, srid=True)

    def test_from_wkb_reads_one_whole_geometry(self):
        point = Point(1, 2, srid=3857)
        assert Geometry.from_wkb(point.to_wkb(), 3857) == point
        with pytest.raises(ValueError, match='None or empty'):
            Geometry.from_wkb(b'', 0)
        with pytest.raises(ValueError, match='trailing bytes'):
            Geometry.from_wkb(point.to_wkb() + b'\xff', 3857)

    def test_each_nested_geometry_carries_its_own_byte_order(self):
        expected = MultiPoint([Point(1, 2), Point(3, 4)])
        for header_order, part_order in [('<', '>'), ('>', '<')]:
            wkb = wkb_header(header_order, 4) + struct.pack(header_order + 'I', 2)
            for x, y in [(1.0, 2.0), (3.0, 4.0)]:
                wkb += wkb_header(part_order, 1) + struct.pack(part_order + 'dd', x, y)
            assert Geometry.from_wkb(wkb, 0) == expected
        # only 0x00 and 0x01 name a byte order
        with pytest.raises(ValueError, match='Invalid WKB byte order: 2'):
            Geometry.from_wkb(b'\x02' + struct.pack('>I', 1) + struct.pack('>dd', 1.0, 2.0), 0)

    def test_wkb_rejects_an_unknown_type_code(self):
        # 1000 is the Z offset on its own, with no kind added to it
        with pytest.raises(ValueError, match='Unknown WKB type code: 1000'):
            Geometry.from_wkb(wkb_header('>', 1000) + struct.pack('>ddd', 1.0, 2.0, 3.0), 0)
        with pytest.raises(ValueError, match='Unknown WKB type code: 8'):
            Geometry.from_wkb(wkb_header('>', 8) + struct.pack('>dd', 1.0, 2.0), 0)

    def test_wkb_empty_point_needs_nan_in_both_ordinates(self):
        empty_with_z = wkb_header('>', 1001) + struct.pack('>ddd', math.nan, math.nan, math.nan)
        assert len(empty_with_z) == 29

        # the z ordinate of an empty 3D point is read and discarded, so nothing is left over
        read = Geometry.from_wkb(empty_with_z, 0)
        assert read.is_empty is True
        assert read.has_z is True
        # a single NaN ordinate encodes no empty point, so the coordinate checks apply
        with pytest.raises(ValueError, match='x coordinate cannot be NaN'):
            Geometry.from_wkb(wkb_header('>', 1) + struct.pack('>dd', math.nan, 2.0), 0)
        with pytest.raises(ValueError, match='y coordinate cannot be NaN'):
            Geometry.from_wkb(wkb_header('>', 1) + struct.pack('>dd', 1.0, math.nan), 0)

    def test_wkb_parts_must_be_of_the_kind_the_collection_holds(self):
        # a multi-point whose only part is a line string
        wkb = wkb_header('>', 4) + struct.pack('>I', 1) \
            + wkb_header('>', 2) + struct.pack('>I', 2) \
            + struct.pack('>dddd', 0.0, 0.0, 1.0, 1.0)
        assert len(wkb) == 50
        with pytest.raises(ValueError, match='Expected Point in MultiPoint, got LineString'):
            Geometry.from_wkb(wkb, 0)

        # a multi-line string whose only part is a point
        wkb = wkb_header('>', 5) + struct.pack('>I', 1) \
            + wkb_header('>', 1) + struct.pack('>dd', 1.0, 2.0)
        assert len(wkb) == 30
        with pytest.raises(ValueError, match='Expected LineString in MultiLineString, got Point'):
            Geometry.from_wkb(wkb, 0)

        # a multi-polygon whose only part is a line string
        wkb = wkb_header('>', 6) + struct.pack('>I', 1) \
            + wkb_header('>', 2) + struct.pack('>I', 2) \
            + struct.pack('>dddd', 0.0, 0.0, 1.0, 1.0)
        assert len(wkb) == 50
        with pytest.raises(ValueError, match='Expected Polygon in MultiPolygon, got LineString'):
            Geometry.from_wkb(wkb, 0)

    def test_read_wkb_requires_a_stream_and_allows_short_reads(self):
        with pytest.raises(ValueError, match='WKB input cannot be None'):
            Geometry.read_wkb(None, 0)
        point = Point(1, 2, 3)
        # a stream may hand back less than was asked for while more is still to come
        assert Geometry.read_wkb(OneByteStream(point.to_wkb()), 0) == point

    def test_read_wkb_leaves_the_stream_after_the_geometry(self):
        point = Point(1, 2)
        empty_with_z = wkb_header('>', 1001) + struct.pack('>ddd', math.nan, math.nan, math.nan)
        line_string = LineString([Point(0, 0), Point(1, 1)])
        stream = io.BytesIO(point.to_wkb() + empty_with_z + line_string.to_wkb() + b'\xff')
        assert Geometry.read_wkb(stream, 0) == point
        read = Geometry.read_wkb(stream, 0)
        assert read.is_empty is True
        assert read.has_z is True
        assert Geometry.read_wkb(stream, 0) == line_string
        assert stream.read(1) == b'\xff'

    def test_reads_the_wkb_postgis_writes_by_default(self):
        # the little-endian ISO WKB PostGIS writes for ST_AsBinary by default
        as_binary = bytes.fromhex(
            '01' + '03000000' + '01000000' + '05000000'
            + '0000000000002440' + '0000000000003440'
            + '0000000000003e40' + '0000000000003440'
            + '0000000000003e40' + '0000000000004440'
            + '0000000000002440' + '0000000000004440'
            + '0000000000002440' + '0000000000003440')
        assert len(as_binary) == 93

        read = Geometry.from_wkb(as_binary, 4326)
        assert read == POSTGIS_POLYGON
        assert read.srid == 4326
        assert read.model == Model.GEOGRAPHIC

    def test_wkb_matches_the_form_postgis_writes_for_xdr(self):
        # the big-endian ISO WKB PostGIS writes for ST_AsBinary with XDR
        xdr = bytes.fromhex(
            '00' + '00000003' + '00000001' + '00000005'
            + '4024000000000000' + '4034000000000000'
            + '403e000000000000' + '4034000000000000'
            + '403e000000000000' + '4044000000000000'
            + '4024000000000000' + '4044000000000000'
            + '4024000000000000' + '4034000000000000')
        assert len(xdr) == 93

        assert POSTGIS_POLYGON.to_wkb() == xdr
        assert Geometry.from_wkb(xdr, 4326) == POSTGIS_POLYGON

    def test_rejects_the_ewkb_postgis_writes(self):
        # the little-endian EWKB with SRID 4326 PostGIS writes for ST_AsEWKB
        ewkb = bytes.fromhex(
            '01' + '03000020' + 'e6100000' + '01000000' + '05000000'
            + '0000000000002440' + '0000000000003440'
            + '0000000000003e40' + '0000000000003440'
            + '0000000000003e40' + '0000000000004440'
            + '0000000000002440' + '0000000000004440'
            + '0000000000002440' + '0000000000003440')
        assert len(ewkb) == 97

        with pytest.raises(ValueError, match='EWKB format detected'):
            Geometry.from_wkb(ewkb, 4326)

        # the M flag alone marks EWKB, as it does in the POINTM PostGIS writes
        point_m = wkb_header('>', 0x40000001) + struct.pack('>ddd', 10.0, 20.0, 30.0)
        assert len(point_m) == 29

        with pytest.raises(ValueError,
                           match=r'EWKB format detected \(type code 0x40000001\)'):
            Geometry.from_wkb(point_m, 4326)
