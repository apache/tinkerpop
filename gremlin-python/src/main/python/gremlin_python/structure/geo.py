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
import abc
import math
import struct
from decimal import Decimal

from aenum import Enum

__all__ = [
    'Model',
    'Geometry',
    'Curve',
    'Surface',
    'Point',
    'LineString',
    'Polygon',
    'GeometryCollection',
    'MultiCurve',
    'MultiSurface',
    'MultiPoint',
    'MultiLineString',
    'MultiPolygon'
]

SRID_CARTESIAN = 0
SRID_GEOGRAPHIC = 4326

# Maximum collection nesting depth, where every collection, including a MultiPoint,
# MultiLineString or MultiPolygon, counts as one level.
MAX_NESTING_DEPTH = 32

# Python's int is unbounded, so these bound an SRID to the signed 32-bit Int that GraphBinary
# carries, and a WKB point, ring or part count to the same range.
_INT32_MIN = -2 ** 31
_INT32_MAX = 2 ** 31 - 1

_WKB_POINT = 1
_WKB_LINESTRING = 2
_WKB_POLYGON = 3
_WKB_MULTIPOINT = 4
_WKB_MULTILINESTRING = 5
_WKB_MULTIPOLYGON = 6
_WKB_GEOMETRYCOLLECTION = 7

_EWKB_Z_FLAG = 0x80000000
_EWKB_M_FLAG = 0x40000000
_EWKB_SRID_FLAG = 0x20000000

_BIG_ENDIAN = 0x00
_LITTLE_ENDIAN = 0x01


class Model(Enum):
    """
    The coordinate interpretation model of a geometry.

    CARTESIAN is planar, with math in coordinate units, and is SRID 0. GEOGRAPHIC is
    longitude/latitude on the WGS84 earth surface, and is SRID 4326.

    OTHER is reported for any SRID other than 0 and 4326. Constructors don't accept it, since it
    doesn't say which SRID is meant, so give the SRID instead.
    """
    CARTESIAN = 'CARTESIAN'
    GEOGRAPHIC = 'GEOGRAPHIC'
    OTHER = 'OTHER'


def _model_for_srid(srid):
    if srid == SRID_CARTESIAN:
        return Model.CARTESIAN
    if srid == SRID_GEOGRAPHIC:
        return Model.GEOGRAPHIC
    return Model.OTHER


def _srid_for_model(model):
    if model is Model.CARTESIAN:
        return SRID_CARTESIAN
    if model is Model.GEOGRAPHIC:
        return SRID_GEOGRAPHIC
    if model is Model.OTHER:
        raise ValueError("Model.OTHER cannot be used in constructors; pass an explicit srid instead")
    raise ValueError(f"model must be Model.CARTESIAN or Model.GEOGRAPHIC, got: {model!r}")


def _check_srid(srid):
    if isinstance(srid, bool) or not isinstance(srid, int):
        raise ValueError(f"srid must be an int, got: {type(srid).__name__}")
    if srid < _INT32_MIN or srid > _INT32_MAX:
        raise ValueError(f"srid must fit in a signed 32 bit integer, got: {srid}")
    return srid


def _resolve_srid(model, srid):
    """Turns a model or an SRID into the SRID to hold. Neither given means CARTESIAN."""
    if model is not None and srid is not None:
        raise ValueError("Pass either a model or an srid, not both")
    if srid is not None:
        return _check_srid(srid)
    if model is None:
        return SRID_CARTESIAN
    return _srid_for_model(model)


def _coordinate(value, name):
    """Returns a coordinate as a float. bool is an int in Python, so it is rejected here."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} coordinate must be an int or a float, got: {type(value).__name__}")
    try:
        return float(value)
    except OverflowError:
        raise ValueError(f"{name} coordinate is too large to hold as a float: {value}") from None


def _check_x(x, srid):
    x = _coordinate(x, 'x')
    if math.isnan(x) or math.isinf(x):
        raise ValueError("x coordinate cannot be NaN or infinite")
    if srid == SRID_GEOGRAPHIC and not -180.0 <= x <= 180.0:
        raise ValueError(f"GEOGRAPHIC longitude must be in [-180, 180], got: {x}")
    return x


def _check_y(y, srid):
    y = _coordinate(y, 'y')
    if math.isnan(y) or math.isinf(y):
        raise ValueError("y coordinate cannot be NaN or infinite")
    if srid == SRID_GEOGRAPHIC and not -90.0 <= y <= 90.0:
        raise ValueError(f"GEOGRAPHIC latitude must be in [-90, 90], got: {y}")
    return y


def _check_z(z):
    z = _coordinate(z, 'z')
    if math.isnan(z):
        raise ValueError("z coordinate cannot be NaN when provided; leave z out for a 2D point")
    if math.isinf(z):
        raise ValueError("z coordinate cannot be infinite")
    return z


def _check_parts(parts, element_type, context):
    """Returns the parts as a tuple, after checking the collection and every element's type."""
    if parts is None:
        raise ValueError(f"{context} cannot be None")
    if isinstance(parts, Geometry):
        raise ValueError(f"{context} must be a collection of {element_type.__name__} values")
    try:
        items = tuple(parts)
    except TypeError:
        raise ValueError(f"{context} must be a collection of {element_type.__name__} values") from None
    for index, item in enumerate(items):
        if item is None:
            raise ValueError(f"{context} element at index {index} cannot be None")
        if not isinstance(item, element_type):
            raise ValueError(f"{context} element at index {index} must be a {element_type.__name__},"
                             f" got: {type(item).__name__}")
    return items


def _common_srid_and_z(items, context):
    """Returns the SRID and z dimension that every part must share."""
    srid = items[0].srid
    has_z = items[0].has_z
    for item in items[1:]:
        if item.srid != srid:
            raise ValueError(f"Mixed SRIDs in {context}: expected {srid}, got {item.srid}")
        if item.has_z != has_z:
            raise ValueError(f"Mixed z dimensions in {context}: some coordinates have z, some do not")
    return srid, has_z


def _check_vertex(point, context, index):
    """A vertex of a line string or a ring must be a real coordinate, so an empty point fails."""
    if point.is_empty:
        raise ValueError(f"{context} vertex at index {index} cannot be an empty point;"
                         f" a vertex must be a real coordinate")


def _check_ring(ring, context):
    """A ring that holds points must be closed and have at least 4 of them."""
    if ring.is_empty:
        return
    points = ring.points
    if len(points) < 4:
        raise ValueError(f"{context} must have at least 4 points, got: {len(points)}")
    if points[0] != points[-1]:
        raise ValueError(f"{context} must be closed (first and last point must be equal)")


def _depth_of(geometry):
    """Returns nesting depth: a collection is one level plus its deepest part, otherwise 0."""
    if isinstance(geometry, GeometryCollection):
        return 1 + max((_depth_of(part) for part in geometry.geometries), default=0)
    return 0


def _check_nesting_depth(depth):
    if depth > MAX_NESTING_DEPTH:
        raise ValueError(f"Collection nesting depth exceeds maximum of {MAX_NESTING_DEPTH}")


def _format_double(value):
    """
    Formats a coordinate as the shortest decimal that reads back as the same float, in plain
    notation with no exponent and no trailing ".0". Negative zero keeps its sign.
    """
    if math.isnan(value):
        raise ValueError("Cannot format NaN in WKT")
    if math.isinf(value):
        raise ValueError("Cannot format infinity in WKT")
    if value == 0.0:
        return '-0' if math.copysign(1.0, value) < 0 else '0'
    # repr gives the shortest digits that read back, but uses an exponent for some magnitudes
    text = repr(value)
    if 'e' in text or 'E' in text:
        text = format(Decimal(text), 'f')
    if '.' in text:
        text = text.rstrip('0').rstrip('.')
    return text


def _write_wkt_tagged(out, geometry):
    """Appends a geometry as its type name, Z tag and either EMPTY or a parenthesized body."""
    out.append(geometry._wkt_type_name())
    if geometry.has_z:
        out.append(' Z')
    if geometry._has_no_parts():
        out.append(' EMPTY')
    else:
        if geometry.has_z:
            out.append(' ')
        out.append('(')
        geometry._write_wkt_body(out)
        out.append(')')


class Geometry(object, metaclass=abc.ABCMeta):
    """
    Abstract base class of the geometry types. Geometry values are immutable.

    Two values are equal when they are of the same kind and carry the same SRID, z dimension and
    coordinates, where -0.0 equals 0.0. Geometry values define no local ordering, so ordering in a
    traversal uses Gremlin's order on the server.
    """

    __slots__ = ('_srid', '_has_z')

    def __init__(self, srid, has_z):
        self._srid = srid
        self._has_z = has_z

    @staticmethod
    def from_wkb(wkb, srid):
        """
        Parses ISO WKB bytes with an explicit SRID. EWKB, the M and ZM dimensions and unknown type
        codes are rejected.

        An empty point is encoded as a point whose x and y are both NaN. Such a point is accepted
        on its own and as an element of a MultiPoint, but not as a vertex of a LineString or a
        polygon ring.

        :param wkb: the ISO WKB bytes
        :param srid: the spatial reference ID, applied as given, range checked only against the
               span of a signed 32 bit integer
        :raises ValueError: if the bytes are malformed, truncated or followed by other bytes
        """
        if wkb is None or len(wkb) == 0:
            raise ValueError("WKB input cannot be None or empty")
        stream = _BytesReader(wkb)
        geometry = _WkbReader(stream, _check_srid(srid)).read_geometry()
        if stream.remaining() > 0:
            raise ValueError("Unexpected trailing bytes in WKB input")
        return geometry

    @staticmethod
    def read_wkb(stream, srid):
        """
        Parses one ISO WKB geometry from a binary stream, leaving the stream positioned on the byte
        after the geometry. Several geometries back to back are read by calling this method once
        each.

        :param stream: a binary stream, read through its read(size) method
        :param srid: the spatial reference ID, applied as given, range checked only against the
               span of a signed 32 bit integer
        :raises ValueError: if the bytes are malformed, or if the stream ends inside the geometry
        """
        if stream is None:
            raise ValueError("WKB input cannot be None")
        return _WkbReader(stream, _check_srid(srid)).read_geometry()

    @property
    def srid(self):
        """The spatial reference ID, 0 for CARTESIAN and 4326 for GEOGRAPHIC."""
        return self._srid

    @property
    def model(self):
        """The coordinate interpretation model derived from the SRID."""
        return _model_for_srid(self._srid)

    @property
    def has_z(self):
        """Whether this geometry carries z coordinates."""
        return self._has_z

    @property
    @abc.abstractmethod
    def is_empty(self):
        """Whether this geometry holds no coordinates. A collection is empty when all parts are."""

    def to_wkt(self):
        """Returns ISO WKT without an SRID prefix."""
        out = []
        _write_wkt_tagged(out, self)
        return ''.join(out)

    def to_ewkt(self):
        """Returns EWKT, the WKT with an "SRID=n;" prefix."""
        return f"SRID={self._srid};{self.to_wkt()}"

    def to_wkb(self):
        """Returns ISO WKB in big-endian byte order."""
        out = bytearray()
        self._write_wkb(out)
        return bytes(out)

    def __eq__(self, other):
        if self is other:
            return True
        if type(self) is not type(other):
            return NotImplemented
        return self._equality_key() == other._equality_key()

    def __hash__(self):
        return hash((type(self).__name__,) + self._equality_key())

    def __str__(self):
        return self.to_wkt()

    def __repr__(self):
        return f"geometry[{self._srid}]({self.to_wkt()})"

    def _equality_key(self):
        return (self._srid, self._has_z) + self._value_key()

    @abc.abstractmethod
    def _value_key(self):
        """The coordinates or parts this geometry is built from, compared after SRID and z."""

    @abc.abstractmethod
    def _wkb_type_code(self):
        """The ISO WKB type code of this kind, without the 1000 that marks a Z geometry."""

    @abc.abstractmethod
    def _wkt_type_name(self):
        """The WKT type name of this kind, such as POINT."""

    @abc.abstractmethod
    def _has_no_parts(self):
        """
        Whether this geometry holds no parts of its own, which is the test the writers use to
        choose between "X EMPTY" and a parenthesized body. It differs from is_empty, which is
        deep: a collection whose parts are all empty is empty but still has parts.
        """

    @abc.abstractmethod
    def _write_wkt_body(self, out):
        """Appends the coordinates or parts of this geometry to a list of strings."""

    def _write_wkb(self, out):
        """Appends this geometry, header and body, to a bytearray in big-endian ISO WKB."""
        out.append(_BIG_ENDIAN)
        type_code = self._wkb_type_code() + (1000 if self._has_z else 0)
        out += struct.pack('>i', type_code)
        self._write_wkb_body(out)

    @abc.abstractmethod
    def _write_wkb_body(self, out):
        """Appends the coordinates or parts of this geometry to a bytearray."""


class Curve(Geometry):
    """Abstract base class of the one-dimensional geometry types. LineString is the only kind."""

    __slots__ = ()


class Surface(Geometry):
    """Abstract base class of the two-dimensional geometry types. Polygon is the only kind."""

    __slots__ = ()


class Point(Geometry):
    """
    A single location in 2D or 3D space. A point built with Point.empty() holds no coordinate, so
    reading x, y or z from it fails. On a 2D point z is None.
    """

    __slots__ = ('_x', '_y', '_z')

    def __init__(self, x, y, z=None, *, model=None, srid=None):
        """
        Creates a point.

        :param x: the x coordinate, the longitude under GEOGRAPHIC
        :param y: the y coordinate, the latitude under GEOGRAPHIC
        :param z: the z coordinate, or None for a 2D point
        :param model: Model.CARTESIAN or Model.GEOGRAPHIC, CARTESIAN when neither is given
        :param srid: the spatial reference ID, as an alternative to a model
        :raises ValueError: if a coordinate is not a number, is NaN or is infinite, if a GEOGRAPHIC
                 x falls outside -180 to 180 or y outside -90 to 90, if the model is Model.OTHER,
                 or if the SRID does not fit in a signed 32 bit integer
        """
        srid = _resolve_srid(model, srid)
        super().__init__(srid, z is not None)
        self._x = _check_x(x, srid)
        self._y = _check_y(y, srid)
        self._z = None if z is None else _check_z(z)

    @classmethod
    def empty(cls, model=None, srid=None):
        """
        Creates an empty 2D point.

        :param model: Model.CARTESIAN or Model.GEOGRAPHIC, CARTESIAN when neither is given
        :param srid: the spatial reference ID, as an alternative to a model
        """
        return cls._empty_with_z(_resolve_srid(model, srid), False)

    @classmethod
    def _empty_with_z(cls, srid, has_z):
        point = cls.__new__(cls)
        Geometry.__init__(point, srid, has_z)
        point._x = None
        point._y = None
        point._z = None
        return point

    @property
    def x(self):
        """The x coordinate, the longitude under GEOGRAPHIC."""
        if self._x is None:
            raise ValueError("Cannot read x from an empty point")
        return self._x

    @property
    def y(self):
        """The y coordinate, the latitude under GEOGRAPHIC."""
        if self._x is None:
            raise ValueError("Cannot read y from an empty point")
        return self._y

    @property
    def z(self):
        """The z coordinate, or None on a 2D point."""
        if self._x is None:
            raise ValueError("Cannot read z from an empty point")
        return self._z

    @property
    def is_empty(self):
        return self._x is None

    def _value_key(self):
        return self._x, self._y, self._z

    def _wkb_type_code(self):
        return _WKB_POINT

    def _wkt_type_name(self):
        return 'POINT'

    def _has_no_parts(self):
        return self._x is None

    def _write_wkt_body(self, out):
        if self._x is None:
            return
        out.append(_format_double(self._x))
        out.append(' ')
        out.append(_format_double(self._y))
        if self._has_z:
            out.append(' ')
            out.append(_format_double(self._z))

    def _write_wkb_body(self, out):
        self._write_wkb_coordinates(out)

    def _write_wkb_coordinates(self, out):
        """Appends x and y, and z for a 3D point. An empty point is written as NaN coordinates."""
        if self._x is None:
            out += struct.pack('>d', math.nan)
            out += struct.pack('>d', math.nan)
            if self._has_z:
                out += struct.pack('>d', math.nan)
            return
        out += struct.pack('>d', self._x)
        out += struct.pack('>d', self._y)
        if self._has_z:
            out += struct.pack('>d', self._z)


class LineString(Curve):
    """
    A sequence of connected line segments. A line string holds either no points, which makes it
    empty, or at least 2 non-empty points sharing one SRID and z dimension.
    """

    __slots__ = ('_points',)

    def __init__(self, points):
        """
        Creates a line string from at least 2 points.

        :param points: at least 2 points, or use LineString.empty() for an empty value
        :raises ValueError: if fewer than 2 points are given, if any element is not a point or is
                 an empty point, or if the points do not all carry the same SRID and z dimension
        """
        items = _check_parts(points, Point, "LineString points")
        if len(items) == 0:
            raise ValueError("LineString must have at least 2 points, got 0;"
                             " use LineString.empty() for an empty line string")
        if len(items) == 1:
            raise ValueError("LineString must have at least 2 points, got 1")
        for index, point in enumerate(items):
            _check_vertex(point, "LineString", index)
        srid, has_z = _common_srid_and_z(items, "LineString")
        self._assign(items, srid, has_z)

    @classmethod
    def empty(cls, model=None, srid=None):
        """
        Creates an empty line string.

        :param model: Model.CARTESIAN or Model.GEOGRAPHIC, CARTESIAN when neither is given
        :param srid: the spatial reference ID, as an alternative to a model
        """
        return cls._empty_with_z(_resolve_srid(model, srid), False)

    @classmethod
    def _empty_with_z(cls, srid, has_z):
        line_string = cls.__new__(cls)
        line_string._assign((), srid, has_z)
        return line_string

    def _assign(self, points, srid, has_z):
        Geometry.__init__(self, srid, has_z)
        self._points = points

    @property
    def points(self):
        """The points as a tuple, empty when the line string is empty."""
        return self._points

    @property
    def is_empty(self):
        return len(self._points) == 0

    def _value_key(self):
        return (self._points,)

    def _wkb_type_code(self):
        return _WKB_LINESTRING

    def _wkt_type_name(self):
        return 'LINESTRING'

    def _has_no_parts(self):
        return len(self._points) == 0

    def _write_wkt_body(self, out):
        for index, point in enumerate(self._points):
            if index > 0:
                out.append(', ')
            point._write_wkt_body(out)

    def _write_wkb_body(self, out):
        out += struct.pack('>i', len(self._points))
        for point in self._points:
            point._write_wkb_coordinates(out)


class Polygon(Surface):
    """
    An area enclosed by a shell ring, with optional hole rings. Every ring that holds points must
    be closed and have at least 4 of them. A polygon with an empty shell has no holes.
    """

    __slots__ = ('_shell', '_holes')

    def __init__(self, shell, holes=()):
        """
        Creates a polygon from a shell ring and any hole rings.

        :param shell: the exterior ring
        :param holes: the interior rings
        :raises ValueError: if a ring that holds points has fewer than 4 points or is not closed,
                 if any hole is empty, if the shell is empty and a hole is given, or if the rings
                 do not all carry the same SRID and z dimension
        """
        if shell is None:
            raise ValueError("Shell ring cannot be None")
        if not isinstance(shell, LineString):
            raise ValueError(f"Shell ring must be a LineString, got: {type(shell).__name__}")
        _check_ring(shell, "Shell ring")
        hole_items = _check_parts(holes, LineString, "Polygon holes")
        for index, hole in enumerate(hole_items):
            if hole.is_empty:
                raise ValueError(f"Hole ring at index {index} cannot be empty")
            if shell.is_empty:
                raise ValueError("Polygon with an empty shell cannot have holes")
            _check_ring(hole, "Hole ring")
        srid, has_z = _common_srid_and_z((shell,) + hole_items, "Polygon")
        self._assign(shell, hole_items, srid, has_z)

    @classmethod
    def empty(cls, model=None, srid=None):
        """
        Creates an empty polygon.

        :param model: Model.CARTESIAN or Model.GEOGRAPHIC, CARTESIAN when neither is given
        :param srid: the spatial reference ID, as an alternative to a model
        """
        return cls._empty_with_z(_resolve_srid(model, srid), False)

    @classmethod
    def _empty_with_z(cls, srid, has_z):
        polygon = cls.__new__(cls)
        polygon._assign(LineString._empty_with_z(srid, has_z), (), srid, has_z)
        return polygon

    def _assign(self, shell, holes, srid, has_z):
        Geometry.__init__(self, srid, has_z)
        self._shell = shell
        self._holes = holes

    @property
    def exterior_ring(self):
        """The shell, an empty line string when the polygon is empty."""
        return self._shell

    @property
    def interior_rings(self):
        """The holes as a tuple, empty when there are none."""
        return self._holes

    @property
    def is_empty(self):
        return self._shell.is_empty

    def _value_key(self):
        return self._shell, self._holes

    def _wkb_type_code(self):
        return _WKB_POLYGON

    def _wkt_type_name(self):
        return 'POLYGON'

    def _has_no_parts(self):
        return self._shell._has_no_parts()

    def _ring_count(self):
        return 0 if self._shell._has_no_parts() else 1 + len(self._holes)

    def _write_wkt_body(self, out):
        if self._shell._has_no_parts():
            return
        out.append('(')
        self._shell._write_wkt_body(out)
        out.append(')')
        for hole in self._holes:
            out.append(', (')
            hole._write_wkt_body(out)
            out.append(')')

    def _write_wkb_body(self, out):
        ring_count = self._ring_count()
        out += struct.pack('>i', ring_count)
        if ring_count > 0:
            self._shell._write_wkb_body(out)
            for hole in self._holes:
                hole._write_wkb_body(out)


class GeometryCollection(Geometry):
    """
    A collection holding any mix of geometries, all carrying the same SRID and z dimension. A part
    may itself be a collection, within 32 levels of nesting, where each collection, including a
    multi kind, counts as one level.
    """

    __slots__ = ('_geometries',)

    def __init__(self, geometries):
        """
        Creates a collection from at least 1 geometry.

        :param geometries: at least 1 geometry, or use GeometryCollection.empty() for an empty
               value
        :raises ValueError: if no geometry is given, if any element is not a geometry, if the
                 geometries do not all carry the same SRID and z dimension, or if the nesting would
                 go deeper than 32 levels
        """
        items = _check_parts(geometries, Geometry, "GeometryCollection geometries")
        if len(items) == 0:
            raise ValueError("GeometryCollection must have at least 1 geometry;"
                             " use GeometryCollection.empty() for an empty geometry collection")
        srid, has_z = _common_srid_and_z(items, "GeometryCollection")
        _check_nesting_depth(1 + max((_depth_of(item) for item in items), default=0))
        self._assign(items, srid, has_z)

    @classmethod
    def empty(cls, model=None, srid=None):
        """
        Creates an empty collection.

        :param model: Model.CARTESIAN or Model.GEOGRAPHIC, CARTESIAN when neither is given
        :param srid: the spatial reference ID, as an alternative to a model
        """
        return cls._empty_with_z(_resolve_srid(model, srid), False)

    @classmethod
    def _empty_with_z(cls, srid, has_z):
        collection = cls.__new__(cls)
        collection._assign((), srid, has_z)
        return collection

    def _assign(self, geometries, srid, has_z):
        Geometry.__init__(self, srid, has_z)
        self._geometries = geometries

    @property
    def geometries(self):
        """The parts as a tuple, empty when there are none."""
        return self._geometries

    @property
    def is_empty(self):
        return all(part.is_empty for part in self._geometries)

    def _value_key(self):
        return (self._geometries,)

    def _wkb_type_code(self):
        return _WKB_GEOMETRYCOLLECTION

    def _wkt_type_name(self):
        return 'GEOMETRYCOLLECTION'

    def _has_no_parts(self):
        return len(self._geometries) == 0

    def _write_wkt_body(self, out):
        for index, part in enumerate(self._geometries):
            if index > 0:
                out.append(', ')
            _write_wkt_tagged(out, part)

    def _write_wkb_body(self, out):
        out += struct.pack('>i', len(self._geometries))
        for part in self._geometries:
            part._write_wkb(out)

    def _write_wkt_parts(self, out):
        """Appends the parts of a MULTI* value, each parenthesized or written as EMPTY."""
        for index, part in enumerate(self._geometries):
            if index > 0:
                out.append(', ')
            if part._has_no_parts():
                out.append('EMPTY')
            else:
                out.append('(')
                part._write_wkt_body(out)
                out.append(')')


class MultiCurve(GeometryCollection):
    """
    Abstract base class of the collections of curves. MultiLineString is the only kind.

    The type code and name stay abstract, because a collection of curves has no WKB or WKT form.
    """

    __slots__ = ()

    @abc.abstractmethod
    def _wkb_type_code(self):
        pass

    @abc.abstractmethod
    def _wkt_type_name(self):
        pass


class MultiSurface(GeometryCollection):
    """
    Abstract base class of the collections of surfaces. MultiPolygon is the only kind.

    The type code and name stay abstract, because a collection of surfaces has no WKB or WKT form.
    """

    __slots__ = ()

    @abc.abstractmethod
    def _wkb_type_code(self):
        pass

    @abc.abstractmethod
    def _wkt_type_name(self):
        pass


class MultiPoint(GeometryCollection):
    """A collection of points. A part may be an empty point."""

    __slots__ = ()

    def __init__(self, points):
        """
        Creates a multi-point from at least 1 point.

        :param points: at least 1 point, or use MultiPoint.empty() for an empty value
        :raises ValueError: if no point is given, if any element is not a point, or if the points
                 do not all carry the same SRID and z dimension
        """
        items = _check_parts(points, Point, "MultiPoint points")
        if len(items) == 0:
            raise ValueError("MultiPoint must have at least 1 point;"
                             " use MultiPoint.empty() for an empty multi-point")
        srid, has_z = _common_srid_and_z(items, "MultiPoint")
        self._assign(items, srid, has_z)

    def _wkb_type_code(self):
        return _WKB_MULTIPOINT

    def _wkt_type_name(self):
        return 'MULTIPOINT'

    def _write_wkt_body(self, out):
        self._write_wkt_parts(out)


class MultiLineString(MultiCurve):
    """A collection of line strings. A part may be an empty line string."""

    __slots__ = ()

    def __init__(self, line_strings):
        """
        Creates a multi-line string from at least 1 line string.

        :param line_strings: at least 1 line string, or use MultiLineString.empty() for an empty
               value
        :raises ValueError: if no line string is given, if any element is not a line string, or if
                 the line strings do not all carry the same SRID and z dimension
        """
        items = _check_parts(line_strings, LineString, "MultiLineString line strings")
        if len(items) == 0:
            raise ValueError("MultiLineString must have at least 1 line string;"
                             " use MultiLineString.empty() for an empty multi-line string")
        srid, has_z = _common_srid_and_z(items, "MultiLineString")
        self._assign(items, srid, has_z)

    def _wkb_type_code(self):
        return _WKB_MULTILINESTRING

    def _wkt_type_name(self):
        return 'MULTILINESTRING'

    def _write_wkt_body(self, out):
        self._write_wkt_parts(out)


class MultiPolygon(MultiSurface):
    """A collection of polygons. A part may be an empty polygon."""

    __slots__ = ()

    def __init__(self, polygons):
        """
        Creates a multi-polygon from at least 1 polygon.

        :param polygons: at least 1 polygon, or use MultiPolygon.empty() for an empty value
        :raises ValueError: if no polygon is given, if any element is not a polygon, or if the
                 polygons do not all carry the same SRID and z dimension
        """
        items = _check_parts(polygons, Polygon, "MultiPolygon polygons")
        if len(items) == 0:
            raise ValueError("MultiPolygon must have at least 1 polygon;"
                             " use MultiPolygon.empty() for an empty multi-polygon")
        srid, has_z = _common_srid_and_z(items, "MultiPolygon")
        self._assign(items, srid, has_z)

    def _wkb_type_code(self):
        return _WKB_MULTIPOLYGON

    def _wkt_type_name(self):
        return 'MULTIPOLYGON'

    def _write_wkt_body(self, out):
        self._write_wkt_parts(out)


class _BytesReader(object):
    """A reader over a bytes-like value that reports how much of it is left."""

    __slots__ = ('_data', '_position')

    def __init__(self, data):
        self._data = data
        self._position = 0

    def read(self, size):
        chunk = self._data[self._position:self._position + size]
        self._position += len(chunk)
        return bytes(chunk)

    def remaining(self):
        return len(self._data) - self._position


class _WkbReader(object):
    """
    Parser for ISO WKB, which rejects EWKB. Each geometry header carries its own byte order
    marker, and a little-endian geometry is swapped as it is read.

    A count read from the input never sizes a list up front, so a corrupt count ends in an
    unexpected end of input rather than a large allocation.
    """

    __slots__ = ('_stream', '_srid', '_depth', '_has_z')

    def __init__(self, stream, srid):
        self._stream = stream
        self._srid = srid
        self._depth = 0
        self._has_z = None

    def read_geometry(self):
        byte_order = self._read_exact(1)[0]
        if byte_order == _BIG_ENDIAN:
            order = '>'
        elif byte_order == _LITTLE_ENDIAN:
            order = '<'
        else:
            raise ValueError(f"Invalid WKB byte order: {byte_order}")

        type_code = struct.unpack(order + 'I', self._read_exact(4))[0]
        if type_code & (_EWKB_Z_FLAG | _EWKB_M_FLAG | _EWKB_SRID_FLAG):
            raise ValueError(f"EWKB format detected (type code 0x{type_code:08X});"
                             f" ISO WKB is expected."
                             f" EWKB with Z, M, or SRID flags is not supported.")
        if 2001 <= type_code <= 2007:
            raise ValueError(f"M dimension not supported (type code {type_code})")
        if 3001 <= type_code <= 3007:
            raise ValueError(f"ZM dimension not supported (type code {type_code})")

        if 1001 <= type_code <= 1007:
            base_type = type_code - 1000
            has_z = True
        elif 1 <= type_code <= 7:
            base_type = type_code
            has_z = False
        else:
            raise ValueError(f"Unknown WKB type code: {type_code}")

        if self._has_z is None:
            self._has_z = has_z
        elif self._has_z != has_z:
            raise ValueError("Mixed Z dimensions in WKB: some geometries have Z, some do not")

        if base_type == _WKB_POINT:
            return self._read_point(has_z, order)
        if base_type == _WKB_LINESTRING:
            return self._read_line_string(has_z, order)
        if base_type == _WKB_POLYGON:
            return self._read_polygon(has_z, order)
        if base_type == _WKB_MULTIPOINT:
            return self._read_multi(has_z, order, Point, MultiPoint, "MultiPoint")
        if base_type == _WKB_MULTILINESTRING:
            return self._read_multi(has_z, order, LineString, MultiLineString, "MultiLineString")
        if base_type == _WKB_MULTIPOLYGON:
            return self._read_multi(has_z, order, Polygon, MultiPolygon, "MultiPolygon")
        if base_type == _WKB_GEOMETRYCOLLECTION:
            return self._read_collection(has_z, order)
        raise ValueError(f"Unknown WKB type: {base_type}")

    def _read_point(self, has_z, order):
        """NaN x and y encode an empty point, whose z ordinate is read and discarded."""
        x = self._read_double(order)
        y = self._read_double(order)
        if math.isnan(x) and math.isnan(y):
            if has_z:
                self._read_double(order)
            return Point._empty_with_z(self._srid, has_z)
        z = self._read_double(order) if has_z else None
        return Point(x, y, z, srid=self._srid)

    def _read_vertex(self, has_z, order, context):
        """Reads a vertex of a line string or a ring, where NaN, an empty point, is malformed."""
        x = self._read_double(order)
        y = self._read_double(order)
        if math.isnan(x) and math.isnan(y):
            raise ValueError(f"A {context} cannot be an empty point;"
                             f" a vertex must be a real coordinate")
        z = self._read_double(order) if has_z else None
        return Point(x, y, z, srid=self._srid)

    def _read_line_string(self, has_z, order):
        count = self._read_count(order, "point count")
        if count == 0:
            return LineString._empty_with_z(self._srid, has_z)
        if count == 1:
            raise ValueError("LineString must have at least 2 points, got 1")
        points = []
        for _ in range(count):
            points.append(self._read_vertex(has_z, order, "linestring vertex"))
        return LineString(points)

    def _read_ring(self, has_z, order, context):
        count = self._read_count(order, "point count in ring")
        if count < 4:
            raise ValueError(f"Polygon ring must have at least 4 points, got {count}")
        points = []
        for _ in range(count):
            points.append(self._read_vertex(has_z, order, "ring vertex"))
        ring = LineString(points)
        _check_ring(ring, context)
        return ring

    def _read_polygon(self, has_z, order):
        count = self._read_count(order, "ring count")
        if count == 0:
            return Polygon._empty_with_z(self._srid, has_z)
        shell = self._read_ring(has_z, order, "Shell ring")
        holes = []
        for _ in range(count - 1):
            holes.append(self._read_ring(has_z, order, "Hole ring"))
        return Polygon(shell, holes)

    def _read_multi(self, has_z, order, part_type, collection_type, context):
        self._depth += 1
        _check_nesting_depth(self._depth)
        count = self._read_count(order, "geometry count")
        if count == 0:
            self._depth -= 1
            return collection_type._empty_with_z(self._srid, has_z)
        parts = []
        for _ in range(count):
            part = self.read_geometry()
            if not isinstance(part, part_type):
                raise ValueError(f"Expected {part_type.__name__} in {context},"
                                 f" got {type(part).__name__}")
            parts.append(part)
        self._depth -= 1
        return collection_type(parts)

    def _read_collection(self, has_z, order):
        self._depth += 1
        _check_nesting_depth(self._depth)
        count = self._read_count(order, "geometry count")
        if count == 0:
            self._depth -= 1
            return GeometryCollection._empty_with_z(self._srid, has_z)
        parts = []
        for _ in range(count):
            parts.append(self.read_geometry())
        self._depth -= 1
        return GeometryCollection(parts)

    def _read_count(self, order, context):
        """
        Reads an unsigned 32 bit count. A count of 2 ** 31 or more cannot address its own bytes,
        so it is rejected.
        """
        count = struct.unpack(order + 'I', self._read_exact(4))[0]
        if count > _INT32_MAX:
            raise ValueError(f"Invalid {context}: {count}")
        return count

    def _read_double(self, order):
        return struct.unpack(order + 'd', self._read_exact(8))[0]

    def _read_exact(self, size):
        chunk = self._stream.read(size)
        if chunk is None:
            chunk = b''
        # a stream may hand back less than was asked for while more is still to come
        while len(chunk) < size:
            more = self._stream.read(size - len(chunk))
            if not more:
                raise ValueError("Truncated WKB input: unexpected end of input")
            chunk += more
        return chunk
