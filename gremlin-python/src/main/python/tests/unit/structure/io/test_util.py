#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

import pytest

from gremlin_python.statics import SingleByte
from gremlin_python.structure.io.util import SymbolUtil, HashableDict, Marker


class TestSymbolUtil(object):

    def test_to_camel_case(self):
        assert SymbolUtil.to_camel_case("sumLong") == "sumLong"
        assert SymbolUtil.to_camel_case("sum_long") == "sumLong"
        assert SymbolUtil.to_camel_case("sum_") == "sum"
        assert SymbolUtil.to_camel_case("on_merge") == "onMerge"

    def test_to_snake_case(self):
        assert SymbolUtil.to_snake_case("sumLong") == "sum_long"
        assert SymbolUtil.to_snake_case("sum_long") == "sum_long"
        assert SymbolUtil.to_snake_case("sum") == "sum"
        assert SymbolUtil.to_snake_case("onMerge") == "on_merge"


class TestHashableDict(object):

    def test_of_converts_nested_dict_to_hashable(self):
        """HashableDict.of recursively converts nested dicts into HashableDict."""
        result = HashableDict.of({'k': {'inner': [1, 2]}})
        assert isinstance(result, HashableDict)
        assert isinstance(result['k'], HashableDict)
        # Nested list values are converted to tuples so they are hashable.
        assert result['k']['inner'] == (1, 2)

    def test_of_result_is_hashable(self):
        """The result of HashableDict.of is hashable."""
        result = HashableDict.of({'k': {'inner': [1, 2]}})
        assert isinstance(hash(result), int)

    def test_of_equal_inputs_are_equal_and_hash_equal(self):
        """Equal inputs produce equal HashableDicts with equal hashes."""
        a = HashableDict.of({'k': {'inner': [1, 2]}})
        b = HashableDict.of({'k': {'inner': [1, 2]}})
        assert a == b
        assert hash(a) == hash(b)

    def test_of_passthrough_for_scalars(self):
        """Non-collection values are returned unchanged."""
        assert HashableDict.of(42) == 42
        assert HashableDict.of("x") == "x"

    def test_of_converts_collections_to_tuples(self):
        """Lists and sets become tuples of converted elements."""
        assert HashableDict.of([1, 2, 3]) == (1, 2, 3)
        assert HashableDict.of([{'a': 1}])[0] == HashableDict.of({'a': 1})

    def test_hash_fallback_with_unsortable_mixed_keys(self):
        """Keys of mutually unsortable types fall back to string-based hashing without error."""
        # int and str keys cannot be sorted against each other in Python 3,
        # so __hash__ must use its except-branch fallback.
        d = HashableDict({1: 'a', 'b': 2})
        assert isinstance(hash(d), int)

    def test_hash_fallback_is_stable(self):
        """The fallback hash is stable across equal dicts."""
        d1 = HashableDict({1: 'a', 'b': 2})
        d2 = HashableDict({1: 'a', 'b': 2})
        assert hash(d1) == hash(d2)


class TestMarker(object):

    def test_of_zero_is_end_of_stream(self):
        """Marker.of(0) returns the end-of-stream marker singleton."""
        assert Marker.of(0) == Marker.end_of_stream()

    def test_of_nonzero_raises_value_error(self):
        """Marker.of with a non-zero value raises ValueError."""
        with pytest.raises(ValueError):
            Marker.of(1)

    def test_end_of_stream_value(self):
        """The end-of-stream marker wraps SingleByte(0)."""
        assert Marker.end_of_stream().get_value() == SingleByte(0)


