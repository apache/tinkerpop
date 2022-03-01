#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

from gremlin_python.structure.io.util import SymbolUtil


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


