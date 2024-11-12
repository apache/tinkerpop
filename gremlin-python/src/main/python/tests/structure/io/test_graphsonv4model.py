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
import pytest

"""
Round trip testing of GraphSONV4 compared to a correct "model".

Set the IO_TEST_DIRECTORY environment variable to the directory where
the .gbin files that represent the serialized "model" are located. 
"""

from gremlin_python.structure.io.graphsonV4 import GraphSONWriter, GraphSONReader
import math
import json
import os

from .model import model

gremlin_test_dir = "gremlin-test/src/main/resources/org/apache/tinkerpop/gremlin/structure/io/graphson/"
directory_search_pattern = "gremlin-python" + os.sep + "src" + os.sep + "main"
default_dir = __file__[:__file__.find(directory_search_pattern)]
test_resource_directory = os.environ.get('IO_TEST_DIRECTORY_GRAPHSON', default_dir + gremlin_test_dir)
writer = GraphSONWriter()
reader = GraphSONReader()

def get_entry(title):
    return model[title]

def read_file_by_name(resource_name):
    full_name = test_resource_directory + resource_name + "-" + "v4" + ".json"
    with open(full_name, 'r') as resource_file:
        return json.dumps(json.load(resource_file), separators=(',', ':'))

def test_pos_bigdecimal():
    def decimal_cmp(x, y):
        # python json library can only read a BigDecimal into float during deser, precision will be lost with GraphSON
        # so compare float only
        if float(x.value) == float(y.value):
            return True
        else:
            return False

    run_writeread("pos-bigdecimal", decimal_cmp)

def test_neg_bigdecimal():
    def decimal_cmp(x, y):
        if float(x.value) == float(y.value):
            return True
        else:
            return False

    run_writeread("neg-bigdecimal", decimal_cmp)

def test_pos_biginteger():
    # gremlin-python adds an extra 0 byte to the value.
    run_writeread("pos-biginteger")

def test_neg_biginteger():
    # gremlin-python adds an extra 0 byte to the value.
    run_writeread("neg-biginteger")

def test_min_byte():
    run("min-byte")
    
def test_max_byte():
    run("max-byte")

def test_empty_binary():
    run("empty-binary")

def test_str_binary():
    run("str-binary")

def test_max_double():
    run("max-double")

def test_min_double():
    run("min-double")

def test_neg_max_double():
    run("neg-max-double")

def test_neg_min_double():
    run("neg-min-double")

def test_nan_double():
    def nan_cmp(x, y):
        if math.isnan(x) and math.isnan(y):
            return True
        else:
            return False
    run("nan-double", nan_cmp)

def test_pos_inf_double():
    run("pos-inf-double")

def test_neg_inf_double():
    run("neg-inf-double")

def test_neg_zero_double():
    run("neg-zero-double")

def test_zero_duration():
    run("zero-duration")

def test_traversal_edge():
    # properties aren't serialized in gremlin-python
    run_writeread("traversal-edge")

def test_no_prop_edge():
    # gremlin-python serializes/deserializes "null" for props not empty list
    run_writeread("no-prop-edge")

def test_max_int():
    run("max-int")

def test_min_int():
    run("min-int")

def test_max_long():
    # attempts to serialize a long as an int
    run_writeread("max-long")

def test_min_long():
    # attempts to serialize a long as an int
    run_writeread("min-long")

def test_var_type_list():
    run("var-type-list")

def test_empty_list():
    run("empty-list")

def test_var_type_map():
    # can't write tuple
    run_read("var-type-map")

def test_empty_map():
    run("empty-map")

# gremlin-python doesn't serialize path
def test_traversal_path():
    run_read("traversal-path")

def test_empty_path():
    run_read("empty-path")

def test_prop_path():
    run_read("prop-path")

def test_edge_property():
    run("edge-property")

def test_null_property():
    run("null-property")

def test_var_type_set():
    # ordering within set might be different
    run_writeread("var-type-set")

def test_empty_set():
    run("empty-set")

def test_max_short():
    # short deserialized to int
    run_writeread("max-short")

def test_min_short():
    # short deserialized to int
    run_writeread("min-short")

def test_specified_uuid():
    run("specified-uuid")

def test_nil_uuid():
    run("nil-uuid")

def test_no_prop_vertex():
    # gremlin-python serializes/deserializes "null" for props not empty list
    run_writeread("no-prop-vertex")

def test_traversal_vertexproperty():
    # properties aren't serialized in gremlin-python
    run_writeread("traversal-vertexproperty")

def test_meta_vertexproperty():
    # properties aren't serialized in gremlin-python
    run_writeread("meta-vertexproperty")

def test_set_cardinality_vertexproperty():
    # properties aren't serialized in gremlin-python
    run_writeread("set-cardinality-vertexproperty")

def test_id_t():
    run("id-t")

def test_out_direction():
    run("out-direction")

def test_var_bulklist():
    run_read("var-bulklist")

def test_empty_bulklist():
    run_read("empty-bulklist")

def test_single_byte_char():
    # char is serialized as string
    run_writeread("single-byte-char")

def test_multi_byte_char():
    # char is serialized as string
    run_writeread("multi-byte-char")

def test_unspecified_null():
    # no serializer for plain null
    run_writeread("unspecified-null")

def test_true_boolean():
    run("true-boolean")

def test_false_boolean():
    run("false-boolean")

def test_single_byte_string():
    run("single-byte-string")

def test_mixed_string():
    run("mixed-string")

def run(resource_name, comparator = None):
    """
    Runs the regular set of tests for the type which is
        1. model to deserialized object
        2. written bytes to read bytes
        3. round tripped (read then written) bytes to read bytes
    
    Optionally takes in a comparator that used for comparison 1.
    """
    resource_json = read_file_by_name(resource_name)

    model = get_entry(resource_name)
    read = reader.read_object(resource_json)
    if comparator is not None:
        assert comparator(model, read)
    else:
        assert model == read

    written = writer.write_object(model)
    assert resource_json == written

    round_tripped = writer.write_object(reader.read_object(resource_json))
    assert resource_json == round_tripped

def run_read(resource_name):
    """
    Runs the read test which compares the model to deserialized object

    This should only be used in cases where there is only a deserializer
    but no serializer for the same type.
    """
    resource_json = read_file_by_name(resource_name)

    model = get_entry(resource_name)
    read = reader.read_object(resource_json)
    assert model == read

def run_writeread(resource_name, comparator = None):
    """
    Runs a reduced set of tests for the type which is
        1. model to deserialized object
        2. model to round tripped (written then read) object
    
    Optionally takes in a comparator that used for comparison 1.

    Use this in cases where the regular run() function in too stringent.
    E.g. when ordering doesn't matter like for sets. Ideally, a type
    should be tested with run() instead when possible.
    """
    resource_json = read_file_by_name(resource_name)

    model = get_entry(resource_name)
    read = reader.read_object(resource_json)
    round_tripped = reader.read_object(writer.write_object(model))

    if comparator is not None:
        assert comparator(model, read)
        assert comparator(model, round_tripped)
    else:
        assert model == read
        assert model == round_tripped