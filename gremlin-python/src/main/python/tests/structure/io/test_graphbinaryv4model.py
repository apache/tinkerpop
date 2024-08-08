from gremlin_python.structure.io.graphbinaryV4 import GraphBinaryWriter, GraphBinaryReader
from os import environ

from .model import model, unsupported

gremlin_test_dir = "gremlin-test/src/main/resources/org/apache/tinkerpop/gremlin/structure/graphbinary/"
test_resource_directory = environ.get('IO_TEST_DIRECTORY', '../' + gremlin_test_dir)
writer = GraphBinaryWriter()
reader = GraphBinaryReader()

def get_entry(title):
    return model[title]

def read_file_by_name(resource_name):
    full_name = test_resource_directory + resource_name + "-" + "v4" + ".gbin"
    with open(full_name, 'rb') as resource_file:
        return bytearray(resource_file.read())


def test_pos_bigdecimal():
    resource_name = "pos-bigdecimal"
    resource_bytes = read_file_by_name(resource_name)
    
    model = get_entry(resource_name)
    read = reader.read_object(resource_bytes)
    assert model.scale == read.scale
    assert model.unscaled_value == read.unscaled_value

    written = writer.write_object(model)
    assert resource_bytes == written

def test_neg_bigdecimal():
    resource_name = "pos-bigdecimal"
    resource_bytes = read_file_by_name(resource_name)
    
    model = get_entry(resource_name)
    read = reader.read_object(resource_bytes)
    assert model.scale == read.scale
    assert model.unscaled_value == read.unscaled_value

    written = writer.write_object(model)
    assert resource_bytes == written

def test_pos_biginteger():
    run("pos-biginteger")

def test_neg_biginteger():
    run("neg-biginteger")

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
    run("nan-double")

def test_pos_inf_double():
    run("pos-inf-double")

def test_neg_inf_double():
    run("neg-inf-double")

def test_neg_zero_double():
    run("neg-zero-double")

def test_zero_duration():
    run("zero-duration")

#def test_forever_duration():
#    run("forever-duration")

def test_traversal_edge():
    run("traversal-edge")

def test_no_prop_edge():
    run("no-prop-edge")

def test_max_float():
    run("max-float")

def test_min_float():
    run("min-float")

def test_neg_max_float():
    run("neg-max-float")

def test_neg_min_float():
    run("neg-min-float")

def test_nan_float():
    run("nan-float")

def test_pos_inf_float():
    run("pos-inf-float")

def test_neg_inf_float():
    run("neg-inf-float")

def test_neg_zero_float():
    run("neg-zero-float")

def test_max_int():
    run("max-int")

def test_min_int():
    run("min-int")

def test_max_long():
    run("max-long")

def test_min_long():
    run("min-long")

def test_var_type_list():
    run("var-type-list")

def test_empty_list():
    run("empty-list")

def test_var_type_map():
    run("var-type-map")

def test_empty_map():
    run("empty-map")

def test_max_offsetdatetime():
    run("max-offsetdatetime")

def test_min_offsetdatetime():
    run("min-offsetdatetime")

def test_traversal_path():
    run("traversal-path")

#TODO: come back
def test_empty_path():
    run("empty-path")

def test_prop_path():
    run("prop-path")

def test_zero_date():
    run("zero-date")

def test_edge_property():
    run("edge-property")

def test_null_property():
    run("null-property")

def test_var_type_set():
    run("var-type-set")

def test_empty_set():
    run("empty-set")

def test_max_short():
    run("max-short")

def test_min_short():
    run("min-short")

def test_tinker_graph():
    run("tinker-graph")

def test_vertex_traverser():
    run("vertex-traverser")

def test_bulked_traverser():
    run("bulked-traverser")

def test_empty_traverser():
    run("empty-traverser")

def test_specified_uuid():
    run("specified-uuid")

def test_nil_uuid():
    run("nil-uuid")

def test_no_prop_vertex():
    run("no-prop-vertex")

def test_id_t():
    run("id-t")

def test_out_direction():
    run("out-direction")

def run(resource_name):
    if resource_name in unsupported:
        return

    resource_bytes = read_file_by_name(resource_name)
    
    model = get_entry(resource_name)
    read = reader.read_object(resource_bytes)
    assert model == read

    written = writer.write_object(model)
    assert resource_bytes == written

    round_tripped = writer.write_object(reader.read_object(resource_bytes))
    assert resource_bytes == round_tripped