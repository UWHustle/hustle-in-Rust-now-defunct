import ctypes
import ctypes.util
from ctypes import c_void_p, c_uint32, c_ubyte, c_char_p
import numpy

# TODO: Revert to find_library
# lib_path = ctypes.util.find_library('execution')
lib_path = '/Users/Matthew/Repos/hustle/build/execution/libexecution.dylib'
assert lib_path is not None, 'Unable to load hustle dylib'
ffi = ctypes.cdll.LoadLibrary(lib_path)


class Relation:
    def __init__(self, relation_p):
        self.relation_p = relation_p
        ffi.ffi_get_name_p.restype = c_void_p
        self.name_p = c_void_p(ffi.ffi_get_name_p(relation_p))
        ffi.ffi_get_col_names_p.restype = c_void_p
        self.col_names_p = c_void_p(ffi.ffi_get_col_names_p(relation_p))
        ffi.ffi_get_type_names_p.restype = c_void_p
        self.type_names_p = c_void_p(ffi.ffi_get_type_names_p(relation_p))
        ffi.ffi_get_n_cols.restype = c_uint32
        self.n_cols = c_uint32(ffi.ffi_get_n_cols(relation_p))

    def __del__(self):
        ffi.ffi_drop_relation(self.relation_p)
        ffi.ffi_drop_c_str(self.name_p)
        ffi.ffi_drop_c_str_vec(self.col_names_p)
        ffi.ffi_drop_c_str_vec(self.type_names_p)

    @classmethod
    def create(cls, col_names, type_names):
        ffi.ffi_new_relation.restype = c_void_p
        relation_p = c_void_p(ffi.ffi_new_relation(
            _encode_c_str_list(col_names),
            _encode_c_str_list(type_names),
            c_uint32(len(col_names))))
        return Relation(relation_p)

    @classmethod
    def from_numpy(cls, array):
        relation = Relation.create(
            array.dtype.names,
            _numpy_to_type_names(array))
        ffi.ffi_copy_buffer(
            relation.relation_p,
            c_void_p(array.ctypes.data),
            c_uint32(array.nbytes))
        return relation

    def to_numpy(self):
        numpy_type = _schema_to_numpy_type(
            self.get_col_names(),
            self.get_type_names())
        ffi.ffi_get_data_p.restype = c_void_p
        data_p = c_void_p(ffi.ffi_get_data_p(self.relation_p))
        if data_p is not None:
            ffi.ffi_get_slice_p.restype = c_void_p
            slice_p = c_void_p(ffi.ffi_get_slice_p(data_p))
            ffi.ffi_get_slice_size.restype = c_uint32
            slice_size = int(ffi.ffi_get_slice_size(data_p))
            buff = (slice_size * c_ubyte).from_address(slice_p.value)
            output = numpy.frombuffer(bytes(buff), dtype=numpy_type)
            ffi.ffi_drop_data(data_p)
            return output
        return numpy.empty(0, dtype=numpy_type)

    def get_name(self):
        return _decode_c_str(self.name_p)

    def get_col_names(self):
        return _decode_c_str_vec(self.col_names_p, self.n_cols.value)

    def get_type_names(self):
        return _decode_c_str_vec(self.type_names_p, self.n_cols.value)

    def import_hustle(self, name):
        ffi.ffi_import_hustle(self.relation_p, _encode_c_str(name))

    def export_hustle(self, name):
        ffi.ffi_export_hustle(self.relation_p, _encode_c_str(name))

    def import_csv(self, filename):
        ffi.ffi_import_csv(self.relation_p, _encode_c_str(filename))

    def export_csv(self, filename):
        ffi.ffi_export_csv(self.relation_p, _encode_c_str(filename))

    def aggregate(self, agg_col_name, group_by_col_names, agg_func):
        ffi.ffi_aggregate.restype = c_void_p
        relation_p = c_void_p(ffi.ffi_aggregate(
            self.relation_p,
            _encode_c_str(agg_col_name),
            _encode_c_str_list(group_by_col_names),
            c_uint32(len(group_by_col_names)),
            _encode_c_str(agg_func)))
        return Relation(relation_p)

    def insert(self, values):
        value_strings = [str(value) for value in values]
        ffi.ffi_insert(
            self.relation_p,
            _encode_c_str_list(value_strings),
            c_uint32(len(values)))

    def join(self, other):
        ffi.ffi_join.restype = c_void_p
        relation_p = c_void_p(ffi.ffi_join(self.relation_p, other.relation_p))
        return Relation(relation_p)

    def limit(self, limit):
        ffi.ffi_limit.restype = c_void_p
        relation_p = c_void_p(ffi.ffi_limit(
            self.relation_p,
            c_uint32(limit)))
        return Relation(relation_p)

    def print(self):
        ffi.ffi_print(self.relation_p)

    def project(self, col_names):
        ffi.ffi_project.restype = c_void_p
        relation_p = c_void_p(ffi.ffi_project(
            self.relation_p,
            _encode_c_str_list(col_names),
            c_uint32(len(col_names))))
        return Relation(relation_p)

    def select(self, predicate):
        ffi.ffi_select.restype = c_void_p
        relation_p = c_void_p(ffi.ffi_select(
            self.relation_p,
            _encode_c_str(predicate)))
        return Relation(relation_p)


def _schema_to_numpy_type(col_names, type_names):
    type_map = {
        'tinyint': numpy.uint8,
        'smallint': numpy.int16,
        'int': numpy.int32,
        'bigint': numpy.int64,
        'real': numpy.float32,
        'double': numpy.float64,
    }
    type_list = []
    for i in range(len(col_names)):
        type_list.append((col_names[i], type_map[type_names[i]]))
    return numpy.dtype(type_list)


def _numpy_to_type_names(array):
    type_map = {
        'uint8': 'tinyint',
        'int16': 'smallint',
        'int32': 'int',
        'int64': 'bigint',
        'float32': 'real',
        'float64': 'double',
    }
    type_names = []
    for name in array.dtype.names:
        type_names.append(type_map[str(array.dtype[name])])
    return type_names


def _encode_c_str(string):
    return string.encode('utf-8')


def _encode_c_str_list(py_list):
    encoded = (ctypes.c_char_p * len(py_list))()
    for i, name in enumerate(py_list):
        encoded[i] = name.encode('utf-8')
    return encoded


def _decode_c_str(c_str):
    return ctypes.cast(c_str, c_char_p).value.decode('utf-8')


def _decode_c_str_vec(vec_p, n_str):
    decoded = []
    for i in range(n_str):
        ffi.ffi_get_str_i.restype = c_void_p
        string = c_void_p(ffi.ffi_get_str_i(vec_p, i))
        decoded.append(_decode_c_str(string))
    return decoded
