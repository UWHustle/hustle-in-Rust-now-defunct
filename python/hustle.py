import ctypes
import ctypes.util
from ctypes import c_void_p, c_uint32, c_ubyte, c_char_p, c_int
import numpy

# TODO: Revert to find_library
# lib_path = ctypes.util.find_library('execution')
lib_path = '/Users/Matthew/Repos/hustle/build/execution/libexecution.dylib'
assert lib_path is not None, 'Unable to load hustle dylib'
ffi = ctypes.cdll.LoadLibrary(lib_path)


class Relation:
    def __init__(self, relation_p, err_p):
        self.err_p = err_p
        self.relation_p = relation_p
        self.name_p = _run_ffi(
            ffi.ffi_get_name_p,
            c_void_p,
            relation_p)
        self.col_names_p = _run_ffi(
            ffi.ffi_get_col_names_p,
            c_void_p,
            relation_p)
        self.type_names_p = _run_ffi(
            ffi.ffi_get_type_names_p,
            c_void_p,
            relation_p)
        self.n_cols = _run_ffi(
            ffi.ffi_get_n_cols,
            c_uint32,
            relation_p)

    def __del__(self):
        ffi.ffi_drop_err_p(self.err_p)
        ffi.ffi_drop_relation(self.relation_p)
        ffi.ffi_drop_c_str(self.name_p)
        ffi.ffi_drop_c_str_vec(self.col_names_p)
        ffi.ffi_drop_c_str_vec(self.type_names_p)

    @classmethod
    def create(cls, col_names, type_names):
        if len(col_names) != len(type_names):
            raise ValueError('number of columns and typenames not equal')
        err_p = _get_err_p()
        relation_p = _run_ffi_p(
            ffi.ffi_new_relation,
            err_p,
            _encode_c_str_list(col_names),
            _encode_c_str_list(type_names),
            c_uint32(len(col_names)))
        return Relation(relation_p, err_p)

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
        data_p = _run_ffi(
            ffi.ffi_get_data_p,
            c_void_p,
            self.relation_p)
        if data_p is not None:
            slice_p = _run_ffi(
                ffi.ffi_get_slice_p,
                c_void_p,
                data_p)
            slice_size = _run_ffi(
                ffi.ffi_get_slice_size,
                c_uint32,
                data_p)
            buff = (slice_size.value * c_ubyte).from_address(slice_p.value)
            output = numpy.frombuffer(bytes(buff), dtype=numpy_type)
            ffi.ffi_drop_data(data_p)
            return output
        else:
            return numpy.empty(0, dtype=numpy_type)

    def get_name(self):
        return _decode_c_str(self.name_p)

    def get_col_names(self):
        return _decode_c_str_vec(self.col_names_p, self.n_cols.value)

    def get_type_names(self):
        return _decode_c_str_vec(self.type_names_p, self.n_cols.value)

    def import_hustle(self, name):
        _run_ffi_i(
            ffi.ffi_import_hustle,
            self.err_p,
            self.relation_p,
            _encode_c_str(name))

    def export_hustle(self, name):
        _run_ffi_i(
            ffi.ffi_export_hustle,
            self.err_p,
            self.relation_p,
            _encode_c_str(name))

    def import_csv(self, filename):
        _run_ffi_i(
            ffi.ffi_import_csv,
            self.err_p,
            self.relation_p,
            _encode_c_str(filename))

    def export_csv(self, filename):
        _run_ffi_i(
            ffi.ffi_export_csv,
            self.err_p,
            self.relation_p,
            _encode_c_str(filename))

    def aggregate(self, agg_col_name, group_by_col_names, agg_func):
        ffi.ffi_aggregate.restype = c_void_p
        relation_p = _run_ffi_p(
            ffi.ffi_aggregate,
            self.err_p,
            self.relation_p,
            _encode_c_str(agg_col_name),
            _encode_c_str_list(group_by_col_names),
            c_uint32(len(group_by_col_names)),
            _encode_c_str(agg_func))
        return Relation(relation_p, _get_err_p())

    def insert(self, values):
        value_strings = [str(value) for value in values]
        _run_ffi_i(
            ffi.ffi_insert,
            self.err_p,
            self.relation_p,
            _encode_c_str_list(value_strings),
            c_uint32(len(values)))

    def join(self, other):
        relation_p = _run_ffi_p(
            ffi.ffi_join,
            self.err_p,
            self.relation_p,
            other.relation_p)
        return Relation(relation_p, _get_err_p())

    def limit(self, limit):
        relation_p = _run_ffi_p(
            ffi.ffi_limit,
            self.err_p,
            self.relation_p,
            c_uint32(limit))
        return Relation(relation_p, _get_err_p())

    def print(self):
        _run_ffi_i(
            ffi.ffi_print,
            self.err_p,
            self.relation_p)

    def project(self, col_names):
        relation_p = _run_ffi_p(
            ffi.ffi_project,
            self.err_p,
            self.relation_p,
            _encode_c_str_list(col_names),
            c_uint32(len(col_names)))
        return Relation(relation_p, _get_err_p())

    def select(self, predicate):
        relation_p = _run_ffi_p(
            ffi.ffi_select,
            self.err_p,
            self.relation_p,
            _encode_c_str(predicate))
        return Relation(relation_p, _get_err_p())


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
        hustle_type = type_names[i]
        numpy_type = type_map[hustle_type]
        if numpy_type is None:
            raise ValueError('no Numpy mapping for type ' + hustle_type)
        type_list.append((col_names[i], numpy_type))
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
    for numpy_type in array.dtype.names:
        hustle_type = type_map[str(array.dtype[numpy_type])]
        if hustle_type is None:
            raise ValueError('no Hustle mapping for type ' + numpy_type)
        type_names.append(hustle_type)
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
        str_p = _run_ffi(
            ffi.ffi_get_str_i,
            c_void_p,
            vec_p,
            i)
        decoded.append(_decode_c_str(str_p))
    return decoded


def _run_ffi_p(function, err_p, *args):
    function.restype = c_void_p
    output = c_void_p(function(*args, err_p))
    if output.value is None:
        raise RuntimeError(_get_err_str(err_p))
    return output


def _run_ffi_i(function, err_p, *args):
    function.restype = c_int
    code = c_int(function(*args, err_p))
    if code.value != 0:
        raise RuntimeError(_get_err_str(err_p))


def _run_ffi(function, restype, *args):
    function.restype = restype
    return restype(function(*args))


def _get_err_str(err_p):
    err_str_p = _run_ffi(
        ffi.ffi_get_err_str_p,
        c_void_p,
        err_p)
    output = _decode_c_str(err_str_p)
    ffi.ffi_drop_c_str(err_str_p)
    return output


def _get_err_p():
    ffi.ffi_get_err_p.restype = c_void_p
    return c_void_p(ffi.ffi_get_err_p())


def _drop_err_p(err_p):
    ffi.ffi_drop_err_p(err_p)
