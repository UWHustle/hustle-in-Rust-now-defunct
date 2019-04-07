import ctypes
import ctypes.util

lib_path = ctypes.util.find_library('execution')
assert lib_path is not None, 'Unable to find Hustle shared lib "execution"'
lib = ctypes.cdll.LoadLibrary(lib_path)


class Relation:
    relation_ptr = ctypes.c_void_p()
    name_ptr = ctypes.c_void_p()

    def __init__(self, relation_ptr):
        self.relation_ptr = relation_ptr
        ffi_get_name = lib.ffi_get_name
        ffi_get_name.restype = ctypes.c_void_p
        self.name_ptr = ctypes.c_void_p(ffi_get_name(self.relation_ptr))

    def __del__(self):
        lib.ffi_drop_relation(self.relation_ptr)
        lib.ffi_drop_c_str(self.name_ptr)

    @classmethod
    def create(cls, col_names, col_type_names):
        col_names_ptr = _encode_c_str_list(col_names)
        col_type_names_ptr = _encode_c_str_list(col_type_names)
        n_cols = ctypes.c_uint32(len(col_names))
        ffi_new_relation = lib.ffi_new_relation
        ffi_new_relation.restype = ctypes.c_void_p
        relation_ptr = ctypes.c_void_p(ffi_new_relation(
            col_names_ptr, col_type_names_ptr, n_cols))
        return Relation(relation_ptr)

    def get_name(self):
        return _decode_c_str(self.name_ptr)

    def import_hustle(self, name):
        lib.ffi_import_hustle(self.relation_ptr, name.encode('utf-8'))

    def export_hustle(self, name):
        lib.ffi_export_hustle(self.relation_ptr, name.encode('utf-8'))

    def import_csv(self, filename):
        lib.ffi_import_csv(self.relation_ptr, filename.encode('utf-8'))

    def export_csv(self, filename):
        lib.ffi_export_csv(self.relation_ptr, filename.encode('utf-8'))

    def aggregate(self, agg_col_name, group_by_col_names, agg_name):
        agg_col_name_ptr = agg_col_name.encode('utf-8')
        group_by_col_names_ptr = _encode_c_str_list(group_by_col_names)
        n_group_by_cols = ctypes.c_uint32(len(group_by_col_names))
        agg_name_ptr = agg_name.encode('utf-8')
        ffi_aggregate = lib.ffi_aggregate
        ffi_aggregate.restype = ctypes.c_void_p
        relation_ptr = ctypes.c_void_p(ffi_aggregate(
            self.relation_ptr,
            agg_col_name_ptr,
            group_by_col_names_ptr,
            n_group_by_cols,
            agg_name_ptr))
        return Relation(relation_ptr)

    def insert(self, values):
        value_strings = [str(value) for value in values]
        value_strings_ptr = _encode_c_str_list(value_strings)
        n_value_strings = ctypes.c_uint32(len(values))
        lib.ffi_insert(self.relation_ptr, value_strings_ptr, n_value_strings)

    def join(self, other):
        ffi_join = lib.ffi_join
        ffi_join.restype = ctypes.c_void_p
        relation_ptr = ctypes.c_void_p(ffi_join(
            self.relation_ptr, other.relation_ptr))
        return Relation(relation_ptr)

    def limit(self, limit):
        limit_c = ctypes.c_uint32(limit)
        ffi_limit = lib.ffi_limit
        ffi_limit.restype = ctypes.c_void_p
        relation_ptr = ctypes.c_void_p(
            ffi_limit(self.relation_ptr, limit_c))
        return Relation(relation_ptr)

    def print(self):
        lib.ffi_print(self.relation_ptr)

    def project(self, col_names):
        col_names_ptr = _encode_c_str_list(col_names)
        n_cols = ctypes.c_uint32(len(col_names))
        ffi_project = lib.ffi_project
        ffi_project.restype = ctypes.c_void_p
        relation_ptr = ctypes.c_void_p(ffi_project(
            self.relation_ptr, col_names_ptr, n_cols))
        return Relation(relation_ptr)

    def select(self, predicate):
        ffi_select = lib.ffi_select
        ffi_select.restype = ctypes.c_void_p
        relation_ptr = ctypes.c_void_p(ffi_select(
            self.relation_ptr, predicate.encode('utf-8')))
        return Relation(relation_ptr)


def _decode_c_str(c_str):
    return ctypes.cast(c_str, ctypes.c_char_p).value.decode('utf-8')


def _encode_c_str_list(py_list):
    encoded = (ctypes.c_char_p * len(py_list))()
    for i, name in enumerate(py_list):
        encoded[i] = name.encode('utf-8')
    return encoded
