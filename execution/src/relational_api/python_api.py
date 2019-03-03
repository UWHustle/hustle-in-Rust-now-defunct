import ctypes

# If this is distributed as a Python package we'll want to find a way to
# include the dynamic lib within that package
lib = ctypes.cdll.LoadLibrary("../../target/debug/libexecution.dylib")


class Relation:
    rust_ptr = ctypes.c_void_p()

    def __init__(self, col_names, col_type_names):
        cstr_col_names = (ctypes.c_char_p * len(col_names))()
        for i, name in enumerate(col_names):
            cstr_col_names[i] = name.encode("utf-8")
        cstr_col_type_names = (ctypes.c_char_p * len(col_type_names))()
        for i, type_name in enumerate(col_type_names):
            cstr_col_type_names[i] = type_name.encode("utf-8")

        new_relation = lib.new_relation
        new_relation.restype = ctypes.c_void_p
        result = new_relation(cstr_col_names, cstr_col_type_names, len(col_names))
        self.rust_ptr = ctypes.c_void_p(result)

    def print_name(self):
        lib.print_relation_name(self.rust_ptr)

    def __del__(self):
        lib.drop_relation(self.rust_ptr)


relation = Relation(["a", "b"], ["int", "int"])
relation.print_name()
del relation
