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
    """
    Represents a table in the Hustle database.

    Relation objects should not be accessed concurrently. This is unlikely to
    be a bottleneck, however, as the Hustle library can be written to
    internally parallelize operations.
    """

    def __init__(self, relation_p, err_p):
        """
        The default constructor.

        :param relation_p: A C pointer to the relation
        :param err_p: A C pointer to a string where error messages are stored
        """
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
        """
        Frees memory resources allocated by the dylib.
        """
        ffi.ffi_drop_err_p(self.err_p)
        ffi.ffi_drop_relation(self.relation_p)
        ffi.ffi_drop_c_str(self.name_p)
        ffi.ffi_drop_c_str_vec(self.col_names_p)
        ffi.ffi_drop_c_str_vec(self.type_names_p)

    @classmethod
    def create(cls, col_names, type_names):
        """
        Allocates a new relation with the specified schema.

        :param col_names: The names of columns in the relation
        :param type_names: Typenames of each column; allowed values are:
            tinyint
            smallint
            int
            bigint/long
            real
            double
            varchar(size)
            char(size)
        :return: A new Relation object
        """
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
        """
        Attempts to convert a Numpy array to a Hustle relation.

        The array should be 1-dimensional, with each value having a compound
        type corresponding to the schema of the relation. For example, an array
        with type:
            dtype([('a', '<i4'), ('b', '<i8')])
        will be converted to a relation with schema:
            ['a', 'b'], ['int', 'bigint']
        The relation receives a copy of the data, so modifying the relation
        will leave the array unchanged.

        :param array: The Numpy array to convert
        :return: A new Relation object containing the same data as the array
        """
        relation = Relation.create(
            array.dtype.names,
            _numpy_to_type_names(array))
        ffi.ffi_copy_buffer(
            relation.relation_p,
            c_void_p(array.ctypes.data),
            c_uint32(array.nbytes))
        return relation

    def to_numpy(self):
        """
        Reverses the behavior of from_numpy, converting a Hustle relation to a
        Numpy array. The Numpy array receives a copy of the data, so modifying
        the array will leave the relation unchanged.

        :return: A Numpy array containing the same data as the relation
        """
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
        """
        Gets the automatically-set name of the relation.

        :return: The name of the relation (a string)
        """
        return _decode_c_str(self.name_p)

    def get_col_names(self):
        """
        Gets a list of the relation's column names.

        This will be equal (at least value-wise) to the col_names list passed
        to the create method.

        :return: A list of column names (strings)
        """
        return _decode_c_str_vec(self.col_names_p, self.n_cols.value)

    def get_type_names(self):
        """
        Gets a list of the relation's type names.

        Some standardization of typenames may have taken place (e.g. referring
        to 'long' as 'bigint', but the result should be logically equal to the
        type_names list passed to the create method.

        :return: A list of type names (strings)
        """
        return _decode_c_str_vec(self.type_names_p, self.n_cols.value)

    def import_hustle(self, name):
        """
        Imports a Hustle relation with the specified name, overwriting data
        currently in the relation.

        The imported relation may have been created using some other interface
        (e.g. SQL through the command line). The name should not include any
        filename extension (.hustle), and should match the name that would be
        used in an SQL query.

        The imported relation is assumed to have the same schema as self. This
        method's behavior is undefined if this isn't the case.

        :param name: The name of the Hustle relation to import
        """
        _run_ffi_i(
            ffi.ffi_import_hustle,
            self.err_p,
            self.relation_p,
            _encode_c_str(name))

    def export_hustle(self, name):
        """
        Exports a Hustle relation containing the data in this relation.

        This writes the relation to disk. It can subsequently be queried using
        SQL syntax and the full Hustle stack.

        :param name: The name to be assigned to the Hustle relation on export
        """
        _run_ffi_i(
            ffi.ffi_export_hustle,
            self.err_p,
            self.relation_p,
            _encode_c_str(name))

    def import_csv(self, filename):
        """
        Imports data from a CSV file, overwriting data currently in the
        relation.

        The layout of the CSV file is assumed to "match" the schema. This means
        there should be a column for every attribute of the relation and that
        values should be parseable to the correct types. This method's behavior
        is undefined if this isn't the case.

        :param filename: The filename to import
        """
        _run_ffi_i(
            ffi.ffi_import_csv,
            self.err_p,
            self.relation_p,
            _encode_c_str(filename))

    def export_csv(self, filename):
        """
        Exports a CSV file containing the data in this relation.

        :param filename: The filename assigned to the exported CSV data
        """
        _run_ffi_i(
            ffi.ffi_export_csv,
            self.err_p,
            self.relation_p,
            _encode_c_str(filename))

    def aggregate(self, agg_col_name, group_by_col_names, agg_func):
        """
        Performs an aggregation on this relation.

        :param agg_col_name: The name of the column on which the aggregation
        should be performed
        :param group_by_col_names: A list of columns to group by
        :param agg_func: The name of the aggregate function to apply; allowed
        values are avg, count, min, max, and sum. Function names are not case-
        sensitive.
        :return: A new relation with the aggregation applied
        """
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

    def insert(self, *args):
        """
        Appends a new row of values to the relation.

        Arguments to this function are "stringified", passed to the Hustle
        dylib, then parsed. This means that the types of values passed to this
        method don't need to exactly match the schema as long as their string
        representations can be parsed to the appropriate type.

        :param args: The values to insert
        """
        if len(args) != self.n_cols.value:
            raise ValueError(
                'should have ' + str(self.n_cols.value) + ' arguments')
        value_strings = [str(arg) for arg in args]
        _run_ffi_i(
            ffi.ffi_insert,
            self.err_p,
            self.relation_p,
            _encode_c_str_list(value_strings),
            self.n_cols)

    def join(self, other):
        """
        Performs a Cartesian product with another relation.

        :param other: The relation to multiply by this one
        :return: A new, joined relation
        """
        relation_p = _run_ffi_p(
            ffi.ffi_join,
            self.err_p,
            self.relation_p,
            other.relation_p)
        return Relation(relation_p, _get_err_p())

    def limit(self, limit):
        """
        Selects the first 'limit' rows from the relation.

        :param limit: The number of rows to select
        :return: A new relation containing the selected rows
        """
        relation_p = _run_ffi_p(
            ffi.ffi_limit,
            self.err_p,
            self.relation_p,
            c_uint32(limit))
        return Relation(relation_p, _get_err_p())

    def print(self):
        """
        Prints the contents of the relation to stdout.

        """
        _run_ffi_i(
            ffi.ffi_print,
            self.err_p,
            self.relation_p)

    def project(self, col_names):
        """
        Projects onto a subset of this relation's columns.

        :param col_names: The names of columns which should be retained
        :return: A new relation containing the specified columns
        """
        relation_p = _run_ffi_p(
            ffi.ffi_project,
            self.err_p,
            self.relation_p,
            _encode_c_str_list(col_names),
            c_uint32(len(col_names)))
        return Relation(relation_p, _get_err_p())

    def select(self, predicate):
        """
        Filters rows according to some predicate.

        Predicates should strings be of the form:
            <col> <op> <val>
        Where <col> is the column to test, <op> is the comparison operator, and
        <val> is the value to compare against. Allowed values of <op> are =, <
        <=, >, <=, or string variants (case insensitive):
            equal
            less
            lessorequal
            greater
            greaterorequal
        <val> should be parseable to the type of <col>.

        :param predicate: The condition used for selection (see above)
        :return: A new relations containing the result of the selection
        """
        relation_p = _run_ffi_p(
            ffi.ffi_select,
            self.err_p,
            self.relation_p,
            _encode_c_str(predicate))
        return Relation(relation_p, _get_err_p())


def _schema_to_numpy_type(col_names, type_names):
    """
    Helper for constructing Numpy dtypes.
    """
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
        if 'varchar' in hustle_type:
            type_str = hustle_type.replace('varchar(', 'S')
            type_str = type_str.replace(')', '')
            numpy_type = numpy.dtype(type_str)
        elif 'char' in hustle_type:
            type_str = hustle_type.replace('char(', 'S')
            type_str = type_str.replace(')', '')
            numpy_type = numpy.dtype(type_str)
        else:
            numpy_type = type_map[hustle_type]
            if numpy_type is None:
                raise ValueError('no Numpy mapping for type ' + hustle_type)
        type_list.append((col_names[i], numpy_type))
    return numpy.dtype(type_list)


def _numpy_to_type_names(array):
    """
    Helper for constructing Hustle schemas from Numpy dtypes.
    """
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
        type_str = str(array.dtype[numpy_type])
        if '|S' in type_str:
            hustle_type = type_str.replace('|S', 'varchar(')
            hustle_type += ')'
        else:
            hustle_type = type_map[str(array.dtype[numpy_type])]
            if hustle_type is None:
                raise ValueError('no Hustle mapping for type ' + numpy_type)
        type_names.append(hustle_type)
    return type_names


def _encode_c_str(string):
    """
    Helper for converting Python strings to C strings.
    """
    return string.encode('utf-8')


def _encode_c_str_list(py_list):
    """
    Helper for converting a list of Python strings to an array of char
    pointers (C strings).
    """
    encoded = (ctypes.c_char_p * len(py_list))()
    for i, name in enumerate(py_list):
        encoded[i] = name.encode('utf-8')
    return encoded


def _decode_c_str(c_str):
    """
    Helper for converting a C string to a Python string.
    """
    return ctypes.cast(c_str, c_char_p).value.decode('utf-8')


def _decode_c_str_vec(vec_p, n_str):
    """
    Helper for converting a Rust Vector of C strings to a list of
    Python strings.

    This is a bit inefficient as it makes repeated dylib calls.
    """
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
    """
    Helper for executing a dylib function which returns a pointer.

    An error is assumed to have occurred if the value of the returned pointer
    is zero/null.
    """
    function.restype = c_void_p
    output = c_void_p(function(*args, err_p))
    if output.value is None:
        raise RuntimeError(_get_err_str(err_p))
    return output


def _run_ffi_i(function, err_p, *args):
    """
    Helper for executing a dylib function which returns an int error code.

    An error is assumed to have occurred if the returned value is nonzero.
    """
    function.restype = c_int
    code = c_int(function(*args, err_p))
    if code.value != 0:
        raise RuntimeError(_get_err_str(err_p))


def _run_ffi(function, restype, *args):
    """
    Helper for executing a dylib function which returns an arbitrary type.
    """
    function.restype = restype
    return restype(function(*args))


def _get_err_str(err_p):
    """
    Helper for decoding the current error string.
    """
    err_str_p = _run_ffi(
        ffi.ffi_get_err_str_p,
        c_void_p,
        err_p)
    output = _decode_c_str(err_str_p)
    ffi.ffi_drop_c_str(err_str_p)
    return output


def _get_err_p():
    """
    Helper for requesting a new location for error strings to be stored.
    """
    ffi.ffi_get_err_p.restype = c_void_p
    return c_void_p(ffi.ffi_get_err_p())
