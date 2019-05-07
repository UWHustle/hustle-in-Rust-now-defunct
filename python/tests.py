from hustle import *
import numpy
import os
import unittest

connection = HustleConnection()


class HustlePythonTests(unittest.TestCase):
    def setUp(self):
        self.test_relation = connection.create_relation(
            ['a', 'b', 'c', 'd'],
            ['int', 'bigint', 'real', 'varchar(10)'])
        self.test_relation.insert(1, 10000, 10.2, 'Hello, ')
        self.test_relation.insert(9, 55, 11.3, 'world')
        self.test_relation.insert(11, 55, 15.7, '.')

    def test_create_empty(self):
        relation = connection.create_relation([], [])
        self.assertEqual([], relation.get_col_names())
        self.assertEqual([], relation.get_type_names())
        self.assertTrue(relation.get_name() is not None)

    def test_create_single_attr(self):
        col_names = ['a']
        type_names = ['int']
        relation = connection.create_relation(col_names, type_names)
        self.assertEqual(col_names, relation.get_col_names())
        self.assertEqual(type_names, relation.get_type_names())
        self.assertTrue(relation.get_name() is not None)

    def test_create(self):
        col_names = ['a', 'b']
        type_names = ['int', 'int']
        relation = connection.create_relation(col_names, type_names)
        self.assertEqual(col_names, relation.get_col_names())
        self.assertEqual(type_names, relation.get_type_names())
        self.assertTrue(relation.get_name() is not None)

    def test_create_all_types(self):
        col_names = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h']
        type_names = [
            'tinyint',
            'smallint',
            'int',
            'bigint',
            'real',
            'double',
            'varchar(10)',
            'char(10)']
        relation = connection.create_relation(col_names, type_names)
        self.assertEqual(col_names, relation.get_col_names())
        self.assertEqual(type_names, relation.get_type_names())
        self.assertTrue(relation.get_name() is not None)

    def test_from_numpy(self):
        dtype = numpy.dtype([('a', 'int32'), ('b', 'S10')])
        array = numpy.zeros(5, dtype)
        relation = connection.from_numpy(array)
        self.assertEqual(['a', 'b'], relation.get_col_names())
        self.assertEqual(['int', 'varchar(10)'], relation.get_type_names())

    def test_to_numpy(self):
        relation = connection.create_relation(
            ['a', 'b'], ['bigint', 'double'])
        relation.insert(55, 11.8)
        relation.insert(12, 13.3)
        actual = relation.to_numpy()
        expected = numpy.zeros(
            2, numpy.dtype([('a', 'int64'), ('b', 'float64')]))
        expected[0] = (55, 11.8)
        expected[1] = (12, 13.3)
        self.assertTrue(numpy.array_equal(expected, actual))

    def test_to_from_hustle(self):
        expected = self.test_relation.to_numpy()
        self.test_relation.export_hustle('U')
        self.test_relation.import_hustle('U')
        actual = self.test_relation.to_numpy()
        self.assertTrue(numpy.array_equal(expected, actual))

    def test_to_from_csv(self):
        expected = self.test_relation.to_numpy()
        self.test_relation.export_csv('U.csv')
        self.test_relation.import_csv('U.csv')
        actual = self.test_relation.to_numpy()
        self.assertTrue(numpy.array_equal(expected, actual))
        self.assertTrue(os.path.isfile('U.csv'))
        self.assertTrue(os.path.getsize('U.csv') > 0)

    def test_simple_aggregate(self):
        aggregate = self.test_relation.aggregate('a', [], 'avg')
        array = aggregate.to_numpy()
        self.assertEqual(7.0, array[0][0])

    # TODO: There are issues with the ordering of values and columns
    def test_grouped_aggregate(self):
        aggregate = self.test_relation.aggregate('a', ['b'], 'avg')
        array = aggregate.to_numpy()
        self.assertTrue(1.0 in array[:][0] or 1.0 in array[:][1])
        self.assertTrue(10.0 in array[:][0] or 10.0 in array[:][1])

    # TODO: Hustle doesn't guarantee unique column names
    def test_cartesian(self):
        other_relation = connection.create_relation(['e'], ['int'])
        other_relation.insert(1)
        other_relation.insert(2)
        other_relation.insert(3)
        other_relation.insert(4)
        array_join = self.test_relation.join(other_relation, [], []).to_numpy()
        array_l = self.test_relation.to_numpy()
        array_r = other_relation.to_numpy()
        self.assertEqual(len(array_l) * len(array_r), len(array_join))

    def test_equijoin(self):
        other_relation = connection.create_relation(
            ['e', 'f'], ['int', 'varchar(10)'])
        other_relation.insert(1, 'some text')
        other_relation.insert(11, 'more text')
        array_join = self.test_relation.join(
            other_relation, ['a'], ['e']).to_numpy()
        self.assertEqual(2, len(array_join))

    def test_limit(self):
        array_limited = self.test_relation.limit(2).to_numpy()
        self.assertEqual(2, len(array_limited))

    def test_project(self):
        projected = self.test_relation.project('a', 'b')
        self.assertEqual(['a', 'b'], projected.get_col_names())
        self.assertEqual(['int', 'bigint'], projected.get_type_names())
        self.assertEqual(
            len(self.test_relation.to_numpy()),
            len(projected.to_numpy()))

    # TODO: Currently doesn't work when comparing against strings with spaces
    def test_select(self):
        array_str_eq = self.test_relation.select('d = world').to_numpy()
        self.assertEqual(9, array_str_eq[0][0])
        self.assertEqual(1, len(array_str_eq))
        array_int_leq = self.test_relation.select('a <= 9').to_numpy()
        self.assertEqual(10000, array_int_leq[0][1])
        self.assertEqual(55, array_int_leq[1][1])


if __name__ == '__main__':
    unittest.main()
