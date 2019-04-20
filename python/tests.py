from hustle import Relation
import unittest


class CreateTestCase(unittest.TestCase):
    def test_create_single_attr(self):
        relation = Relation.create(['a', 'b'], ['int', 'int'])
        self.assertEquals(len(relation.get_col_names()), 2)

    def test_fail(self):
        self.fail('Supposed to fail')


if __name__ == '__main__':
    unittest.main()
