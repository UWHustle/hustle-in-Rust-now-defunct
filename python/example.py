from hustle import *
import numpy as np

connection = HustleConnection()
relation = connection.create_relation(['a', 'b'], ['int', 'int'])
relation.insert(1, 2)
print(relation.get_name())
relation.print()

array = relation.to_numpy()
array = np.resize(array, 3)
array[1] = (3, 4)
array[2] = (4, 5)
print(array.dtype)
print(array)

relation = connection.from_numpy(array)
print(relation.get_name())
relation.print()

relation.export_hustle('T')
del relation

relation = connection.create_relation(['c', 'd'], ['int', 'int'])
relation.import_hustle('T')
print(relation.get_name())
relation.print()

relation.export_csv('T.csv')
del relation

relation = connection.create_relation(['e', 'f'], ['int', 'int'])
relation.import_csv('T.csv')
print(relation.get_name())
relation.print()

count_e = relation.aggregate('e', [], 'count')
print(count_e.get_name())
count_e.print()

other = connection.create_relation(['g'], ['long'])
other.insert(1234)
other.insert(5678)
joined = relation.join(other)
print(joined.get_name())
joined.print()

limited = relation.limit(2)
print(limited.get_name())
limited.print()

projected = relation.project('e')
print(projected.get_name())
projected.print()

selected = relation.select('e >= 3')
print(selected.get_name())
selected.print()