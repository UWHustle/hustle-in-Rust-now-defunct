from hustle import Relation
import numpy as np

relation = Relation.create(['a', 'b'], ['int', 'int'])
relation.insert([1, 2])
print(relation.get_name())
relation.print()

array = relation.to_numpy()
array = np.resize(array, 3)
array[1] = (3, 4)
array[2] = (4, 5)
print(array.dtype)
print(array)

relation = Relation.from_numpy(array)
print(relation.get_name())
relation.print()

relation.export_hustle('T')
del relation

relation = Relation.create(['c', 'd'], ['int', 'int'])
relation.import_hustle('T')
print(relation.get_name())
relation.print()

relation.export_csv('T.csv')
del relation

relation = Relation.create(['e', 'f'], ['int', 'int'])
relation.import_csv('T.csv')
print(relation.get_name())
relation.print()

sum_e = relation.aggregate('e', [], 'count')
print(sum_e.get_name())
sum_e.print()

other = Relation.create(['g'], ['long'])
other.insert([1234])
other.insert([5678])
joined = relation.join(other)
print(joined.get_name())
joined.print()

limited = relation.limit(2)
print(limited.get_name())
limited.print()

projected = relation.project(['e'])
print(projected.get_name())
projected.print()

selected = relation.select('e >= 3')
print(selected.get_name())
selected.print()