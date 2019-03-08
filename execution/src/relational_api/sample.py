from python_api import Relation

relation = Relation.create(['a', 'b'], ['int', 'int'])
relation.import_hustle('T')
print('Relation name: ' + relation.get_name())
relation.print()

print('Sum of a grouped by b:')
relation.aggregate('a', ['b'], 'sum').print()

print('T join A:')
join_relation = Relation.create(
    ['w', 'x', 'y', 'z'], ['int', 'int', 'int', 'int'])
join_relation.import_hustle('A')
joined = relation.join(join_relation)
joined.print()

print('T limited to 10:')
relation.limit(10).print()

print('Projection onto a:')
relation.project(['a']).print()

print('T where a < 20:')
relation.select('a < 20').print()

relation.export_hustle('T_copy')
relation.export_csv('test-data/T.csv')