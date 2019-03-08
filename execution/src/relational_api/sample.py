from python_api import Relation

relation = Relation.create(['a', 'b'], ['int', 'int'])
print(relation.get_name())
del relation
