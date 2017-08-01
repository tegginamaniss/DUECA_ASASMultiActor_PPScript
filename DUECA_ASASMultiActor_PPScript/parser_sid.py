import re
import os, glob
import dataset
import pprint

pp = pprint.PrettyPrinter(indent=4)
db = dataset.connect('sqlite:///:memory:')

table = db.create_table('Log Files Register', primary_id='name', primary_type='String')
table.insert(dict(name='John Doe', age=37))
table.insert(dict(name='Jane Doe', age=34, gender='female'))

table2 = db['Scenario Data']
table2.insert(dict(name='Siddarth Tegginamani', age=25, gender='male', degree='MSc'))


# john = table.distinct(name='John Doe', age=34)
# print(john)

for it in db['Log File Info']['name']:
    pp.pprint(it)

pp.pprint(db.tables)

print(db['Log Files Register'].columns)

print(db['Scenario Data'].columns)

# table.drop_column('id')
#
# print(table.columns)
