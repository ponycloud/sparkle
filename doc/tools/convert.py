#!/usr/bin/python -tt
import sys
from xml.etree.ElementTree import tostring
from xml.etree import ElementTree
from os import remove
from subprocess import Popen, PIPE

nsUrl = '{http://www.lysator.liu.se/~alla/dia/}'
ElementTree.register_namespace("dia", nsUrl[1:-1])
file_name = sys.argv[1]

def get_attribtue_number(attribute_number):
	if attribute_number % 2 == 1:
		attribute_number = attribute_number - 1
	return (attribute_number - 12) / 2

db = ElementTree.parse(file_name)
root = db.getroot()
tables = {}

render_root = ElementTree.parse(file_name).getroot()
for layer in render_root.findall('{ns}layer'.format(ns=nsUrl)):
   for table in layer.findall('{ns}object'.format(ns=nsUrl)):
       layer.remove(table)


for table in root.findall('{ns}layer/{ns}object[@type="Database - Table"]'.format(ns=nsUrl)):
	table_id = table.attrib['id']
	table_name = table.find('{ns}attribute[@name="name"]/{ns}string'.format(ns=nsUrl)).text.strip()[1:-1]
	table_comment = table.find('{ns}attribute[@name="comment"]/{ns}string'.format(ns=nsUrl)).text.strip()[1:-1]

	render_root.find('{ns}layer'.format(ns=nsUrl)).append(table)
	tmp_file_name = "{render_table}.tmp.dia".format(render_table=table_name)
	png_file_name = "{render_table}.table.png".format(render_table=table_name)
	ElementTree.ElementTree(render_root).write(tmp_file_name, "UTF-8", True)
	assert Popen(['dia', '-t', 'png', '-e', png_file_name, tmp_file_name], stdout=PIPE).wait() == 0
	remove(tmp_file_name)
	render_root.find('{ns}layer'.format(ns=nsUrl)).remove(table)

	table_attributes = []
	attribute_order = 0

	for attribute in table.findall('{ns}attribute[@name="attributes"]/{ns}composite[@type="table_attribute"]'.format(ns=nsUrl)):
		data = {}

		for attribute_name in ('name', 'type', 'comment'):
			data[attribute_name] = attribute.find('{ns}attribute[@name="{name}"]/{ns}string'.format(name=attribute_name,ns=nsUrl)).text.strip()[1:-1]

		for attribute_name in ('primary_key', 'nullable', 'unique'):
			data[attribute_name] = attribute.find('{ns}attribute[@name="{name}"]/{ns}boolean'.format(name=attribute_name,ns=nsUrl)).attrib['val'] == "true"

		table_attributes.append(data)
		attribute_order += 2

	tables[table_id] = {
		'id': table_id,
		'name': table_name,
		'comment': table_comment,
		'attributes': table_attributes,
		'referenced_by': []
	}

for references in root.findall('{ns}layer/{ns}object[@type="Database - Reference"]'.format(ns=nsUrl)):
	table_references = references.findall('{ns}connections/{ns}connection'.format(ns=nsUrl))

	"""
		table connection numbers are like this:
		0 1 2 3 4    -> top edge
		5       6	 -> header with table name
		----------
		12		13	 -> attributes
		14		15
		16		...
		----------
		7 8 9 10 11  -> bottom edge
	"""
	# not linked to attribute - incoming reference from different table
	if  int(table_references[0].attrib['connection']) < 12:
		ref_to = table_references[0]
		ref_from = table_references[1]
	# outgoing attribute to different table
	else:
		ref_to = table_references[1]
		ref_from = table_references[0]

	from_attribute_number = get_attribtue_number(int(ref_from.attrib['connection']))

	tables[ref_to.attrib['to']]['referenced_by'].append({
		'table_id': ref_from.attrib['to'],
		'table_name': tables[ref_from.attrib['to']]['name'],
		'attribute_name': tables[ref_from.attrib['to']]['attributes'][from_attribute_number]['name'],
	})

	to_attribute_number = get_attribtue_number(int(ref_from.attrib['connection']))

	tables[ref_from.attrib['to']]['attributes'][to_attribute_number]['referenced_table'] = {
		'table_name': tables[ref_to.attrib['to']]['name'],
		'table_id': tables[ref_to.attrib['to']]['id'],
	}

print '% ' + render_root.find('{ns}layer'.format(ns=nsUrl)).attrib['name']
print ''

for key, table in sorted(tables.iteritems(), key=lambda (key,val): (val['name'],key)):
	print '# <span id="{id}">{name}</span>'.format(name=table['name'], id=key)
	print ''

	print '![{name}]({path})<span></span>'.format(name=table['name'], path=table['name']+'.table.png')
	print ''

	if table['comment']:
		print table['comment']
		print ''

		print '## Columns'
		print ''

	for attribute in table['attributes']:
		cssclasses = ''
		for attribute_property in ('primary_key', 'nullable', 'unique'):
			if attribute[attribute_property] == True:
				cssclasses += ' '+attribute_property
		print '### &lt;{type}&gt; <span id="{id}" class="{cssclasses}">{name}</span>'.format(name=attribute['name'], id=key+attribute['name'], type=attribute['type'], cssclasses=cssclasses)
		print ''

		if attribute['type'][:4] == 'enum':
			enum_type = attribute['comment'].split('\n', 1)

			if len(enum_type) == 2:
				attribute['comment'] = enum_type[1]
			else:
				attribute['comment'] = ''

			print '* Possible values are {values}.'.format(values=', '.join(['`%s`' % x.strip() for x in enum_type[0][1:-1].split(',')]))
			print ''

		if attribute['comment']:
			print attribute['comment']
			print ''

		if 'referenced_table' in  attribute:
			print '* References [{name}](#{id}).'.format(name=attribute['referenced_table']['table_name'], \
						id=attribute['referenced_table']['table_name'])
			print ''

	if table['referenced_by']:
		print '## References from other tables'
		print ''

		for reference in table['referenced_by']:
			print '* [{name}](#{id})'.format(name=reference['table_name']+'.'+reference['attribute_name'], \
											id=reference['table_id']+reference['attribute_name'])
			print ''
