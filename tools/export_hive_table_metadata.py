from solr_client import SolrServer, SolrCore
import csv
import operator
import requests, json
import sys


USAGE = """
Usage: export_hive_table_metadata hostname port user password csvfilename
e.g.,
    localhost 7187 user password sample_data.csv
"""

def return_value_if_key_exists(keyname, customProperties):
	keys = customProperties.keys()
	if keyname in keys:
		return customProperties.get(keyname)
	else:
		return ""

def return_custom_properties(customProperties, main_map, prefix, index = 0):
	customProp = {}
	for property in customProperties:
		customProp[prefix + property[index:]] = return_value_if_key_exists(property.split('.', 1)[-1], main_map)
	return customProp

def get_managed_properties(hostname, port, class_name):
	url = "http://" + hostname + ":" + str(port) + "/api/v9/models/packages/nav/classes/" + class_name + "/properties"
	headers = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8'}
	r = requests.get(url, headers=headers)
	properties = r.json()
	propertiesList = []
	for property in properties:
		property_name = property.get("type") + "." + property.get("namespace") + "." + property.get("name")
		propertiesList.append(property_name)
	return propertiesList

def get_managed_properties_without_type(hostname, port, class_name):
	url = "http://" + hostname + ":" + str(port) + "/api/v9/models/packages/nav/classes/" + class_name + "/properties"
	headers = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8'}
	r = requests.get(url, headers=headers)
	properties = r.json()
	propertiesList = []
	for property in properties:
		property_name =  property.get("namespace") + "." + property.get("name")
		propertiesList.append(property_name)
	return propertiesList

def get_all_custom_properties_keys(core):
	params = {}
	params['fl'] = "up_*"
	props = []
	for x in core.get_docs(q='sourceType:HIVE AND type:TABLE', sort='identity asc', params=params):
		if len(x) > 0:
			props = props + x.keys()
	return props

if __name__ == "__main__":
	if len(sys.argv) < 5:
		sys.exit(USAGE)
	hostname = sys.argv[1]
	port = sys.argv[2]
	username = sys.argv[3]
	password = sys.argv[4]
	csvfile_name = sys.argv[5]
	server = SolrServer(hostname, port, username, password)
	core = SolrCore(server, "nav_elements")
	managed_properties = get_managed_properties(hostname, port, "hv_table")
	managed = ",".join(managed_properties)
	params = {}
	custom_properties_headers = get_all_custom_properties_keys(core)
	headers1 = ['sourceType', 'type', 'parentPath', 'originalName'] 
	custom_properties_headers_modified = ["CM." + s[3:] for s in custom_properties_headers]
	managed_properties_headers = ["MM." + s for s in managed_properties]
	headers2 = ['name', 'description', 'tags']

	headers = headers1 + headers2 + custom_properties_headers_modified + managed_properties_headers
	params['fl'] = ",".join(headers1 + headers2 + custom_properties_headers + get_managed_properties_without_type(hostname, port, "hv_table"))

	with open(csvfile_name, 'wb') as csvfile:
		csvwriter = csv.DictWriter(csvfile, delimiter=',', quoting=csv.QUOTE_MINIMAL, fieldnames = headers)
		csvwriter.writeheader()
		for x in core.get_docs(q='sourceType:HIVE AND type:TABLE', sort='identity asc', params=params):
			name = return_value_if_key_exists("name", x)
			description = return_value_if_key_exists("description", x)
			tags = return_value_if_key_exists("tags", x)
			tags = ','.join(tags)
			temp = {}
			for header in headers1:
				temp[header] = x[header]
			temp['name'] = name
			temp['description'] = description
			temp['tags'] = tags
			customProperties = return_custom_properties(custom_properties_headers, x, "CM.", 3)
			temp = dict(temp, **customProperties)
			managedprop = return_custom_properties(managed_properties, x, "MM.")
			temp = dict(temp, **managedprop)
			csvwriter.writerow(temp)