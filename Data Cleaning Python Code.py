import xml.etree.ElementTree as ET
import csv
import codecs
import pprint
import re
from collections import defaultdict


OSM_FILE = "Biloxi"


# List File Sizes
import os

def padStr( x, n ):
    '''Python print format with dot leader
    Reference:
    http://stackoverflow.com/questions/28588316/python-print-format-with-dot-leader'''  
    
    x += ' '
    return x + '.'*(n - len(x) )

folder = 'C:\Users\keela\OneDrive\Desktop\OSM Project'
file_size = 0
for (path, dirs, files) in os.walk(folder):
    for file in files:
        filename = os.path.join(path,file)
        file_size = os.path.getsize(filename)
        print('%s %0.2f MB' %( padStr(file, 50), ((file_size)/(1024*1024.0))))


# Function lists types and number of tags in file (Iterative Parsing Quiz)
def count_tags(filename):
        tags = {}
        for event, element in ET.iterparse(filename):
           
            if element.tag not in tags.keys():
                tags[element.tag] = 1
            
            else:
                tags[element.tag] += 1
        
        return tags


# Prints list and count of tags
pprint.pprint(count_tags(OSM_FILE))


# Checks for problems in tag k values (Tag Types Quiz)

lower = re.compile(r'^([a-z]|_)*$')
lower_colon = re.compile(r'^([a-z]|_)*:([a-z]|_)*$')
problemchars = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')

def key_type(element, keys):
    if element.tag == "tag":
        k = element.attrib['k']
        if re.search(lower, k):
            keys['lower'] += 1
        elif re.search(lower_colon, k):
            keys['lower_colon'] += 1
        elif re.search(problemchars, k):
            keys['problemchars'] += 1
        else:
            keys['other'] +=1
        pass
        
    return keys

def keys_map(filename):
    keys = {"lower": 0, "lower_colon": 0, "problemchars": 0, "other": 0}
    for _, element in ET.iterparse(filename):
        keys = key_type(element, keys)

    return keys


# Prints number of problematic "k" values in different categories
pprint.pprint(keys_map(OSM_FILE))


# Returns set of unqiue user_ids (Exploring Users Quiz)

def users_map(filename):
    users = set()
    
    for _, element in ET.iterparse(filename):
        
        if element.get('uid'):
            id = element.attrib['uid']
            users.add(id)

    return users

print len(users_map(OSM_FILE))


# Extracts Elements from File
def get_element(osm_file, tags=('node', 'way', 'relation')):
    
    context = iter(ET.iterparse(osm_file, events=('start', 'end')))
    _, root = next(context)
    
    for event, elem in context:
        
        if event == 'end' and elem.tag in tags:
            
            yield elem
            root.clear()

# Creates Dict of keys with sets of values
def waynode_kv(osmfile):
    
    osm_file = open(osmfile, "r")
    unique_kv = defaultdict(set)
    
    for i, element in enumerate(get_element(osm_file)):
        
        if element.tag == "node" or element.tag == "way":                         
            
            for tag in element.iter("tag"):        
                unique_kv[tag.attrib['k']].add(tag.attrib['v'])    
    
    return unique_kv

# Prints previously created Dict
pprint.pprint(waynode_kv(OSM_FILE))


# Audit Street Types
street_type_re = re.compile(r'\b\S+\.?$', re.IGNORECASE)

street_expected = ["Street", "Avenue", "Boulevard", "Drive", "Court", "Circle", "Place", "Lane", "Road", 
            "Trail", "Parkway", "Alley", "Cove", "Way", "Creek"]

def audit_street_type(street_types, street_name):
    m = street_type_re.search(street_name)
    if m:
        street_type = m.group()
        if street_type not in street_expected:
            street_types[street_type].add(street_name)

def is_street_name(elem):
    return (elem.attrib['k'] == "addr:street")

def audit(osmfile):
    osm_file = open(osmfile, "r")
    street_types = defaultdict(set)
    for event, elem in ET.iterparse(osm_file, events=("start",)):

        if elem.tag == "node" or elem.tag == "way":
            for tag in elem.iter("tag"):
                if is_street_name(tag):
                    audit_street_type(street_types, tag.attrib['v'])
    osm_file.close()
    return street_types

audit(OSM_FILE)


# Update Street Types
streetmapping = { "Rr": "Road",
            "Blvd": "Boulevard",
            "Pkwy": "Parkway",
            "Pointe": "Point"
            }

def update_name_street(name, streetmapping):
    
    m = street_type_re.search(name)
    street_type = m.group()
    
    if street_type in streetmapping.keys():
            name = re.sub(m.group(), streetmapping[m.group()],name)
    
    return name

for event, element in ET.iterparse(OSM_FILE, events=("start",)):
    if element.tag == "node" or element.tag == "way":
        for tag in element.iter("tag"):
            if tag.attrib['k'] == "addr:street":
                tag.attrib['v'] = update_name_street(tag.attrib['v'], streetmapping)


# Audit Zip Codes
def is_zip_code(elem):
    return (elem.attrib['k'] == "addr:postcode")

def audit_zips(osmfile):

    osm_file = open(osmfile, "r")
    zip_codes = {}
    for event, elem in ET.iterparse(osm_file, events=("start",)):
        if elem.tag == "node" or elem.tag == "way":
            for tag in elem.iter("tag"):
                if is_zip_code(tag):                    
                        if tag.attrib['v'] not in zip_codes:
                            zip_codes[tag.attrib['v']] = 1
                        else:
                            zip_codes[tag.attrib['v']] += 1
    return zip_codes

audit_zips(OSM_FILE)


# Update Zip Codes
def update_zips(name):
    if len(name) > 5: #take only first 5 numbers of zip
        print name, 'updated to:'
        
        if re.search('[0-9]{5}', name):
            updated_name = re.findall('[0-9]{5}', name)
            name = updated_name[0]
            print name
            return name
    
    else:
        return name
       
for event, element in ET.iterparse(OSM_FILE, events=("start",)):
    if element.tag == "node" or element.tag == "way":
        for tag in element.iter("tag"):
            if tag.attrib['k'] == "addr:postcode":
                tag.attrib['v'] = update_zips(tag.attrib['v'])


# Audit City Names
def is_city(elem):
    return (elem.attrib['k'] == "addr:city")

def audit_city(osmfile):

    osm_file = open(osmfile, "r")
    cities = {}
    for event, elem in ET.iterparse(osm_file, events=("start",)):
        if elem.tag == "node" or elem.tag == "way":
            for tag in elem.iter("tag"):
                if is_city(tag):
                    if tag.attrib['v'] not in cities:
                        cities[tag.attrib['v']] = 1
                    else:
                        cities[tag.attrib['v']] += 1
    return cities

audit_city(OSM_FILE)


# Update City Names
citymapping = { "Diberville": "D'Iberville",
                "Keesler Air Force Base": "Biloxi" }            
                
def update_name_city(name, citymapping):
    if name in citymapping:
        print name, "updated to:"
        name = citymapping[name] 
        print name
        return name

    else:
        return name

for event, element in ET.iterparse(OSM_FILE, events=("start",)):
    if element.tag == "node" or element.tag == "way":
        for tag in element.iter("tag"):
            if tag.attrib['k'] == "addr:city":
                tag.attrib['v'] = update_name_city(tag.attrib['v'], citymapping)


#Audit Cuisine
def is_cuisine(elem):
    return (elem.attrib['k'] == "cuisine")

def audit_cuisine(osmfile):

    osm_file = open(osmfile, "r")
    cuisines = {}
    for event, elem in ET.iterparse(osm_file, events=("start",)):
        if elem.tag == "node" or elem.tag == "way":
            for tag in elem.iter("tag"):
                if is_cuisine(tag):
                        if tag.attrib['v'] not in cuisines:
                            cuisines[tag.attrib['v']] = 1
                        else:
                            cuisines[tag.attrib['v']] += 1
                        
    return cuisines

audit_cuisine(OSM_FILE)


# Update Cuisine
cuisinemapping = { "america": "american",
                  "chicken;american": "american",
                  "chinese": "asian",
                  "japanese": "asian",
                  "vietnamese": "asian"
            }

               

def update_name_cuisine(name, cuisinemapping):
    if name in cuisinemapping:
        print name, 'updated to:'
        name = cuisinemapping[name]
        print name
        return name

    else:
        return name

for event, element in ET.iterparse(OSM_FILE, events=("start",)):
    if element.tag == "node" or element.tag == "way":
        for tag in element.iter("tag"):
            if tag.attrib['k'] == "cuisine":
                tag.attrib['v'] = update_name_cuisine(tag.attrib['v'], cuisinemapping)


#############################################################
#                       Main Project                        #
#############################################################

import cerberus
import xml.etree.cElementTree as ET
import csv
import codecs
import pprint
import re
from collections import defaultdict

OSM_PATH = "Biloxi"

NODES_PATH = "nodes.csv"
NODE_TAGS_PATH = "nodes_tags.csv"
WAYS_PATH = "ways.csv"
WAY_NODES_PATH = "ways_nodes.csv"
WAY_TAGS_PATH = "ways_tags.csv"

LOWER_COLON = re.compile(r'^([a-z]|_)+:([a-z]|_)+')
PROBLEMCHARS = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')

SCHEMA = {
    'node': {
        'type': 'dict',
        'schema': {
            'id': {'required': True, 'type': 'integer', 'coerce': int},
            'lat': {'required': True, 'type': 'float', 'coerce': float},
            'lon': {'required': True, 'type': 'float', 'coerce': float},
            'user': {'required': True, 'type': 'string'},
            'uid': {'required': True, 'type': 'integer', 'coerce': int},
            'version': {'required': True, 'type': 'string'},
            'changeset': {'required': True, 'type': 'integer', 'coerce': int},
            'timestamp': {'required': True, 'type': 'string'}
        }
    },
    'node_tags': {
        'type': 'list',
        'schema': {
            'type': 'dict',
            'schema': {
                'id': {'required': True, 'type': 'integer', 'coerce': int},
                'key': {'required': True, 'type': 'string'},
                'value': {'required': True, 'type': 'string'},
                'type': {'required': True, 'type': 'string'}
            }
        }
    },
    'way': {
        'type': 'dict',
        'schema': {
            'id': {'required': True, 'type': 'integer', 'coerce': int},
            'user': {'required': True, 'type': 'string'},
            'uid': {'required': True, 'type': 'integer', 'coerce': int},
            'version': {'required': True, 'type': 'string'},
            'changeset': {'required': True, 'type': 'integer', 'coerce': int},
            'timestamp': {'required': True, 'type': 'string'}
        }
    },
    'way_nodes': {
        'type': 'list',
        'schema': {
            'type': 'dict',
            'schema': {
                'id': {'required': True, 'type': 'integer', 'coerce': int},
                'node_id': {'required': True, 'type': 'integer', 'coerce': int},
                'position': {'required': True, 'type': 'integer', 'coerce': int}
            }
        }
    },
    'way_tags': {
        'type': 'list',
        'schema': {
            'type': 'dict',
            'schema': {
                'id': {'required': True, 'type': 'integer', 'coerce': int},
                'key': {'required': True, 'type': 'string'},
                'value': {'required': True, 'type': 'string'},
                'type': {'required': True, 'type': 'string'}
            }
        }
    }
}

# Make sure the fields order in the csvs matches the column order in the sql table schema
NODE_FIELDS = ['id', 'lat', 'lon', 'user', 'uid', 'version', 'changeset', 'timestamp']
NODE_TAGS_FIELDS = ['id', 'key', 'value', 'type']
WAY_FIELDS = ['id', 'user', 'uid', 'version', 'changeset', 'timestamp']
WAY_TAGS_FIELDS = ['id', 'key', 'value', 'type']
WAY_NODES_FIELDS = ['id', 'node_id', 'position']


#variable for shape_element
street_type_re = re.compile(r'\b\S+\.?$', re.IGNORECASE)

# ================================================== #
#               Shape_Element Mapping                #
# ================================================== #

streetmapping = { "Rr": "Road",
            "Blvd": "Boulevard",
            "Pkwy": "Parkway",
            "Pointe": "Point"
            }

citymapping = { "Diberville": "D'Iberville",
                "Keesler Air Force Base": "Biloxi" }   

cuisinemapping = { "america": "american",
                  "chicken;american": "american",
                  "chinese": "asian",
                  "japanese": "asian",
                  "vietnamese": "asian"
            }


# ================================================== #
#               Shape_Element Functions              #
# ================================================== #

def update_name_street(name, streetmapping):
    
    m = street_type_re.search(name)
    street_type = m.group()
    
    if street_type in streetmapping.keys():
            name = re.sub(m.group(), streetmapping[m.group()],name)
    
    return name


def update_zips(name):
    if len(name) > 5: #take only first 5 numbers of zip
        print name, 'updated to:'
        
        if re.search('[0-9]{5}', name):
            updated_name = re.findall('[0-9]{5}', name)
            name = updated_name[0]
            print name
            return name
    
    else:
        return name
    

def update_name_city(name, citymapping):
    if name in citymapping:
        print name, "updated to:"
        name = citymapping[name] 
        print name
        return name

    else:
        return name


def update_name_cuisine(name, cuisinemapping):
    if name in cuisinemapping:
        print name, 'updated to:'
        name = cuisinemapping[name]
        print name
        return name

    else:
        return name

def right_key(k):
    index = k.find(':')
    types = k[:index]
    k = k[index + 1:]
    return k,types


def shape_element(element, node_attr_fields=NODE_FIELDS, way_attr_fields=WAY_FIELDS,
                  problem_chars=PROBLEMCHARS, default_tag_type='regular'):
    """Clean and shape node or way XML element to Python dict"""

    node_atts = {}
    way_atts = {}
    way_nodes = []
    tags = []  # Handle secondary tags the same way for both node and way elements

    if element.tag == 'node': #fill dictionary with k/v pairs from NODE_FIELDS
        for i in node_attr_fields:
            node_atts[i] = element.attrib[i]

    if element.tag == 'way':
        for i in way_attr_fields:
            way_atts[i] = element.attrib[i]

    for tag in element.iter("tag"): #loop through tags looking for problem values
        dic = {}
        attributes = tag.attrib
        if tag.attrib['k'] == "addr:street":
            tag.attrib['v'] = update_name_street(tag.attrib['v'], streetmapping)
        elif tag.attrib['k'] == "addr:city":
            tag.attrib['v'] = update_name_city(tag.attrib['v'], citymapping)
        elif tag.attrib['k'] == "addr:postcode":
            tag.attrib['v'] = update_zips(tag.attrib['v'])
        elif tag.attrib['k'] == "cuisine":
            tag.attrib['v'] = update_name_cuisine(tag.attrib['v'], cuisinemapping)
               
        if problem_chars.search(tag.attrib['k']):
            continue

        if element.tag == 'node': #add node id for attributes
            dic['id'] = node_atts['id']
        else:
            dic['id'] = way_atts['id'] #add way id for attributes

        dic['value'] = attributes['v'] #value of key for each type

        colon_k=LOWER_COLON.search(tag.attrib['k'])
        
        if colon_k:
            #print colon_k.group(0)
            #print tag.attrib['k']
            dic['key'],dic['type'] = right_key(tag.attrib['k']) #call function to split at colon
        else:
            dic['key'] = attributes['k'] #assign regular that there was no colon problem
            dic['type'] = 'regular'

        tags.append(dic)

    if element.tag == 'way':
        position = 0
        for nd in element.iter("nd"): #loop through nd child tags numbering them
            way_node_dic = {}
            way_node_dic['id'] = way_atts['id']
            way_node_dic['node_id'] = nd.attrib['ref']
            way_node_dic['position'] = position
            position = position + 1
            way_nodes.append(way_node_dic)
    
    
    
    if element.tag == 'node':       #process the above for node tags for final formatting
        return {'node': node_atts, 'node_tags': tags}

    elif element.tag == 'way':      #process the above for way tags for final formatting
        return {'way': way_atts, 'way_nodes': way_nodes, 'way_tags': tags}


# ================================================== #
#               Helper Functions                     #
# ================================================== #
def get_element(osm_file, tags=('node', 'way', 'relation')):
    #Yield element if it is the right type of tag

    context = ET.iterparse(osm_file, events=('start', 'end'))
    _, root = next(context)
    for event, elem in context:
        if event == 'end' and elem.tag in tags:
            yield elem
            root.clear()


def validate_element(element, validator, schema=SCHEMA):
    
    #Raise ValidationError if element does not match schema
    if validator.validate(element, schema) is not True:
        field, errors = next(validator.errors.iteritems())
        message_string = "\nElement of type '{0}' has the following errors:\n{1}"
        error_string = pprint.pformat(errors)
        
        raise Exception(message_string.format(field, error_string))


#Extend csv.DictWriter to handle Unicode input
class UnicodeDictWriter(csv.DictWriter, object):

    def writerow(self, row):
        super(UnicodeDictWriter, self).writerow({
            k: (v.encode('utf-8') if isinstance(v, unicode) else v) for k, v in row.iteritems()
        })

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)


# ================================================== #
#               Main Function                        #
# ================================================== #

#Iteratively process each XML element and write to csv(s)
def process_map(file_in, validate):
    
    with codecs.open(NODES_PATH, 'w') as nodes_file, \
         codecs.open(NODE_TAGS_PATH, 'w') as nodes_tags_file, \
         codecs.open(WAYS_PATH, 'w') as ways_file, \
         codecs.open(WAY_NODES_PATH, 'w') as way_nodes_file, \
         codecs.open(WAY_TAGS_PATH, 'w') as way_tags_file:

        nodes_writer = UnicodeDictWriter(nodes_file, NODE_FIELDS)
        node_tags_writer = UnicodeDictWriter(nodes_tags_file, NODE_TAGS_FIELDS)
        ways_writer = UnicodeDictWriter(ways_file, WAY_FIELDS)
        way_nodes_writer = UnicodeDictWriter(way_nodes_file, WAY_NODES_FIELDS)
        way_tags_writer = UnicodeDictWriter(way_tags_file, WAY_TAGS_FIELDS)

        nodes_writer.writeheader()
        node_tags_writer.writeheader()
        ways_writer.writeheader()
        way_nodes_writer.writeheader()
        way_tags_writer.writeheader()

        validator = cerberus.Validator()

        for element in get_element(file_in, tags=('node', 'way')):
            el = shape_element(element)
            if el:
                if validate is True:
                    validate_element(el, validator)

                if element.tag == 'node':
                    nodes_writer.writerow(el['node'])
                    node_tags_writer.writerows(el['node_tags'])
                elif element.tag == 'way':
                    ways_writer.writerow(el['way'])
                    way_nodes_writer.writerows(el['way_nodes'])
                    way_tags_writer.writerows(el['way_tags'])

if __name__ == '__main__':
    process_map(OSM_PATH, validate=True)


# ================================================== #
#                 SQL Queries                        #
# ================================================== #


import sqlite3
import csv
from pprint import pprint

sqlite_file = 'OSMData.db'

# Connect to the database
conn = sqlite3.connect(sqlite_file)

# Get a cursor object
cur = conn.cursor()

# Before you (re)create the table, you will have to drop the table if it already exists: 
cur.execute('DROP TABLE IF EXISTS nodes')
conn.commit()

# Create the table, specifying the column names and data types:

cur.execute('''
    CREATE TABLE nodes (
    id INTEGER PRIMARY KEY NOT NULL,
    lat REAL,
    lon REAL,
    user TEXT,
    uid INTEGER,
    version INTEGER,
    changeset INTEGER,
    timestamp TEXT)
''')

# commit the changes
conn.commit()

#Read in the csv file as a dictionary, format the
# data as a list of tuples:
with open('nodes.csv','rb') as fin:
    dr = csv.DictReader(fin) # comma is default delimiter
    to_db = [(i['id'].decode("utf-8"), i['lat'].decode("utf-8"),i['lon'].decode("utf-8"), i['user'].decode("utf-8"), 
              i['uid'].decode("utf-8"), i['version'].decode("utf-8"), i['changeset'].decode("utf-8"), 
              i['timestamp'].decode("utf-8")) for i in dr]
    
# insert the formatted data
cur.executemany("INSERT INTO nodes(id, lat, lon, user, uid, version, changeset, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?);", to_db)

# commit the changes
conn.commit()

# close the connection
conn.close()


### IMPORT NODES_TAGS

sqlite_file = 'OSMData.db'

# Connect to the database
conn = sqlite3.connect(sqlite_file)

# Get a cursor object
cur = conn.cursor()

# Before you (re)create the table, you will have to drop the table if it already exists: 
cur.execute('DROP TABLE IF EXISTS nodes_tags')
conn.commit()

# Create the table, specifying the column names and data types:
cur.execute('''
    CREATE TABLE nodes_tags (
    id INTEGER,
    key TEXT,
    value TEXT,
    type TEXT,
    FOREIGN KEY (id) REFERENCES nodes(id))
''')

# commit the changes
conn.commit()

#Read in the csv file as a dictionary, format the
# data as a list of tuples:
with open('nodes_tags.csv','rb') as fin:
    dr = csv.DictReader(fin) # comma is default delimiter
    to_db = [(i['id'].decode("utf-8"), i['key'].decode("utf-8"),i['value'].decode("utf-8"), i['type'].decode("utf-8")) for i in dr]
    
# insert the formatted data
cur.executemany("INSERT INTO nodes_tags(id, key, value,type) VALUES (?, ?, ?, ?);", to_db)

# commit the changes
conn.commit()

# close the connection
conn.close()


### IMPORT WAYS

sqlite_file = 'OSMData.db'

# Connect to the database
conn = sqlite3.connect(sqlite_file)

# Get a cursor object
cur = conn.cursor()

# Before you (re)create the table, you will have to drop the table if it already exists: 
cur.execute('DROP TABLE IF EXISTS ways')
conn.commit()

# Create the table, specifying the column names and data types:
cur.execute('''
    CREATE TABLE ways (
    id INTEGER PRIMARY KEY NOT NULL,
    user TEXT,
    uid INTEGER,
    version TEXT,
    changeset INTEGER,
    timestamp TEXT)
''')

# commit the changes
conn.commit()

#Read in the csv file as a dictionary, format the
# data as a list of tuples:
with open('ways.csv','rb') as fin:
    dr = csv.DictReader(fin) # comma is default delimiter
    to_db = [(i['id'].decode("utf-8"), i['user'].decode("utf-8"),i['uid'].decode("utf-8"), i['version'].decode("utf-8"),
             i['changeset'].decode("utf-8"), i['timestamp'].decode("utf-8")) for i in dr]
    
# insert the formatted data
cur.executemany("INSERT INTO ways(id, user, uid, version, changeset, timestamp) VALUES (?, ?, ?, ?, ?, ?);", to_db)

# commit the changes
conn.commit()

# close the connection
conn.close()


### IMPORT WAYS_TAGS

sqlite_file = 'OSMData.db'

# Connect to the database
conn = sqlite3.connect(sqlite_file)

# Get a cursor object
cur = conn.cursor()

# Before you (re)create the table, you will have to drop the table if it already exists: 
cur.execute('DROP TABLE IF EXISTS ways_tags')
conn.commit()

# Create the table, specifying the column names and data types:
cur.execute('''
    CREATE TABLE ways_tags (
    id INTEGER NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    type TEXT,
    FOREIGN KEY (id) REFERENCES ways(id)
)
''')

# commit the changes
conn.commit()

#Read in the csv file as a dictionary, format the
# data as a list of tuples:
with open('ways_tags.csv','rb') as fin:
    dr = csv.DictReader(fin) # comma is default delimiter
    to_db = [(i['id'].decode("utf-8"), i['key'].decode("utf-8"),i['value'].decode("utf-8"), i['type'].decode("utf-8")) for i in dr]
    
# insert the formatted data
cur.executemany("INSERT INTO ways_tags(id, key, value,type) VALUES (?, ?, ?, ?);", to_db)

# commit the changes
conn.commit()

# close the connection
conn.close()


### IMPORT WAYS_NODES

sqlite_file = 'OSMData.db'

# Connect to the database
conn = sqlite3.connect(sqlite_file)

# Get a cursor object
cur = conn.cursor()

# Before you (re)create the table, you will have to drop the table if it already exists: 
cur.execute('DROP TABLE IF EXISTS ways_nodes')
conn.commit()

# Create the table, specifying the column names and data types:
cur.execute('''
    CREATE TABLE ways_nodes (
    id INTEGER NOT NULL,
    node_id INTEGER NOT NULL,
    position INTEGER NOT NULL,
    FOREIGN KEY (id) REFERENCES ways(id),
    FOREIGN KEY (node_id) REFERENCES nodes(id))
''')

# commit the changes
conn.commit()

#Read in the csv file as a dictionary, format the
# data as a list of tuples:
with open('ways_nodes.csv','rb') as fin:
    dr = csv.DictReader(fin) # comma is default delimiter
    to_db = [(i['id'].decode("utf-8"), i['node_id'].decode("utf-8"),i['position'].decode("utf-8")) for i in dr]
    
# insert the formatted data
cur.executemany("INSERT INTO ways_nodes(id, node_id, position) VALUES (?, ?, ?);", to_db)

# commit the changes
conn.commit()

# close the connection
conn.close()


# Number of Nodes
conn = sqlite3.connect(sqlite_file)
cursor = conn.cursor()
cursor.execute('''
    SELECT COUNT(*) FROM nodes;
''')
results = cursor.fetchall()
print results
conn.close


# Number of Ways
conn = sqlite3.connect(sqlite_file)
cursor = conn.cursor()
cursor.execute('''
    SELECT COUNT(*) FROM ways;
''')
results = cursor.fetchall()
print results
conn.close


# Number of Unique Users
conn = sqlite3.connect(sqlite_file)
cursor = conn.cursor()
cursor.execute('''
    SELECT Count(DISTINCT both.user)
    FROM (SELECT user FROM nodes
    UNION ALL SELECT user FROM ways) as both;
''')
results = cursor.fetchall()
print results
conn.close


import pandas as pd

# Top 5 Cities
conn = sqlite3.connect(sqlite_file)
cursor = conn.cursor()
cursor.execute('''
    SELECT both.value, COUNT(*) as Total 
    FROM (SELECT * FROM nodes_tags UNION ALL 
    SELECT * FROM ways_tags) as both
    WHERE both.key == 'city'
    GROUP BY both.value
    ORDER BY Total DESC
    LIMIT 10;
''')

results = cursor.fetchall()
df = pd.DataFrame(results)
print df
conn.close


# Top 5 Zip Codes
conn = sqlite3.connect(sqlite_file)
cursor = conn.cursor()
cursor.execute('''
    SELECT both.value, COUNT(*) as Total 
    FROM (SELECT * FROM nodes_tags UNION ALL 
    SELECT * FROM ways_tags) as both
    WHERE both.key == 'postcode'
    GROUP BY both.value
    ORDER BY Total DESC
    LIMIT 5;
''')

results = cursor.fetchall()
df = pd.DataFrame(results)
print df
conn.close


# Top 5 Cuisines
conn = sqlite3.connect(sqlite_file)
cursor = conn.cursor()
cursor.execute('''
    SELECT both.value, COUNT(*) as Total 
    FROM (SELECT * FROM nodes_tags UNION ALL 
    SELECT * FROM ways_tags)
    as both
    WHERE both.key == 'cuisine'
    GROUP BY both.value
    ORDER BY Total DESC
    LIMIT 5;
''')

results = cursor.fetchall()
df = pd.DataFrame(results)
print df
conn.close