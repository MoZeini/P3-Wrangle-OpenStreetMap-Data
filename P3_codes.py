
# coding: utf-8

# ### OpenStreetMap Data Case Study (Boston)
# by Mohamad Zeini Jahromi
# 
# ===================================== 
#  Types of tags and attributes
# ===================================== 

import xml.etree.cElementTree as ET
import pprint

# original OSM file and its sample
OSM_FILE = "boston_massachusetts.osm"             
SAMPLE_FILE = "boston_massachusetts_sample.osm"

# This function counts the unique number of tags in the given file
def count_tags(filename):                         
    tags = {}
    context = ET.iterparse(filename)
    for event, elem in context:
        if elem.tag not in tags.keys():
            tags[elem.tag] = 1
        else:
            tags[elem.tag] +=1
    return tags

# ===================================== 
# Improving Street Names
# ===================================== 

import xml.etree.cElementTree as ET
from collections import defaultdict
import re
import pprint

# A regular expression to find the end word of address string which can includes "." 
street_type_re = re.compile(r'\b\S+\.?$', re.IGNORECASE)

# List of expected street types 
expected = ["Street", "Avenue", "Boulevard", "Drive", "Court", "Place", "Square", "Lane", "Road", 
            "Trail", "Parkway", "Commons", 'Circle','Highway','Center','Turnpike','Way']

# This function creates a list of all unexpected street types 
# which are not in the expected list
def audit_street_type(street_types, street_name):
    m = street_type_re.search(street_name)
    if m:
        street_type = m.group()
        if street_type not in expected:
            street_types[street_type].add(street_name)

# This function checks if the address type is a street type
def is_street_name(elem):
    return (elem.attrib['k'] == "addr:street")

# The main function to audit the OSM file and returns the unexpected street types
# and their respective examples
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

# Print out the examples of unexpected street types
#street_types = audit(SAMPLE_FILE)
street_types = audit(OSM_FILE)
print 'Number of street types =', len(street_types)
for street_type, ways in street_types.items()[0:5]: print street_type, ":", ways


# Dictionary of unexpected street types as keys and their appropriate ones as values
mapping_street = {"Ave": "Avenue","Ave.":"Avenue","Ct":"Court","Dr":"Drive","Ext":"Exit",
           "HIghway":"Highway","Hwy":"Highway","Pkwy":"Parkway","Pl":"Place","Rd":"Road",
           "ST":"Street","Sq.":"Square","St":"Street","St,":"Street","St.":"Street",
           "Street.":"Street","rd.":"Road","st":"Street","street":"Street"}

# This function update street names using the "mapping_street" dictionary
def update_street(name, mapping):
    name = name.split(" ")
    if name[-1] in mapping.keys():
        name[-1] = mapping[name[-1]]
    name = " ".join(name)
    return name

# Print out the examples of update function
for street_type, ways in street_types.items()[0:5]:
    for name in ways:
        better_name = update_street(name, mapping_street)
        print name, "=>", better_name
        

# ===================================== 
# Improving State Names
# ===================================== 

# This function creates a list of all types of states  
def audit_state_type(state_types, state_name):
    if state_name not in state_types:
        state_types[state_name] = 1
    else:
        state_types[state_name] += 1

# This function checks if the address type is a state type
def is_state_name(elem):
    return (elem.attrib['k'] == "addr:state")

# The main function to audit the OSM file and returns all types of states
# in the OSM file
def audit(osmfile):
    osm_file = open(osmfile, "r")
    state_types = defaultdict(set)
    for event, elem in ET.iterparse(osm_file, events=("start",)):
        if elem.tag == "node" or elem.tag == "way":
            for tag in elem.iter("tag"):
                if is_state_name(tag):
                    audit_state_type(state_types, tag.attrib['v'])
    osm_file.close()
    return state_types

# Print out all types of states 
#st_types = audit(SAMPLE_FILE)
state_types = audit(OSM_FILE)
print 'Number of state types =',len(state_types)
for k, v in state_types.items(): print k, ":", v


# Dictionary of unexpected state types as keys and their appropriate ones as values
mapping_state = { "MA- MASSACHUSETTS": "MA",
            "MASSACHUSETTS": "MA",
            "Ma": "MA",
            "Massachusetts": "MA",
            "ma": "MA"
            }

# This function update state names using the "mapping_state" dictionary
def update_state(name, mapping):
    if name in mapping.keys():
        name = mapping[name]
    return name

# Print out the examples of update function
for state_type, num in state_types.iteritems():
    better_name = update_state(state_type, mapping_state)
    print state_type, "=>", better_name
    

# ===================================== 
# Improving ZIP Codes
# ===================================== 

# This function creates a list of all types of zipcode  
def audit_zipcode(zipcode_types, zipcode):
    if zipcode not in zipcode_types:
        zipcode_types[zipcode] = 1
    else:
        zipcode_types[zipcode] += 1

# This function checks if the address type is a zipcode type        
def is_zipcode(elem):
    return (elem.attrib['k'] == "addr:postcode")

# The main function to audit the OSM file and returns all types of zipcode
# in the OSM file
def audit(osmfile):
    osm_file = open(osmfile, "r")
    zipcode_types = defaultdict(set)
    for event, elem in ET.iterparse(osm_file, events=("start",)):

        if elem.tag == "node" or elem.tag == "way":
            for tag in elem.iter("tag"):
                if is_zipcode(tag):
                    audit_zipcode(zipcode_types, tag.attrib['v'])
    osm_file.close()
    return zipcode_types

# Print out all types of zipcode along with their cout numbers 
#st_types = audit(SAMPLE_FILE)
zipcode_types = audit(OSM_FILE)
print 'Number of zipcode types =',len(zipcode_types)
for k, v in zipcode_types.items()[0:5]: print k, ":", v


# A regular expression to find zipcodes (first five digits) within a string 
zipcode_re = re.compile(r'\d+')

# This function update zipcode using extracted digits from regular expression
# It returns "0" if the zipcode was not found or if it is outside of Boston area.
def update_zipcode(zipcode):
    zipcode = zipcode_re.findall(zipcode)
    
    if zipcode != [] and len(zipcode[0]) == 5:
        zipcode = zipcode[0]
        if int(zipcode) <= 1431 or int(zipcode) >= 2770:
            zipcode = '0'
    else:
        zipcode = '0'
    return zipcode

# Print out the examples of update function
for zipcode_type, num in zipcode_types.items()[0:5]:
    better_zipcode = update_zipcode(zipcode_type)
    print zipcode_type, "=>", better_zipcode
    

# ===================================== 
# Preparing CSV files for SQL Database
# ===================================== 

import csv
import codecs
import pprint
import re
import xml.etree.cElementTree as ET
import cerberus
import schema

#OSM_PATH = "boston_massachusetts_sample.osm"
OSM_PATH = "boston_massachusetts.osm"

# Pathes to save the CSV files
NODES_PATH = "nodes.csv"
NODE_TAGS_PATH = "nodes_tags.csv"
WAYS_PATH = "ways.csv"
WAY_NODES_PATH = "ways_nodes.csv"
WAY_TAGS_PATH = "ways_tags.csv"

# Regular expression to find tags with a colon in their names (lower_colon)
# or tags with problematic characters (problemchars).    
LOWER_COLON = re.compile(r'^([a-z]|_)+:([a-z]|_)+')
PROBLEMCHARS = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')

# The Udacity pre-defined schema to transform each element into the correct format. 
SCHEMA = schema.schema

# the fields order in the csvs matches the column order in the sql table schema
NODE_FIELDS = ['id', 'lat', 'lon', 'user', 'uid', 'version', 'changeset', 'timestamp']
NODE_TAGS_FIELDS = ['id', 'key', 'value', 'type']
WAY_FIELDS = ['id', 'user', 'uid', 'version', 'changeset', 'timestamp']
WAY_TAGS_FIELDS = ['id', 'key', 'value', 'type']
WAY_NODES_FIELDS = ['id', 'node_id', 'position']

# The shape_element function will transform each element into the correct format. 
# using schema.py file and checks the format using the cerberus library 
# and their respective values using update functions.
def shape_element(element, node_attr_fields=NODE_FIELDS, way_attr_fields=WAY_FIELDS,
                  problem_chars=PROBLEMCHARS, default_tag_type='regular'):
    """Clean and shape node or way XML element to Python dict"""

    node_attribs = {}
    way_attribs = {}
    way_nodes = []
    tags = []  # Handle secondary tags the same way for both node and way elements

    if element.tag == 'node':
        node_attribs['id'] = element.attrib['id']
        node_attribs['user'] = element.attrib['user']
        node_attribs['uid'] = element.attrib['uid']
        node_attribs['version'] = element.attrib['version']
        node_attribs['lat'] = element.attrib['lat']
        node_attribs['lon'] = element.attrib['lon']
        node_attribs['timestamp'] = element.attrib['timestamp']
        node_attribs['changeset'] = element.attrib['changeset']

        for tag in element.iter("tag"):
            d={}
            d['id'] = node_attribs['id']
            k = tag.attrib['k']
            if PROBLEMCHARS.match(k) == None:
                if LOWER_COLON.match(k) != None:
                    d['type'] = k.split(':')[0]
                    d['key'] = ':'.join(k.split(':')[1:])
                else:
                    d['type'] = 'regular'
                    d['key'] = k
                    
            if  k == "addr:street":
                d['value'] = update_street(tag.attrib['v'], mapping_street)
            elif  k == "addr:state":
                d['value'] = update_state(tag.attrib['v'], mapping_state)
            elif  k == "addr:postcode":
                d['value'] = update_zipcode(tag.attrib['v'])
            else:
                d['value'] = tag.attrib['v']
            tags.append(d)
        return {'node': node_attribs, 'node_tags': tags}
    
    elif element.tag == 'way':
        way_attribs['id'] = element.attrib['id']
        way_attribs['user'] = element.attrib['user']
        way_attribs['uid'] = element.attrib['uid']
        way_attribs['version'] = element.attrib['version']
        way_attribs['timestamp'] = element.attrib['timestamp']
        way_attribs['changeset'] = element.attrib['changeset']

        for tag in element.iter("tag"):
            d={}
            d['id'] = way_attribs['id']
            k = tag.attrib['k']
            if PROBLEMCHARS.match(k) == None:
                if LOWER_COLON.match(k) != None:
                    d['type'] = k.split(':')[0]
                    d['key'] = ':'.join(k.split(':')[1:])
                else:
                    d['type'] = 'regular'
                    d['key'] = k
                    
            if  k == "addr:street":
                d['value'] = update_street(tag.attrib['v'], mapping_street)
            elif  k == "addr:state":
                d['value'] = update_state(tag.attrib['v'], mapping_state)
            elif  k == "addr:postcode":
                d['value'] = update_zipcode(tag.attrib['v'])
            else:
                d['value'] = tag.attrib['v']
            tags.append(d)
        
        index = 0
        for tag in element.iter("nd"):
            d={}
            d['id'] = way_attribs['id']
            d['node_id'] = tag.attrib['ref']
            d['position'] = index
            way_nodes.append(d)
            index +=1
        return {'way': way_attribs, 'way_nodes': way_nodes, 'way_tags': tags}

def get_element(osm_file, tags=('node', 'way', 'relation')):
    """Yield element if it is the right type of tag"""

    context = ET.iterparse(osm_file, events=('start', 'end'))
    _, root = next(context)
    for event, elem in context:
        if event == 'end' and elem.tag in tags:
            yield elem
            root.clear()

def validate_element(element, validator, schema=SCHEMA):
    """Raise ValidationError if element does not match schema"""
    if validator.validate(element, schema) is not True:
        field, errors = next(validator.errors.iteritems())
        message_string = "\nElement of type '{0}' has the following errors:\n{1}"
        error_string = pprint.pformat(errors)
        
        raise Exception(message_string.format(field, error_string))

class UnicodeDictWriter(csv.DictWriter, object):
    """Extend csv.DictWriter to handle Unicode input"""

    def writerow(self, row):
        super(UnicodeDictWriter, self).writerow({
            k: (v.encode('utf-8') if isinstance(v, unicode) else v) for k, v in row.iteritems()
        })

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)

def process_map(file_in, validate):
    """Iteratively process each XML element and write to csv(s)"""

    with codecs.open(NODES_PATH, 'w') as nodes_file,          codecs.open(NODE_TAGS_PATH, 'w') as nodes_tags_file,          codecs.open(WAYS_PATH, 'w') as ways_file,          codecs.open(WAY_NODES_PATH, 'w') as way_nodes_file,          codecs.open(WAY_TAGS_PATH, 'w') as way_tags_file:

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

process_map(OSM_PATH, validate=True)

# ===================================== 
# Create SQL DB from CSV files
# Table for nodes
# ===================================== 

import csv
import sqlite3

# Creates the SQL database
con = sqlite3.connect('boston_massachusetts.db')
con.text_factory = str
cur = con.cursor()

# Creates the "nodes" table in the database
#cur.execute('''DROP TABLE nodes;''')
cur.execute('''CREATE TABLE nodes (
    id INTEGER PRIMARY KEY NOT NULL,
    lat REAL,
    lon REAL,
    user TEXT,
    uid INTEGER,
    version INTEGER,
    changeset INTEGER,
    timestamp TEXT);''') 

# Inserting values to the "nodes" table from CSV files
with open ('nodes.csv', 'rb') as table:
    dicts = csv.DictReader(table)
    to_db = ((i['id'], i['lat'],i['lon'],i['user'],i['uid'],i['version'],i['changeset'],i['timestamp']) for i in dicts) 
    cur.executemany("INSERT INTO nodes (id, lat, lon, user, uid, version, changeset, timestamp) VALUES (?,?,?,?,?,?,?,?);", to_db)
con.commit()
#QUERY = '''PRAGMA table_info(nodes)'''
QUERY = '''SELECT id, lat, lon, user, uid, version, changeset FROM nodes LIMIT 5;'''
rows = cur.execute(QUERY).fetchall()
#print(rows)
pprint.pprint(rows)

# ===================================== 
# Table for nodes_tags
# ===================================== 

# Creates the "nodes_tags" table in the database
#cur.execute('''DROP TABLE nodes_tags;''')
cur.execute('''CREATE TABLE nodes_tags (
    id INTEGER,
    key TEXT,
    value TEXT,
    type TEXT,
    FOREIGN KEY (id) REFERENCES nodes(id));''') 

# Inserting values to the "nodes_tags" table from CSV files
with open ('nodes_tags.csv', 'rb') as table:
    dicts = csv.DictReader(table)
    to_db = ((i['id'], i['key'],i['value'],i['type']) for i in dicts)  
    cur.executemany("INSERT INTO nodes_tags (id, key,value,type) VALUES (?,?,?,?);", to_db)
con.commit()
#QUERY = '''PRAGMA table_info(nodes_tags)'''
QUERY = '''SELECT * FROM nodes_tags LIMIT 5;'''
rows = cur.execute(QUERY).fetchall()
#print(rows)
pprint.pprint(rows)

# ===================================== 
# Table for ways
# ===================================== 

# Creates the "ways" table in the database
#cur.execute('''DROP TABLE ways;''')
cur.execute('''CREATE TABLE ways (
    id INTEGER PRIMARY KEY NOT NULL,
    user TEXT,
    uid INTEGER,
    version TEXT,
    changeset INTEGER,
    timestamp TEXT);''') 

# Inserting values to the "ways" table from CSV files
with open ('ways.csv', 'rb') as table:
    dicts = csv.DictReader(table)
    to_db = ((i['id'], i['user'],i['uid'],i['version'],i['changeset'],i['timestamp']) for i in dicts)  
    cur.executemany("INSERT INTO ways (id, user,uid,version,changeset,timestamp) VALUES (?,?,?,?,?,?);", to_db)
con.commit()
#QUERY = '''PRAGMA table_info(ways)'''
QUERY = '''SELECT * FROM ways LIMIT 5;'''
rows = cur.execute(QUERY).fetchall()
#print(rows)
pprint.pprint(rows)

# ===================================== 
# Table for ways_tags
# ===================================== 

# Creates the "ways_tags" table in the database
#cur.execute('''DROP TABLE ways_tags;''')
cur.execute('''CREATE TABLE ways_tags (
    id INTEGER NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    type TEXT,
    FOREIGN KEY (id) REFERENCES ways(id));''') 

# Inserting values to the "ways_tags" table from CSV files
with open ('ways_tags.csv', 'rb') as table:
    dicts = csv.DictReader(table)
    to_db = ((i['id'], i['key'],i['value'],i['type']) for i in dicts)  
    cur.executemany("INSERT INTO ways_tags(id, key,value,type) VALUES (?,?,?,?);", to_db)
con.commit()
#QUERY = '''PRAGMA table_info(ways_tags)'''
QUERY = '''SELECT * FROM ways_tags LIMIT 5;'''
rows = cur.execute(QUERY).fetchall()
#print(rows)
pprint.pprint(rows)

# ===================================== 
# Table for ways_nodes 
# ===================================== 

# Creates the "ways_nodes" table in the database
#cur.execute('''DROP TABLE ways_nodes;''')
cur.execute('''CREATE TABLE ways_nodes (
    id INTEGER NOT NULL,
    node_id INTEGER NOT NULL,
    position INTEGER NOT NULL,
    FOREIGN KEY (id) REFERENCES ways(id),
    FOREIGN KEY (node_id) REFERENCES nodes(id));''') 

# Inserting values to the "ways_nodes" table from CSV files
with open ('ways_nodes.csv', 'rb') as table:
    dicts = csv.DictReader(table)
    to_db = ((i['id'], i['node_id'],i['position']) for i in dicts)  
    cur.executemany("INSERT INTO ways_nodes(id, node_id,position) VALUES (?,?,?);", to_db)
con.commit()
#QUERY = '''PRAGMA table_info(ways_nodes)'''
QUERY = '''SELECT * FROM ways_nodes LIMIT 5;'''
rows = cur.execute(QUERY).fetchall()
#print(rows)
pprint.pprint(rows)

# ===================================== 
# Data Overview 
# File Sizes
# ===================================== 

# Creates a list of file sizes
import os
files_lst = ['nodes.csv', 'nodes_tags.csv', 'ways.csv', 'ways_tags.csv', 'ways_nodes.csv',
             'boston_massachusetts_sample.db', 'boston_massachusetts.osm']
for i in files_lst: 
    print "file {!r} is {!s} MB".format(i,round(os.path.getsize(i)/(1024*1024.0),1))

# ===================================== 
# Number of nodes 
# ===================================== 

# Queries the Number of nodes
QUERY=('''SELECT COUNT(*) FROM nodes;''')
rows = cur.execute(QUERY).fetchall()
print rows[0][0]

# ===================================== 
# Number of ways 
# ===================================== 

# Queries the Number of ways
QUERY=('''SELECT COUNT(*) FROM ways;''')
rows = cur.execute(QUERY).fetchall()
print rows[0][0]

# ===================================== 
# Data Exploration
# Top Postal Codes
# ===================================== 

# Queries the Top 10 Postal Codes by count
QUERY=('''SELECT tags.value, COUNT(*) as count 
        FROM (SELECT * FROM nodes_tags 
        UNION ALL 
        SELECT * FROM ways_tags) tags
        WHERE tags.key='postcode'
        GROUP BY tags.value
        ORDER BY count DESC LIMIT 10;''')
rows = cur.execute(QUERY).fetchall()
for k, v in rows: print k, ':', v

# ===================================== 
# Sort cities by count, descending
# ===================================== 

# Queries the Top 10 cities by count
QUERY=('''SELECT tags.value, COUNT(*) as count 
FROM (SELECT * FROM nodes_tags UNION ALL 
      SELECT * FROM ways_tags) tags
WHERE tags.key LIKE '%city'
GROUP BY tags.value
ORDER BY count DESC
LIMIT 10;''')
rows = cur.execute(QUERY).fetchall()
for k, v in rows: print k, ':', v

# ===================================== 
# Number of  unique users 
# ===================================== 

# Queries the Number of unique users
QUERY=('''SELECT COUNT(DISTINCT(e.uid))          
FROM (SELECT uid FROM nodes UNION ALL SELECT uid FROM ways) e;''')
rows = cur.execute(QUERY).fetchall()
print rows[0][0]

# ===================================== 
# Top 10 contributing users
# ===================================== 

# Queries Top 10 contributing users
QUERY=('''SELECT e.user, COUNT(*) as num
FROM (SELECT user FROM nodes UNION ALL SELECT user FROM ways) e
GROUP BY e.user
ORDER BY num DESC
LIMIT 10;''')
rows = cur.execute(QUERY).fetchall()
for k, v in rows: print k, ':', v

# ===================================== 
# Number of users appearing only once (having 1 post)
# ===================================== 

# Queries the Number of users appearing only once (having 1 post)
con = sqlite3.connect('boston_massachusetts.db')
con.text_factory = str
cur = con.cursor()
QUERY=('''SELECT COUNT(*) 
FROM
    (SELECT e.user, COUNT(*) as num
     FROM (SELECT user FROM nodes UNION ALL SELECT user FROM ways) e
     GROUP BY e.user
     HAVING num=1)  u;''')
rows = cur.execute(QUERY).fetchall()
print rows[0][0]

# ===================================== 
# Top 10 appearing amenities
# ===================================== 

# Queries Top 10 appearing amenities
QUERY=('''SELECT value, COUNT(*) as num
FROM nodes_tags
WHERE key='amenity'
GROUP BY value
ORDER BY num DESC
LIMIT 10;''')
rows = cur.execute(QUERY).fetchall()
for k, v in rows: print k, ':', v

# ===================================== 
# Biggest religion (no surprise here)
# ===================================== 

# Queries the Biggest religion
QUERY=('''SELECT nodes_tags.value, COUNT(*) as num
FROM nodes_tags 
    JOIN (SELECT DISTINCT(id) FROM nodes_tags WHERE value='place_of_worship') i
    ON nodes_tags.id=i.id
WHERE nodes_tags.key='religion'
GROUP BY nodes_tags.value
ORDER BY num DESC
LIMIT 1;''')
rows = cur.execute(QUERY).fetchall()
for k, v in rows: print k, ':', v

# ===================================== 
# Most popular cuisines
# ===================================== 

# Queries Top 10 popular cuisines
QUERY=('''SELECT nodes_tags.value, COUNT(*) as num
FROM nodes_tags 
    JOIN (SELECT DISTINCT(id) FROM nodes_tags WHERE value='restaurant') i
    ON nodes_tags.id=i.id
WHERE nodes_tags.key='cuisine'
GROUP BY nodes_tags.value
ORDER BY num DESC
LIMIT 10;''')
rows = cur.execute(QUERY).fetchall()
for k, v in rows: print k, ':', v

# ===================================== 
# Additional Ideas
# ===================================== 

# Queries all types of buildings
QUERY=('''SELECT value, COUNT(*) as num
FROM nodes_tags
WHERE key='building'
GROUP BY value
ORDER BY num DESC;''')
rows = cur.execute(QUERY).fetchall()
for k, v in rows: print k, ':', v
