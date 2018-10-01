            ####################################################
            ###### OPEN STREET MAP, DATA CLEANING PROJECT ######
            ####################################################



###### This code here imports xml file downloaded from **Open Street Map** ######
###### which contains the map data for Bengaluru City, in xml format, and  ######
###### cleans that data, finally converting it into json format, and       ######
###### storing it in MongoDB for further use.                              ######


## Importing the required modules
import xml.etree.cElementTree as ET   # Module to parse the xml file
import pprint                         # Pretty printing in python
import re                             # Module to use regular expressions
import time                           # Module to access time related functions
import codecs                         # Module for handling file encoding
import json                           # Module to write in a json format
from pymongo import MongoClient       # Module to access mongoDB database



####--------------------------------------------------------------------------------------####
####--------------------------------------------------------------------------------------####


## Below are some useful global constants

lower = re.compile(r'^([a-z]|_)*$')
lower_colon = re.compile(r'^([a-z]|_)*:([a-z]|_)*$')
problemchars = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')
colon = re.compile(r':')

CREATED = [ "version", "changeset", "timestamp", "user", "uid"]



####--------------------------------------------------------------------------------------####
####--------------------------------------------------------------------------------------####



## Function to count number of tags in the whole xml file.
## Input: XML file
## Output: Dictionary where keys are the tag name and values
##         are the number of their occurences
def count_tags(filename):

    tree = ET.iterparse(filename)

    # Dictionary that holds name of tags as keys, and
    # their number of occurence in the whole document 
    # as values.
    tags = {}

    for event, element in tree:

        tag = element.tag

        if tag in tags.keys():
            tags[tag] += 1

        else:
            tags[tag] = 1


    return tags



####--------------------------------------------------------------------------------------####
####--------------------------------------------------------------------------------------####



## From our xml data we can see that there is a "way" element which
## represnts highways, roads, streets, etc. It has a tag element, which
## specifies its type, name, and other attributes, FOR EXAMPLE =>
##
##             <tag k="highway" v="residential"/>
##             <tag k="name" v="Clipstone Street"/>
##             <tag k="oneway" v="yes"/>
##
## We need to check the value for attribute "k", if there is any invalid
## character such as symbols or not. For this we will take 3 regular
## expressions, which are as follows =>
## 
## lower = "^([a-z]|_)*$", for lower case values and which are valid
## lower_colon = "^([a-z]|_)*:([a-z]|_)*$", for lower case values with
##               semicolon in between them
## problemchars = "[=\+/&<>;\'"\?%#$@\,\. \t\r\n]",  for values with 
##                problematic characters
## other, for the values which do not belong the above three categories
##
## Here, we'll count the occurences of such values.



## Below is a function just to perform the above task
## Input: XML elements
## Output: Dictionary with counts of occurences of values
##         matching the above regex
def types_of_text(filename):

    tree = ET.iterparse(filename)

    # Dictionary to hold count of values of different types
    value_types = {'lower' : 0, 'lower_colon' : 0,
                   'problemchars' : 0, 'other' : 0}
    

    for _, element in tree:

        if element.tag == "tag":

            key = element.attrib['k']

            if re.search(lower, key):
                value_types['lower'] += 1
            elif re.search(lower_colon, key):
                value_types['lower_colon'] += 1
            elif re.search(problemchars, key):
                value_types['problemchars'] += 1
            else:
                value_types['other'] += 1


    
    return value_types




####--------------------------------------------------------------------------------------####
####--------------------------------------------------------------------------------------####



## In our xml file some tag contains some attributes which specify
## the name of the user, we can extract those attributes and,
## which will tell us how many unique user ids are there in our 
## data. Below function will do that.

## Function to output unique users
## Input: XML data
## Output: Set of unique user ids
def unique_users(filename):

    tree = ET.iterparse(filename)

    # Set object to hold unique user ids
    users = set()

    for _, element in tree:

        if "user" in element.attrib.keys():

            users.add(element.attrib['user'])



    return users





####--------------------------------------------------------------------------------------####
####--------------------------------------------------------------------------------------####


## In our XML file we have several tags, such as node, and ways, which contains attributes
## whose value gives us some valuable informatio, also these tags have child tags, which 
## also contain useful information. We will extract that information and write it into a json
## file. After that we will upload the processed data in mongoDB. Below function does that.

## Function to read each tag store the relevant information in dictionary
## Input: filename
## Output: Dictionary element
def get_info(element):

        dict_element = {}


        if element.tag == "node" or element.tag == "way":

            dict_element["type"] = element.tag

            for attr in element.attrib:


                # Checking if the attribute is in CREATED list
                if attr in CREATED:

                    if "created" not in dict_element:

                        dict_element["created"] = {}
                        dict_element["created"][attr] = element.attrib[attr]

                    dict_element["created"][attr] = element.attrib[attr]
                        
                # Checking if the attribute is longitude or latitude
                elif attr in ["lat", "lon"]:
                    
                    if "pos" not in dict_element:
                        
                        dict_element["pos"] = [None, None]

                    if attr == "lat":

                        dict_element["pos"][0] = element.attrib["lat"]

                    if attr == "lon":

                        dict_element["pos"][1] = element.attrib["lon"]

                else:

                    dict_element[attr] = element.attrib[attr]


            # Parsing children of "node" element having a tag, "tag"
            for tag in element.iter("tag"):

                key = tag.attrib['k']

                if not(problemchars.search(key)) and key.startswith("addr:"):

                    if "address" not in dict_element:

                        dict_element["address"] = {}

                    dict_element["address"][key[5:]] = tag.attrib['v']

                elif (not(problemchars.search(key)) and not(colon.search(key))):

                    dict_element[key] = tag.attrib['v']


            # Parsing children of "way" element having a tag, "nd"
            for nd in element.iter("nd"):

                value = nd.attrib['ref']

                if "node_refs" not in dict_element:

                    dict_element["node_refs"] = []

                dict_element["node_refs"].append(value)



            return(dict_element)
            
        else:

            return(dict_element)
            

        


####--------------------------------------------------------------------------------------####
####--------------------------------------------------------------------------------------####


## Above function "get_info()", extracts useful information from the xml file. Now, we
## will write that information in a json file. Below is a function just to perform that.

## Function writes the dictionary in a json file.
## Input: Filename
## Output: List of dictionary element, and writes dictionary elements in
##         a json file.
def write_info(filename, pretty = False):

    file_out = "{0}.json".format(filename)

    counter = 0

    with codecs.open(file_out, "w") as fo:

        fo.write("[")

        for _, element in ET.iterparse(filename):

            el = get_info(element)
            

            if el:

                counter += 1

                if pretty:

                    fo.write(json.dumps(el, indent=2)+","+"\n")

                else:

                    fo.write(json.dumps(el) + "," + "\n")

        fo.write("]")


    print("Number of entries -> {0}".format(counter))





####--------------------------------------------------------------------------------------####
####--------------------------------------------------------------------------------------####

## Now that we have written all the refined in a json file, our next task is to store that
## data in mongoDB. For this we will use "pymongo" module of python. Below is the function
## to do that.

## Function stores the data from json file to mongoDB.
## Input: Reads each line form the json file.
## Output: Return nothing, stores data in a mongoDB database.
def store_info(filename):

    # Connecting to openStreetMapData database in mongoDB
    client = MongoClient("mongodb://localhost:27017")
    db = client.openStreetMapData

    with open(filename) as f:

        data = json.loads(f.read())
        db.largeData.insert_many(data)
        print(db.smallData.find_one())




####--------------------------------------------------------------------------------------####
####--------------------------------------------------------------------------------------####



if __name__ == "__main__":
    
    start_time = time.time()
    #result = count_tags("D:/DataSets/map (1)")
    #result = types_of_text("D:/DataSets/map1")
    result = unique_users("D:/DataSets/map (1)")
    #write_info("D:/DataSets/map (1)")
    #pprint.pprint(data)
    #store_info("D:/DataSets/map (1).json")
    end_time = time.time()
    #pprint.pprint(result)
    print("\nNumber of user => {0}".format(len(result)))
    print("Time taken -> {0}".format(end_time - start_time))




####--------------------------------------------------------------------------------------####
####--------------------------------------------------------------------------------------####
    
