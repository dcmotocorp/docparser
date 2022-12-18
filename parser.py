import re
import pandas as pd
import xml.sax
from config import XMLHandler 
from engine import session,users
import numpy
from sqlalchemy.sql import insert

#------------------parser --------------#
parser = xml.sax.make_parser()
parser.setFeature(xml.sax.handler.feature_namespaces, 0)
Handler = XMLHandler()
parser.setContentHandler( Handler )
parser.parse("models.xml")
validation_dict = Handler.metadata()
#------------------parser --------------#



required_field = ["id","name","surname","address"]
rx_dict = {

    'userdetails': re.compile(r'(?P<userdetails>name|surname|address)'),

}
def _parse_line(line):
    """
    Do a regex search against all defined regexes and
    return the key and match result of the first matching regex

    """

    for key, rx in rx_dict.items():
        match = rx.search(line)
        if match:
            return key, match
    return None, None

def parse_file(filepath):
    """
    Parse text at given filepath
    Parameters
    ----------
    filepath : str
        Filepath for file_object to be parsed
    Response :
                data : pd.DataFrame
                    Parsed data
    """

    data = [] 
    with open(filepath, 'r') as file_object:
        line = file_object.readline()
        while line:
            key, match = _parse_line(line)
            if key == 'userdetails':
                value_type = match.group('userdetails')
                line = file_object.readline()
                while line.strip():
                    number, value = line.strip().split(',')
                    value = value.strip()
                    row = {
                        'id': number,
                        value_type: value
                    }
                    data.append(row)
                    line = file_object.readline()

            line = file_object.readline()
        data = pd.DataFrame(data)
        data.set_index([ 'id'], inplace=True)
        data = data.groupby(level=data.index.names).first()
        data = data.apply(pd.to_numeric, errors='ignore')
    return data

#---------files read section----------#

def read_csv_files(file):
    return pd.read_csv(file)

def read_json_files(file):
    import json
    with open(file,'r') as f:
        return json.load(f)
def read_from_excel_file(file):
    return pd.read_excel(file)

def read_from_html_file(file):
    return pd.read_html(file)

def read_from_xml_file(file):
    return pd.read_xml(file)
    
def read_from_hdf_files(file):
    return pd.read_hdf(file)
#---------files read section----------#

#----------- mysql changes -----------#

def update_database(data,session):
    """
    insert data into database table
    """
    with session() as session:
        cat_stmt = insert(users).\
                    values(name = data['name'],
                    surname = data['surname'],
                    address = data['address'])
        record_row = session.execute(cat_stmt)
        session.commit()


#------- -End mysql changes ---------#
#------- -validation layer ----------#

def validate_field(data,lookupfields):
    tem = {}
    for _data in lookupfields.keys():
        if lookupfields[_data] == "int":
            tem[_data] = int
            # tem[_data] = numpy.int64
        if lookupfields[_data] == "string":
            tem[_data] = str
            # tem[_data] = numpy.str_
    data = data.to_dict()
    if len(set(data.keys())) == len(set(required_field)):
        if len(set(data.keys()).difference(set(required_field)) ) ==0:
            status = True
            for index in data.keys():
                if index in required_field :
                    if not type(data[index]) == tem[index]:
                        status = False
                        
            if status ==True :
                update_database(data,session)
                print("data inserted to db")                 
    return tem

def validate_field_for_json(data,lookupfields):
    tem = {}
    for _data in lookupfields.keys():
        if lookupfields[_data] == "int":
            tem[_data] = int
            # tem[_data] = numpy.int64
        if lookupfields[_data] == "string":
            tem[_data] = str
            # tem[_data] = numpy.str_
    if len(set(data.keys())) == len(set(required_field)):
        if len(set(data.keys()).difference(set(required_field)) ) ==0:
            status = True
            for index in data.keys():
                if index in required_field :
                    if not type(data[index]) == tem[index]:
                        status = False
                        print("===falise",type(data[index]),tem[index])
            if status ==True :
                update_database(data,session)
                print("data inserted to db")                  
    return tem

def validate_csv_data(data):
    for _data in data.iloc:
        validate_field(_data,validation_dict)
    
def validate_json_data(data):
    validate_field_for_json(data,validation_dict)

#-------- end validation layer ----------#


if __name__ == '__main__':
    #input file
    filepath = 'files/sample.xml'
    
    check_file = filepath.split(".")
    if check_file[-1] == "csv":
        data = read_csv_files(filepath)
        data = validate_csv_data(data)
    
    elif check_file[-1] == "json":
        data = read_json_files(filepath)
        tem ={}
        for index,data in enumerate(data):
            tem = data
            tem["id"] = index
        data = validate_json_data(tem)

    elif check_file[-1] == "html":
        data = read_from_html_file(filepath)
        data = data[0]
        for index,i in enumerate(data.iloc):
            data["id"] = index
        data = validate_csv_data(data)

    elif check_file[-1] == "xml":
        data = read_from_xml_file(filepath)
        for index,i in enumerate(data.iloc):
            data["id"] = index
        data = validate_csv_data(data)
    
    elif check_file[-1] in ["xls","xlsx"]:
        data = read_from_excel_file(filepath)
        for index,i in enumerate(data.iloc):
            data["id"] = index
        data = validate_csv_data(data)
        
    elif check_file[-1] == "hdf":
        data = read_from_hdf_files(filepath)
    
    else:
        data = parse_file(filepath)
        for index,i in enumerate(data.iloc):
            data["id"] = index
        data = validate_csv_data(data)
    