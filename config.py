import xml.sax

class XMLHandler(xml.sax.ContentHandler):
    def __init__(self):
        self.CurrentData = ""
        self.price = ""
        self.surname = ""
        self.address = ""
        self.id = ""

   # Call when an element starts
    def startElement(self, tag, attributes):
        self.CurrentData = tag
        if(tag == "user"):
            title = attributes["number"]
            self.id = title

   # Call when an elements ends
    def endElement(self, tag):
        if(self.CurrentData == "name"):
            pass
        elif(self.CurrentData == "surname"):
            pass
        elif(self.CurrentData == "address"):
            pass
        self.CurrentData = ""

   # Call when a character is read
    def characters(self, content):
        if(self.CurrentData == "name"):
            self.name = content
        elif(self.CurrentData == "surname"):
            self.surname = content
        elif(self.CurrentData == "address"):
            self.address = content
    
    def metadata(self):
        meta_data = {}
        meta_data['id'] = self.id
        meta_data['name'] = self.name
        meta_data['surname'] = self.surname
        meta_data['address'] = self.address
        return meta_data
