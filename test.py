#python program to read
# json file
#import tools
import json, ast, re, bs4, datetime, io, sqlite3, os
from bs4 import BeautifulSoup
from sqlite3 import Error

def getOnetMap(strFileName):
  tdmos = {}
  #setup map onet soc data dictionary
  osf = open(strFileName,"r")
  # process records
  for i in osf:
    # split on , to prep for dictionary
    lstx = i.split(",")
    # add in " : etc to place in dictionary
    ti = '{"'+lstx[0]+'": "'+lstx[1].strip()+'"}'
    # ingore heading row
    if "onet" not in i:
      tdmos.update(ast.literal_eval(ti))
  osf.close()
  return tdmos

#def createDB (mydbfile):
if os.path.exists('mysqlEmsi.db'):
    os.remove('mysqlEmsi.db')

conn = sqlite3.connect('mysqlEmsi.db')
#  return conn

#def CreateTable():
c = conn.cursor()

c.execute("""CREATE TABLE tblJobPosting (
  body TEXT,
  title TEXT,
  expired DATE,
  posted DATE,
  state TEXT,
  city TEXT,
  onet TEXT,
  soc5 TEXT,
  soc2 TEXT
  )""")
  
c.execute("""CREATE TABLE tblonet_soc (
        onet TEXT,
        soc5 TEXT
        )""")
conn.commit()
#return c


def insert_jobposting(strbody, strTitle, dtExpired, dtPosted, strState, strCity, strOnet, strSoc5, strSoc2):
    strSQL ="INSERT INTO tblJobPosting (body, title, expired, posted, state, city, onet, soc5, soc2) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
    #print (strSQL)
    c.execute(strSQL,(strbody, strTitle, dtExpired, dtPosted, strState, strCity, strOnet, strSoc5, strSoc2))

def get_posting():
  strSQL ="""SELECT * FROM tblJobPosting"""
  c.execute(strSQL)
  return c.fetchall()


# def insert_jobposting( c, strbody, strTitle, dtExpired, dtPosted, strState, strCity, strOnet, strSoc5, strSoc2):
  # strSQL ="""INSERT INTO tblJobPosting (body, title, expired, posted, state, city, onet, soc5, soc2) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"""
  # #print (strSQL)
   # c.execute(strSQL,(strbody, strTitle, dtExpired, dtPosted, strState, strCity, strOnet, strSoc5, strSoc2))


def findObject(self, attr, value):
  if getattr(self, attr) == value:
    return self
  else:
    for child in self.children:
      match = child.findObject(attr, value)
      if match:
        return match

def getSocHierarchy(strFileName):
  tdmos = {}
  #setup map onet soc data dictionary
  osf = open(strFileName,"r")
  # process records
  for i in osf:
    # split on , to prep for dictionary
    lstx = i.split(",")
    # add in " : etc to place in dictionary
    ti = '{"'+lstx[0]+'": "'+lstx[1].strip()+'"}'
    # ingore heading row
    if "child" not in i:
      tdmos.update(ast.literal_eval(ti))
  osf.close()
  return tdmos
  
def procJobFile(InputFile, dmos):
  # dimesion varible
  numHTML = 0
  # Opening JSON file
  f = open(InputFile, "r")
  
  #print(f.readline())
  for i in f:
    data = ast.literal_eval(i)
    #data = json.dumps(data)
    strBody = data["body"]
    if bool(BeautifulSoup(strBody,"html.parser").find()):
      strBody = BeautifulSoup(strBody,"lxml").txt
      numHTML+=1
    strTitle = data["title"]
    dtExpired = data["expired"]
    dtPosted = data["posted"]
    strState = data["state"]
    strCity = data["city"]
    strOnet = data["onet"]
    strSoc5 = dmos[data["onet"]]
    strSoc2 = "soc2"
    insert_jobposting (strBody, strTitle, dtExpired, dtPosted,strState, strCity, strOnet, strSoc5, strSoc2)
    conn.commit()
    #print (strSoc5)
  #return numHTML


# create db and tables
#conn = createDB('mysqlEmsi.db')
#c = CreateTable()

#dictionary for mapping to soc5
dmos= getOnetMap("map_onet_soc.csv")

#dictionary for Soc Hierarchy
#dsh = getSocHierarchy("getSocHierarchy")

#process job posting file
#NumProc = procJobFile ("sample", dmos, conn, c)
#procJobFile("../data_engineer_technical_project/sample",dmos,conn,c)

# dimesion varible
numHTML = 0
# Opening JSON file
f = open("sample", "r")
  
#print(f.readline())
for i in f:
  data = ast.literal_eval(i)
  #data = json.dumps(data)
  #while True:
  try:
            data = ast.literal_eval(i)
            #data = json.dumps(i)
            strBody  = data["body"]
            if bool(BeautifulSoup(strBody,"html.parser").find()):
              strBody = BeautifulSoup(strBody,"lxml").txt
              numHTML+=1
            strTitle = data["title"]
            dtExpired = data["expired"]
            dtPosted = data["posted"]
            strState = data["state"]
            strCity = data["city"]
            strOnet = data["onet"]
            strSoc5 = dmos[data["onet"]]
            strSoc2 = "soc2"
            insert_jobposting (c, strBody, strTitle, dtExpired, dtPosted, strState, strCity, strOnet, strSoc5, strSoc2)
            #print BeautifulSoup(data["body"],"lxml").text
            #print (data["onet"])
            #print (dmos[data["onet"]]) # found soc5
            #print (json.dumps(data))
            #data = json.load(i)
            #result = json.loads(s)   # try to parse...
#            break                    # parsing worked -> exit loop
  except Exception as e:
            # "Expecting , delimiter: line 34 column 54 (char 1158)"
            # position of unexpected character after '"'
            print (e)
            print (re.findall(r'\(char(\d+)\)', str(e)))
            #unexp = int(re.findall(r'\(char (\d+)\)', str(e))[0])
            ## position of unescaped '"' before that
            #unesc = i.rfind(r'"', 0, unexp)
            #i = i[:unesc] + r'\"' + i[unesc+1:]
            ## position of correspondig closing '"' (+2 for inserted '\')
            #closg = i.find(r'"', unesc + 2)
            #i = i[:closg] + r'\"' + i[closg+1:]


procJobFile("sample", dmos)

print (numHTML)

conn.commit()
print get_posting(c)[1]

f.close()
conn.close()