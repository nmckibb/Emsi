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

def createDB (mydbfile):
  if os.path.exists(mydbfile):
    os.remove(mydbfile)

  conn = sqlite3.connect(mydbfile)
  return conn

def CreateTable(conn):
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
  return c


def insert_jobposting(c, strbody, strTitle, dtExpired, dtPosted, strState, strCity, strOnet, strSoc5, strSoc2):
    strSQL ="INSERT INTO tblJobPosting (body, title, expired, posted, state, city, onet, soc5, soc2) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
    #print (strSQL)
    try:
      c.execute(strSQL,(strbody, strTitle, dtExpired, dtPosted, strState, strCity, strOnet, strSoc5, strSoc2))
    except Exception as e:
      print (strOnet)
      print(e)

def get_posting(c):
  strSQL ="""SELECT * FROM tblJobPosting"""
  c.execute(strSQL)
  return c.fetchall()

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
  
def procJobFile(InputFile, dmos,conn, c):
  # dimesion varible
  numHTML = 0
  # Opening JSON file
  f = open(InputFile, "r")
  
  for i in f:
    i.strip()
    try:
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
      insert_jobposting (c, strBody.strip(), strTitle.strip(), dtExpired.strip(), dtPosted.strip(), strState.strip(), strCity.strip(), strOnet.strip(), strSoc5.strip(), strSoc2.strip())
      conn.commit()
    except Exception as e:
      print (i)
      print (e)
      print (strOnet)
  
  
  f.close()
  return numHTML


# create db and tables
conn = createDB('mysqlEmsi.db')
c = CreateTable(conn)

#dictionary for mapping to soc5
dmos= getOnetMap("map_onet_soc.csv")

#dictionary for Soc Hierarchy
#dsh = getSocHierarchy("getSocHierarchy")

#process job posting file
#procJobFile("../data_engineer_technical_project/sample",dmos,conn,c)

# dimesion varible
numHTML = procJobFile("sample", dmos, conn, c)

print (numHTML)

conn.commit()
#print (get_posting(c)[1])


conn.close()