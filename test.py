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
    strSQL ="""INSERT INTO tblJobPosting (body, title, expired, posted, state, city, onet, soc5, soc2) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"""
    #print (strSQL)
    c.execute(strSQL,(strbody, strTitle, dtExpired, dtPosted, strState, strCity, strOnet, strSoc5, strSoc2))

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
  
def procJobFile(InputFile, dmos, conn, c):
  # Opening JSON file
  f = open(InputFile, "r")
  
  #print(f.readline())
  for i in f:
    data = ast.literal_eval(i)
    #data = json.dumps(data)
    strBody = data["body"]
    if bool(BeautifulSoup(strBody,"lxml.parser").find()):
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
    insert_jobposting (c, strBody, strTitle, dtExpired, dtPosted,strState, strCity, strOnet, strSoc5, strSoc2)
    conn.commit()
    print strSoc5
  return numHTML


# create db and tables
conn = createDB('mysqlEmsi.db')
c = CreateTable(conn)

#dictionary for mapping to soc5
dmos= getOnetMap("map_onet_soc.csv")

#dictionary for Soc Hierarchy
dsh = getSocHierarchy("getSocHierarchy")

#process job posting file
print(procJobFile("sample",dmos,conn, c))
#procJobFile("../data_engineer_technical_project/sample",dmos,conn,c)


conn.commit()
print get_posting(c)[1]
print (numHTML)
f.close()
conn.close()