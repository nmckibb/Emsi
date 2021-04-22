#python program to read
# json file
#import tools
import json, ast, re, bs4, datetime, io, sqlite3, os, math
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

def get_posting_count(c):
  strSQL ="""SELECT count(*) FROM tblJobPosting"""
  c.execute(strSQL)
  return c.fetchall()

def get_count_of_soc2(c):
  strSQL ="""SELECT soc2,count(*) FROM tblJobPosting GROUP BY soc2"""
  c.execute(strSQL)
  return c.fetchall()

def get_count_ActiveRecord(c):
  strSQL ="""SELECT count(*) FROM tblJobPosting  WHERE expired > '2017-02-01' and posted < '2017-02-02'"""
  c.execute(strSQL)
  return c.fetchall()

def getSocHierarchy(strFileName):
  #child,parent,level,name
  tdmos = {}
  #setup map onet soc data dictionary
  osf = open(strFileName,"r")
  # process records
  for i in osf:
    # add in " : etc to place in dictionary
    if i[18]=='"':
      ti = '{"'+ i[0:7] +'": "'+ i[8:15] +'" ,"level": '+ i[16] +',"soc2_name": '+i[18:].strip()+'}'
    else:
      ti = '{"'+ i[0:7] +'": "'+ i[8:15] +'" ,"level": '+ i[16] +',"soc2_name": "'+i[18:].strip()+'"}'
    # ingore heading row
    if "child" not in i:
      tdmos.update(ast.literal_eval(ti))
  osf.close()
  return tdmos
# Function to remove tags

def strip_non_ascii(string):
    ''' Returns the string without non ASCII characters'''
    stripped = (c for c in string if 0 < ord(c) < 127)
    return ''.join(stripped)

def procJobFile(InputFile, dmos,conn, c, dsh):
  # dimesion varible
  numHTML = 0
  numRecords = 0
  # Opening JSON file
  f = open(InputFile, "r")
  
  for i in f:
    i.strip()
    if math.fmod(numRecords, 2000)==0:
      print ("Records Processed : " + str(numRecords))
    try:
      data = ast.literal_eval(i)
      #data = json.dumps(data)
      strOBody = str(strip_non_ascii(data["body"]).encode( "ascii", "ignore"))
      #strOBody = data["body"]
      #if bool(BeautifulSoup(strOBody,"html.parser").find()):
      strCBody = BeautifulSoup(str(strOBody),"lxml").get_text
      if strOBody == strCBody:
        strBody = strOBody
      else:
        #strBody = str(strCBody).encode("utf-8", "ignore")
        #strBody = strCBody
        strBody = str(strCBody)
        numHTML+=1
      
      strTitle = data["title"]
      dtExpired = data["expired"]
      dtPosted = data["posted"]
      strState = data["state"]
      strCity = data["city"]
      strOnet = data["onet"]
      strSoc5 = dmos[data["onet"]]
      strSoc2 = dsh[strSoc5]
      insert_jobposting (c, strBody, str(strTitle), dtExpired, dtPosted, str(strState), str(strCity), str(strOnet), str(strSoc5), str(strSoc2))
      conn.commit()
    except Exception as e:
      print (i)
      print (e)
    numRecords+=1
  
  f.close()
  print ("Records Processed : " + str(numRecords))
  return numHTML


# create db and tables
conn = createDB('mysqlEmsi.db')
c = CreateTable(conn)

#dictionary for mapping to soc5
dmos= getOnetMap("map_onet_soc.csv")

#dictionary for Soc Hierarchy
dsh = getSocHierarchy('soc_hierarchy.csv')

#process job posting file
numHTML = procJobFile("sample",dmos,conn,c,dsh)


# sumary of process
f = open("Sumary.txt", "w")

f.writelines("Count of documents for each `soc2`:")
print("Count of documents for each `soc2`:")
f.writelines(str(get_count_of_soc2(c)))
print(get_count_of_soc2(c))

f.writelines("Number of documents from which you successfully removed HTML tags:" + str(numHTML))
print (" Number of documents from which you successfully removed HTML tags:" + str(numHTML))

f.writelines("Total number of postings that were active on February 1st, 2017: " )
print ("Total number of postings that were active on February 1st, 2017: " )

f.writelines(str(get_count_ActiveRecord(c)))
print(get_count_ActiveRecord(c))


# close and exit
f.close()
conn.commit()
conn.close()