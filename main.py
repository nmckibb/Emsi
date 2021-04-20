
# Python program to read
import json, ast, re, bs4, datetime, io, sqlite3, os, math
from bs4 import BeautifulSoup
from sqlite3 import Error

conn = sqlite3.connect('mysqlEmsi.db')
c = conn.cursor()

def get_posting(c):
  strSQL ="""SELECT * FROM tblJobPosting"""
  c.execute(strSQL)
  return c.fetchall()

def get_count_of_soc2(c):
  strSQL ="""SELECT soc2,count(soc2) FROM tblJobPosting GROUP BY soc2"""
  c.execute(strSQL)
  return c.fetchall()

def get_count_ActiveRecord(c):
  strSQL ="""SELECT count(soc2) WHERE expired=>'2017-02-01' and posted<='2017-02-01' FROM tblJobPosting"""
  c.execute(strSQL)
  return c.fetchall()


print(get_count_of_soc2(c))
print(get_count_of_soc2(c))
conn.close()