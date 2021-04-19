import sqlite3
import subprocess, os 

from sqlite3 import Error

mydbfile='mysqlEmsi.db'
if os.path.exists(mydbfile):
  os.remove(mydbfile)
#subprocess.call("rm -f mysqlEmsi.db")
#onn = sqlite3.connect(':memory:')
conn = sqlite3.connect(mydbfile)

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
conn.close()
