
# Python program to read
# json file

import json, re

# Opening JSON file

f = open('sample1.json',)
print(f)
# returns JSON object as
# a dictionary

#while True:
try:
  data = json.load(f)
  for i in data['body']:
      print(i)

  #result = json.loads(s)   # try to parse...
#           break                    # parsing worked -> exit loop
except Exception as e:
   # "Expecting , delimiter: line 34 column 54 (char 1158)"
   # position of unexpected character after '"'
  unexp = int(re.findall(r'\(char (\d+)\)', str(e))[0])
  print (unexp)
  # position of unescaped '"' before that
#  unesc = f.find(r'"', 0, unexp)
#           f = f[:unesc] + r'\"' + f[unesc+1:]
           # position of correspondig closing '"' (+2 for inserted '\')
#           closg = f.find(r'"', unesc + 2)
#           f = f[:closg] + r'\"' + f[closg+1:]
f.close()
