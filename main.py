# Webcawler

# Modules

import urllib.request
import urllib.parse
from html.parser import HTMLParser
import http.client
import sqlite3
import re
import datetime
import time


def adapt_datetime(ts):
    return time.mktime(ts.timetuple())

## Storage


sqlite3.register_adapter(datetime.datetime, adapt_datetime)

dbconn = sqlite3.connect(':memory:')
dbconn.row_factory = sqlite3.Row
c = dbconn.cursor()
c.execute('''CREATE TABLE system(
    id INTEGER PRIMARY KEY,
    date int,
    datatype text unique,
    data text
    )''')

c.execute('''CREATE TABLE urls(
    id INTEGER PRIMARY KEY,
    date int,
    scheme text,
    netloc text unique,
    indexed int
    )''')

c.execute('''CREATE TABLE pages(
    id INTEGER PRIMARY KEY,
    page text,
    indexed int,
    pageurl int,
    FOREIGN KEY(pageurl) REFERENCES urls(id)
    )''')
# a = urllib.parse.urlparse('http://example.com')
# s = (None,1159704000,a[0],a[1],0)
# c.execute("INSERT INTO urls VALUES (?,?,?,?,?)",s)

dbconn.commit()

#t = ('RHAT',)
#c.execute('SELECT * FROM stocks WHERE symbol=?', t)



#c.executemany('INSERT INTO stocks VALUES (?,?,?,?,?)', purchases)

#for row in c.execute("SELECT * FROM stocks WHERE trans = 'BUY' ORDER BY price"):
    #print(row)



## regex

r = re.compile(r'href=[\'"]?([^\'" >]+)')
r2 = re.compile(r'^/')
## HTML parser


# site = urllib.parse.urlparse('http://example.com')
# s = (None,1159704000,site[0],site[1],0)
# c.execute("INSERT INTO urls VALUES (?,?,?,?,?)",s)

# urls table
#   id INTEGER PRIMARY KEY,
#   date int,
#   scheme text,
#   netloc text unique,
#   indexed int

# pages table
#   id INTEGER PRIMARY KEY,
#   page text,
#   indexed int,
#   pageurl int,   FOREIGN KEY(pageurl) REFERENCES urls(id)

## requests

url = urllib.request.Request("http://www.iana.org/domains/reserved", data=None)

with urllib.request.urlopen(url) as f:
    now = datetime.datetime.now()
    a = urllib.parse.urlparse(url.full_url)
    s = (None, now, a[0], a[1], 0)
    c.execute('INSERT INTO urls VALUES (?,?,?,?,?)', s)
    #print("Adding ? to database",s)

    dbconn.commit()

   # print(c.execute("SELECT id FROM urls WHERE netloc = 'www.iana.org'"))
    if a[2] != "":
        #print("Found macth: " + a[2])
        a2 = (a[1],)
        #print(a2)
        c.execute('SELECT id FROM urls WHERE netloc=?', a2)
        g = c.fetchone()
        #print(g)
        s = (None,a[2],0,g[0])
        c.execute('INSERT INTO pages VALUES (?,?,?,?)', s)

        # c.execute('SELECT * FROM pages')
        # g = c.fetchall()
        # for line in g:
        #     for a in line:
        #         print(a)

    site = f.read().decode('utf-8')
    #print(site)
    #print(r.findall(site))
    for each in r.findall(site):
        #print(each)
        now = datetime.datetime.now()
        a = urllib.parse.urlparse(each)
        s = (None, now, a[0], a[1], 0)
        if a[1] != '':
            try:
                c.execute('INSERT INTO urls VALUES (?,?,?,?,?)', s)
                #print("Adding ? to database", s)

            except sqlite3.IntegrityError as error:
                print("Exeption sqlite3.IntegrityError: ", error, "\nFor data: ", s)
                dbconn.rollback()
                pass



        dbconn.commit()

        if a[2] != "":
            if a[1] == '':
                a3 = urllib.parse.urlparse(url.full_url)
            else:
                a3 = a
            #print(a[2])
            a2 = (a3[1],)
            #print(a2)
            c.execute('SELECT id FROM urls WHERE netloc=?', a2)
            g = c.fetchone()
            while g != None:
                s = (None, a[2], 0, g[0])
                print("adding: ", s, "to db")
                c.execute('INSERT INTO pages VALUES (?,?,?,?)', s)
                g = c.fetchone()

    a = urllib.parse.urlparse(url.full_url)
    a2 = (a[1],)
    c.execute('UPDATE urls SET indexed = 1 where netloc = ?', a2)
    dbconn.commit()

c.execute('SELECT * FROM urls')
g = c.fetchall()
for line in g:
    for a in line:
        print(a)

c.execute('SELECT * FROM pages')
g = c.fetchall()
for line in g:
    for a in line:
        print(a)