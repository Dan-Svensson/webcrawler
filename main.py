# Webcawler

# Modules

import datetime
import re
import sqlite3
import time
import urllib.request
import urllib.parse


class db:
    def __init__(self, dbtype):
        self.dbc = self.dbconn(dbtype)
        self.dbcu = self.dbcursor()
        self.dbc.row_factory = sqlite3.Row
        sqlite3.register_adapter(datetime.datetime, parsing.adapt_datetime)

    def DBcreate(self):
        self.dbcu.execute('''CREATE TABLE system(
        id INTEGER PRIMARY KEY,
        date int,
        datatype text unique,
        data text
        )''')

        self.dbcu.execute('''CREATE TABLE urls(
        id INTEGER PRIMARY KEY,
        date int,
        scheme text,
        netloc text unique,
        indexed int
        )''')

        self.dbcu.execute('''CREATE TABLE pages(
        id INTEGER PRIMARY KEY,
        page text,
        indexed int,
        pageurl int,
        FOREIGN KEY(pageurl) REFERENCES urls(id)
        )''')
        self.dbcommit()

    def dbconn(self, db):
        return sqlite3.connect(db)

    def dbcursor(self):
        return self.dbc.cursor()

    def dbcommit(self):
        self.dbc.commit()

    def dbdump(self):

        self.dbcu.execute('SELECT * FROM urls')

        for line in self.dbcu.fetchall():
            for a in line:
                print(a)

        self.dbcu.execute('SELECT * FROM pages')

        for line in self.dbcu.fetchall():
            for a in line:
                print(a)


class parsing:
    def __init__(self):
        self.re1 = self.regex(r'href=[\'"]?([^\'" >]+)')
        self.re2 = self.regex(r'^/')

    ## regex patterns
    ## r'href=[\'"]?([^\'" >]+)'
    ## r'^/'

    def regex(self, pat):
        return re.compile(pat)

    def adapt_datetime(self, ts):
        return time.mktime(ts.timetuple())


# def Init():
#     DBConnect(':memory:')
#     DBInit()
#     regex()
#
# def adapt_datetime(ts):
#     return time.mktime(ts.timetuple())

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

class req:
    def __init__(self, url):
        self.url = urllib.request.Request(url)

    def request(self):
        urllib.request.urlopen(self.url)


class run:
    def __init__(self, dbtype):
        self.a = db(dbtype)
        self.b = parsing()


x = run(':memory:')
x.a.DBcreate()
x.a.dbdump()


# with urllib.request.urlopen(url) as f:
#     now = datetime.datetime.now()
#     a = urllib.parse.urlparse(url.full_url)
#     s = (None, now, a[0], a[1], 0)
#     c.execute('INSERT INTO urls VALUES (?,?,?,?,?)', s)
#     #print("Adding ? to database",s)
#
#     dbconn.commit()
#
#    # print(c.execute("SELECT id FROM urls WHERE netloc = 'www.iana.org'"))
#     if a[2] != "":
#         #print("Found macth: " + a[2])
#         a2 = (a[1],)
#         #print(a2)
#         c.execute('SELECT id FROM urls WHERE netloc=?', a2)
#         g = c.fetchone()
#         #print(g)
#         s = (None,a[2],0,g[0])
#         c.execute('INSERT INTO pages VALUES (?,?,?,?)', s)
#
#         # c.execute('SELECT * FROM pages')
#         # g = c.fetchall()
#         # for line in g:
#         #     for a in line:
#         #         print(a)
#
#     site = f.read().decode('utf-8')
#     #print(site)
#     #print(r.findall(site))
#     for each in r.findall(site):
#         #print(each)
#         now = datetime.datetime.now()
#         a = urllib.parse.urlparse(each)
#         s = (None, now, a[0], a[1], 0)
#         if a[1] != '':
#             try:
#                 c.execute('INSERT INTO urls VALUES (?,?,?,?,?)', s)
#                 #print("Adding ? to database", s)
#
#             except sqlite3.IntegrityError as error:
#                 print("Exeption sqlite3.IntegrityError: ", error, "\nFor data: ", s)
#                 dbconn.rollback()
#                 pass
#
#
#
#         dbconn.commit()
#
#         if a[2] != "":
#             if a[1] == '':
#                 a3 = urllib.parse.urlparse(url.full_url)
#             else:
#                 a3 = a
#             #print(a[2])
#             a2 = (a3[1],)
#             #print(a2)
#             c.execute('SELECT id FROM urls WHERE netloc=?', a2)
#             g = c.fetchone()
#             while g != None:
#                 s = (None, a[2], 0, g[0])
#                 print("adding: ", s, "to db")
#                 c.execute('INSERT INTO pages VALUES (?,?,?,?)', s)
#                 g = c.fetchone()
#
#     a = urllib.parse.urlparse(url.full_url)
#     a2 = (a[1],)
#     c.execute('UPDATE urls SET indexed = 1 where netloc = ?', a2)
#     dbconn.commit()
#
# c.execute('SELECT * FROM urls')
# g = c.fetchall()
# for line in g:
#     for a in line:
#         print(a)
#
# c.execute('SELECT * FROM pages')
# g = c.fetchall()
# for line in g:
#     for a in line:
#         print(a)
