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

    def execute(self, sql):
        s = (sql,)
        self.dbcu.execute(s)
        self.dbcommit()

    def insertURLs(self, urls):
        try:
            print("Inserting URL: ", urls)
            self.dbcu.execute('INSERT INTO urls VALUES (?,?,?,?,?)', urls)
        except sqlite3.IntegrityError as error:
            print("Exeption sqlite3.IntegrityError: ", error, "\nFor data: ", urls)
            self.dbc.rollback()
        finally:
            self.dbcommit()

    def updateURLs(self, netloc):
        self.dbcu.execute('UPDATE urls SET indexed = 1 WHERE netloc = ?', netloc)
        self.dbcommit()

    def insertPage(self, page):
        self.dbcu.execute('INSERT INTO pages VALUES (?,?,?,?)', page)
        self.dbcommit()

    def updatepages(self, page, pageurl):
        conditions = (page, pageurl)
        print("Page is ", page, " and type: ", type(page))
        print("Pageurl is ", pageurl, " and type: ", type(pageurl))
        print("Conditions is ", conditions, " and type: ", type(conditions))
        self.dbcu.execute("UPDATE pages SET indexed = 1 WHERE page = '" + page + "' AND pageurl = '" + pageurl + "'")
        self.dbcommit()

    def selectID(self, netloc):
        netloc2 = (netloc,)
        self.dbcu.execute('SELECT id FROM urls WHERE netloc= (?)', netloc2)

    def selectURL(self):
        self.dbcu.execute('SELECT netloc FROM urls WHERE indexed = 0')

    def selectPage(self, ID):
        print("SELECT pageurl ID...")
        # print("ID: ", ID, "type: ", type)
        self.dbcu.execute('SELECT page FROM pages WHERE indexed = "0" and pageurl = "' + ID + '"')
        print("Pageurl ID SELECT done!")

    def crafturllist(self):
        print("Crafting URL list...")
        self.selectURL()
        url = self.dbcu.fetchone()[0]
        # print("URL IS ::", url)
        self.selectID(url)
        id = str(self.dbcu.fetchone()[0])
        # print("ID IS :: ", id)
        self.selectPage(id)
        pages = self.dbcu.fetchall()
        # print(pages)
        urllist = list()
        for i in pages:
            # rint(i)
            # print(url + i[0])
            # print()
            urllist.append(url + i[0])
        # print("URl= ", urllist)
        print("URLlist Done!")
        return urllist

class parsing:
    def __init__(self):
        self.re1 = self.regex(r'href=[\'"]?([^\'" >]+)')
        self.re2 = self.regex(r'^/')

    ## regex patterns
    ## r'href=[\'"]?([^\'" >]+)'
    ## r'^/'

    def regex(self, pat):
        return re.compile(pat)

    def urlparse(self, url):
        return urllib.parse.urlparse(url)

    def now(self):
        return self.adapt_datetime(datetime.datetime.now())

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
    def request(self, url):
        print("req: ", url)
        return urllib.request.urlopen(self.urlcraft(url))

    def urlcraft(self, url):
        print("crafting: ", url)
        return urllib.request.Request(url, data=None)


class run:
    def __init__(self, dbtype):
        self.a = db(dbtype)
        self.b = parsing()
        self.c = req()

    def index(self, url):
        print("index: ", url)
        with self.c.request(url) as f:

            # now = datetime.datetime.now()
            print("Time: ", self.b.now())
            a = self.b.urlparse(url)
            s = (None, self.b.now(), a[0], a[1], 0)

            ## Dirty fix
            self.a.insertURLs(s)
            print("Adding ? to database", s)

            # dbconn.commit()

            # print(c.execute("SELECT id FROM urls WHERE netloc = 'www.iana.org'"))
            if a[2] != "":
                # print("Found macth: " + a[2])
                a2 = (a[1],)
                print("printing a2: ", a2)
                self.a.selectID(a[1])

                try:
                    g = self.a.dbcu.fetchone()
                except:
                    pass
                print("Printing g: ", g)
                s = (None, a[2], 0, g[0])
                self.a.insertPage(s)

                # c.execute('SELECT * FROM pages')
                # g = c.fetchall()
                # for line in g:
                #     for a in line:
                #         print(a)

            site = f.read().decode('utf-8')
            # print(site)
            # print(r.findall(site))
            for each in self.b.re1.findall(site):
                # print(each)
                # now = datetime.datetime.now()
                a = self.b.urlparse(each)
                s = (None, self.b.now(), a[0], a[1], 0)
                if a[1] != '':
                    self.a.insertURLs(s)
                    # print("Adding ? to database", s)

                if a[2] != "":
                    if a[1] == '':
                        a3 = self.b.urlparse(url)
                    else:
                        a3 = a
                    # print(a[2])
                    a2 = (a3[1],)

                    self.a.selectID(a3[1])
                    g = self.a.dbcu.fetchone()
                    while g != None:
                        s = (None, a[2], 0, g[0])
                        print("adding: ", s, "to db")
                        self.a.insertPage(s)
                        g = self.a.dbcu.fetchone()

            a = self.b.urlparse(url)
            print("A2 is ", a[2])
            if a[2] == '':
                print("No sub page!!!")
                a2 = (a[1],)
                self.a.updateURLs(a2)
            else:
                print("Subpage " + a[2] + " On url: " + a[1])
                netloc = a[1]
                print("netlock is :", netloc)
                self.a.selectID(a[1])
                # print("ID to page update", self.a.dbcu.fetchone()[0])
                print(type(a[2]))

                # print("ab = = ", ab)
                self.a.updatepages(a[2], str(self.a.dbcu.fetchone()[0]))


x = run('test.db')
x.a.crafturllist()
# x.a.DBcreate()
# x.a.insertURLs((None,100000,'HTTP','example.org',0))
# x.index('http://www.iana.org')

#x.a.dbdump()


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
