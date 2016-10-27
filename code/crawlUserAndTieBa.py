# -*- coding: cp936 -*-
import TiebaCrawler
import sqlite3
import time
import threading
import traceback

DB_PATH = '../data/tieba.db'

UPDATE_CMT = "UPDATE COMMENT SET UID = %s WHERE UNAME = '%s'"
UPDATE_CCMT = "UPDATE CCOMMENT SET UID = %s WHERE UNAME = '%s'"

INSERT_USER = "INSERT INTO USER(UID, UNAME, SEX, BIRTHDAY, " +\
              "BIRTHPLACE, ADDRESS, SCHOOL) VALUES " +\
              "(%s, '%s', '%s', '%s', '%s', '%s', '%s')"

SELECT_USER = "SELECT * FROM USER WHERE UNAME = '%s'"

INSERT_FOLLOWS = "INSERT INTO FOLLOWS(UNAME, FOLLOWNAME) " +\
                 "VALUES ('%s', '%s')"

SELECT_FOLLOWS = "SELECT * FROM FOLLOWS " +\
                "WHERE UNAME = '%s' AND FOLLOWNAME = '%s'"

INSERT_FANS = "INSERT INTO FANS(UNAME, FANSNAME) " +\
                 "VALUES ('%s', '%s')"

SELECT_FANS = "SELECT * FROM FANS " +\
                "WHERE UNAME = '%s' AND FANSNAME = '%s'"

INSERT_TIEBA = "INSERT INTO TIEBA(TNAME, SLOGAN, MEMNUM, INFONUM) " +\
                 "VALUES ('%s', '%s', %s, %s)"

SELECT_TIEBA = "SELECT * FROM TIEBA WHERE TNAME = '%s'"

INSERT_MEMBER = "INSERT INTO MEMBER(UID, TNAME, LEVEL, ISTOP) " +\
                 "VALUES (%s, '%s', %s, %s)"

SELECT_MEMBER = "SELECT * FROM MEMBER WHERE TNAME = '%s' AND UID = %s"

def hasUser(conn, UNAME):
    sql = SELECT_USER % UNAME
    cursor = conn.execute(sql)
    for row in cursor:
        return True
    return False

def sqlInsertTieba(conn, TNAME, SLOGAN, MEMNUM, INFONUM):
    sql = SELECT_TIEBA % TNAME.replace("'", "''")
    cursor = conn.execute(sql)
    for row in cursor:
        print 'same TIEBA'
        return
    sql = INSERT_TIEBA % (TNAME.replace("'", "''"), SLOGAN.replace("'", "''"),
                          MEMNUM, INFONUM)
    conn.execute(sql)

def sqlInsertMEMBER(conn, UID, TNAME, LEVEL, ISTOP):
    sql = SELECT_MEMBER % (TNAME.replace("'", "''"), UID)
    cursor = conn.execute(sql)
    for row in cursor:
        print 'same MEMBER'
        return
    sql = INSERT_MEMBER % (UID, TNAME.replace("'", "''"), LEVEL, ISTOP)
    conn.execute(sql)

def sqlInsertUser(conn, UID, UNAME, SEX, BIRTHDAY, BIRTHPLACE, ADDRESS, SCHOOL):
    if hasUser(conn, UNAME):
        print 'same USER'
        return
    sql = INSERT_USER % (int(UID), UNAME.replace("'", "''"),
                         SEX.replace("'", "''"), BIRTHDAY.replace("'", "''"),
                         BIRTHPLACE.replace("'", "''"), ADDRESS.replace("'", "''"),
                         SCHOOL.replace("'", "''"))
    conn.execute(sql)

def sqlInsertFOLLOWS(conn, UNAME, FOLLOWNAME):
    sql = SELECT_FOLLOWS % (UNAME.replace("'", "''"),
                            FOLLOWNAME.replace("'", "''"))
    cursor = conn.execute(sql)
    for row in cursor:
        print 'same FOLLOWS'
        return
    sql = INSERT_FOLLOWS % (UNAME.replace("'", "''"),
                            FOLLOWNAME.replace("'", "''"))
    conn.execute(sql)

def sqlInsertFANS(conn, UNAME, FANSNAME):
    sql = SELECT_FANS % (UNAME.replace("'", "''"),
                         FANSNAME.replace("'", "''"))
    cursor = conn.execute(sql)
    for row in cursor:
        print 'same FANS'
        return
    sql = INSERT_FANS % (UNAME.replace("'", "''"),
                            FANSNAME.replace("'", "''"))
    conn.execute(sql)

def selectUname():
    conn = sqlite3.connect(DB_PATH)

    sql = 'SELECT DISTINCT(LZ) FROM TIEZI WHERE LZ NOT IN (SELECT UNAME FROM USER)'
    cursor = conn.execute(sql)
    nameDict = {}
    for row in cursor:
        if row[0] not in nameDict:
            nameDict[row[0]] = 0
    
    sql = 'SELECT DISTINCT(UNAME) FROM COMMENT WHERE UNAME NOT IN (SELECT UNAME FROM USER)'
    cursor = conn.execute(sql)
    nameDict = {}
    for row in cursor:
        if row[0] not in nameDict:
            nameDict[row[0]] = 0
            
    sql = 'SELECT DISTINCT(UNAME) FROM CCOMMENT WHERE UNAME NOT IN (SELECT UNAME FROM USER)'
    cursor = conn.execute(sql)
    for row in cursor:
        if row[0] not in nameDict:
            nameDict[row[0]] = 0

    conn.close()
    return nameDict.keys()

def updateUserInfo(crawler, unames):
    conn = sqlite3.connect(DB_PATH)
    exist_tieba = {}
    for name in unames:
        try:
            print u'crawling ' + name
            # update uid
            uid = crawler.getUidByName(name)
            if hasUser(conn, name):
                print u'Has user ' + name
                continue
            sql = UPDATE_CMT % (uid, name)
            conn.execute(sql)
            sql = UPDATE_CCMT % (uid, name)
            conn.execute(sql)
        
            # insert user detail
            detail = {u'性别':'', u'生日':'', u'出生地':'',
                      u'居住地':'', u'大学':''}
            attrs = crawler.getUserDetail(name)
            for key, v in attrs:
                if key.decode('utf8') in detail:
                    detail[key.decode('utf8')] = v.decode('utf8')
            sqlInsertUser(conn, uid, name, detail[u'性别'], detail[u'生日'],
                          detail[u'出生地'], detail[u'居住地'], detail[u'大学'])

            '''
            # get follows and fans
            follows, fans = crawler.getFollowsAndFans(name)
            for follow in follows:
                sqlInsertFOLLOWS(conn, name, follow.decode('utf8'))
            for fan in fans:
                sqlInsertFANS(conn, name, fan.decode('utf8'))
            '''
            
            # get what tieba does user concern
            concerns = crawler.getConcernTieba(name)
            for tname, level in concerns:
                tname = tname.decode('utf8')
                if tname not in exist_tieba:
                    info = crawler.getTiebaInformation(tname)
                    sqlInsertTieba(conn, tname, info['slogan'],
                                   info['menNum'].replace(',', ''),
                                   info['infoNum'].replace(',', ''))
                    exist_tieba[tname] = info['drl']
                istop = 0
                if name in exist_tieba[tname]:
                    istop = 1
                sqlInsertMEMBER(conn, uid, tname, level, istop)
            conn.commit()
        except:
            #print traceback.print_exc()
            conn.close()
            conn = sqlite3.connect(DB_PATH)
    conn.close()

if __name__ == '__main__':
    # initial
    crawler = TiebaCrawler.TiebaCrawler()
    unames = selectUname()
    updateUserInfo(crawler, unames)
