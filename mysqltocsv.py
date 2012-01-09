#!/usr/bin/env python
# -*- coding: utf-8 -*-

import MySQLdb, csv, sys
from optparse import OptionParser

class MySQLtoCSV:

    hostname=""
    port=""
    username=""
    password=""
    database=""
    query=None

    delimiter='\t'
    doublequote=True
    escapechar=None
    lineterminator='\r\n'
    quotechar='"'
    quoting=csv.QUOTE_MINIMAL
    
    def __init__(self, opt):
        self.hostname = opt.host_name
        self.port     = opt.port_num
        self.socket   = opt.path
        self.username = opt.user_name
        self.password = opt.password
        self.database = opt.db_name
        self.query    = opt.statement

        self.delimiter     = opt.delimiter
#        self.doublequot   = opt.doublequote
        self.escapechar    = opt.escapechar
        self.lineterminator = opt.lineterminator
        self.quotechar     = opt.quotechar
        self.quoting       = csv.QUOTE_MINIMAL

        self.conn = None 
        
        # Read Query from stdin
        if self.query == None:
            self.query = sys.stdin.read()


    def mysqltocsv(self):

        try:
            self.conn = MySQLdb.connect (host = self.hostname, port = self.port, user = self.username, passwd = self.password, db = self.database )

        except (TypeError, MySQLdb.OperationalError) as err:
            print "ERROR : MySQL Connection :", err
            exit( 2 )
            
        cursor = self.conn.cursor ()

        try:
            tabledata = csv.writer( sys.stdout , delimiter=self.delimiter, lineterminator = self.lineterminator, 
                                    quotechar = self.quotechar, quoting = self.quoting )
        except TypeError as err:  
            print "ERROR : CSV Writer :", err 
            exit( 3 )

        try:
            cursor.execute ( self.query )
            while (1):
                row = cursor.fetchone ()
                if row == None:
                    break
                tabledata.writerow ( row ) 
        except (TypeError, MySQLdb.ProgrammingError) as err:
            print "ERROR : MySQL Query :", err
            exit( 4 )


        cursor.close ()
        self.conn.close ()

if __name__ == '__main__':

    parser = OptionParser()
    parser.add_option("--host", "-H", dest="host_name", default="localhost" , help="MySQL Hostname")
    parser.add_option("--port", "-P", dest="port_num", default=3306, help="MySQL Port", type="int")
    parser.add_option("--socket", "-S", dest="path", help="MySQL Socket")
    parser.add_option("--user", "-u", dest="user_name", help="MySQL Username")
    parser.add_option("--password", "-p", dest="password" , help="MySQL Password")
    parser.add_option("--database", "-D", dest="db_name" , help="MySQL Database")
    parser.add_option("--execute", "-e", dest="statement" , help="MySQL SELECT Query")

    parser.add_option("--fields-enclosed-by", dest="quotechar", default='"' , help="CSV Enclosed Character")
    parser.add_option("--fields-escaped-by", dest="escapechar", default=None , help="CSV Escaped Character")
#    parser.add_option("--fields-optionally-enclosed-by", default= , dest="", help="CSV Optionally Enclosed Character")
    parser.add_option("--fields-terminated-by", dest="delimiter", default="\t" , help="CSV Field Terminated by")
    parser.add_option("--lines-terminated-by", dest="lineterminator", default="\r\n" , help="CSV Lines Terminated by")

    (options, args) = parser.parse_args()

    mycsv = MySQLtoCSV(options)

    mycsv.mysqltocsv()

