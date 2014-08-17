#!/usr/bin/env python2.7
from ConfigParser import SafeConfigParser
import os
import threading
import mysql.connector
import csv
import time

options = dict()
cxn = False
file_status = None
file_variables = None

def read_options():
  config = SafeConfigParser(allow_no_value = True)
  config.read('/mysql/5.6.14_3306/my.cnf')
  print config.items('yatmom')


def connect_mysql():
  try:
    return mysql.connector.connect(user='yatmom', password='dummypass', host='127.0.0.1')
  except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
      print "Something is wrong with your user name or password"
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
      print "Database does not exists"
    else:
      print err
  else:
    cnx.close()

def _append_to_file(line, f):
  myfile = open(f, 'a')
  wr = csv.writer(myfile, quoting=csv.QUOTE_MINIMAL)  
  wr.writerow(line)

def create_log_file(file_name, headers):
  if not os.path.exists(file_name):
    try:
      my_file = open(file_name, 'a')
      wr = csv.writer(my_file)
      wr.writerow(_rows_to_headers(headers))
      my_file.close()
    except IOError:
      print "Error: File could not be created."
      return 0
    
def set_headers(headers, file_name):
  my_file = open(file_name, 'a')
  wr = csv.writer(my_file)
  wr.writerow(_rows_to_headers(_get_global_status()))
 
def _rows_to_headers(rows):
  i = 0
  headers = []
  for row in rows:
    headers.append(row[0])
  headers.insert(0,"Datetime")
  return headers

def _rows_to_list(rows):
  i = 0
  values = [] 
  values.append(time.strftime("%Y-%m-%d %H:%H:%S", time.gmtime()))
  for row in rows:
    values.append(row[1])              
  return values

def _get_global_status():
  query = "SHOW GLOBAL STATUS"
  cursor = cnx.cursor()
  cursor.execute(query) 
  return cursor.fetchall()

def monitor_server(interval=1):
  v =  _rows_to_list(_get_global_status())
  _append_to_file(v,"status.csv")
  thread = threading.Timer(1, monitor_server).start()

read_options()
cnx = connect_mysql()
create_log_file("status.csv", _get_global_status())
monitor_server(1)
