#!/usr/bin/env python2.7
from ConfigParser import SafeConfigParser
import os
import threading
import mysql.connector
import csv
import time
import argparse


class GetOptions:

  option_file = None
  params = dict()  

  def __init__(self, defaults_file = None):
    if defaults_file is None:
      self.option_file = "/etc/my.cnf"          
    else:
      self.option_file = defaults_file

  def read_options(self):
    config = SafeConfigParser(allow_no_value = True)
    config.read(self.option_file)
    self.params = dict(config.items('yatmym'))

class MySQLMonitor:
  con = False
  cursor = False
  global_status = [] 
  global_variables = []
  interval = 1
  def __init__(self, u, p, h, P):
    try:
      self.con =  mysql.connector.connect(user=u, password=p, host=h, port=P)
      self.cursor = self.con.cursor()
      self.create_log_file(file_name = "status.csv")
    except mysql.connector.Error as err:
      if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print "Something is wrong with your user name or password"
      elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print "Database does not exists"
      else:
        print err
  
  def set_inverval(n):
    self.interval = n

  def create_log_file(self, file_name):
    if not os.path.exists(file_name):
      try:
        my_file = open(file_name,'a')
        wr = csv.writer(my_file)
        wr.writerow(self.rows_to_headers(self.get_global_status()))
        my_file.close()
      except IOError:
        print "Error: File could not be created."
        return 0
     

  def rows_to_headers(self,rows):
    i = 0
    headers = []
    for row in rows:
      headers.append(row[0])
    headers.insert(0,"Datetime")
    return headers

  def get_global_variables(self):
    query = "SHOW GLOBAL VARIABLES"
    self.cursor.execute(query) 
    self.global_status = self.cursor.fetchall()
  
  def get_global_status(self):
    query = "SHOW GLOBAL STATUS"
    self.cursor.execute(query) 
    return self.cursor.fetchall()
 
def _rows_to_list(rows):
  i = 0
  values = [] 
  values.append(time.strftime("%Y-%m-%d %H:%H:%S", time.gmtime()))
  for row in rows:
    values.append(row[1])              
  return values
parser = argparse.ArgumentParser(description="This script captures the GLOBAL STATUS & VARIABLES of a MySQl Server.")
parser.add_argument("--defaults-file", help="The options file where YaTMyM would find options")
args = parser.parse_args()

x = GetOptions(defaults_file = args.defaults_file)
x.read_options()
options = x.params
print options
monitor = MySQLMonitor(u = options["user"], p = options["password"], h = options["host"], P = options["port"])
monitor.set_interval = options["interval"]
monitor.get_global_status()
print  monitor.global_status
cxn = monitor.con

file_status = None
file_variables = None




def monitor_server(interval=1):
  v = _rows_to_list(_get_global_status())
  _append_to_file(v,"status.csv")
  thread = threading.Timer(1, monitor_server).start()

