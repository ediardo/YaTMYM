#!/usr/bin/env python2.7
from ConfigParser import SafeConfigParser
import os
import threading
import mysql.connector
import csv
import time
import argparse
import logging


# class GetOptions 
# reads configuration file
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

class Monitor:
  con = False
  cursor = False
  global_status = [] 
  global_variables = []
  interval = 1
  def __init__(self, u, p, h, P):
    try:
      self.con =  mysql.connector.connect(user=u, password=p, host=h, port=P)
      self.cursor = self.con.cursor()
      self.get_global_status()
      self.get_global_variables()
      self.create_log_files()
    except mysql.connector.Error as err:
      if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print "Something is wrong with your user name or password"
      elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print "Database does not exists"
      else:
        print err
  
  def set_interval(self,n):
    self.interval = n

  # creates logs files if they don't exist
  def create_log_files(self):
    if not os.path.exists("status.csv"):
      try:
        log_file = open("status.csv",'a')
        wr = csv.writer(log_file)
        wr.writerow(self.rows_to_headers(self.global_status))
        log_file.close()
      except IOError:
        print "Error: File could not be created."
        return 0
    if not os.path.exists("variables.csv"):
      try:
        log_file = open("variables.csv",'a')
        wr = csv.writer(log_file)
        wr.writerow(self.rows_to_headers(self.global_variables))
        log_file.close()
      except IOError:
        print "Error: File could not be created."
        return 0
     
  # appends a new line to log file
  def write_log_file(self, line, file_name):
    log_file = open(file_name, 'a')
    wr = csv.writer(log_file, quoting=csv.QUOTE_MINIMAL)
    wr.writerow(line)    

  # creates a list of words, needed for csv headers
  def rows_to_headers(self, rows):
    i = 0
    headers = []
    for row in rows:
      headers.append(row[0])
    headers.insert(0,"Datetime")
    return headers
  
  # executes query for global variables
  # do not like function name
  def get_global_variables(self):
    query = "SHOW GLOBAL VARIABLES"
    self.cursor.execute(query) 
    self.global_variables = self.cursor.fetchall()

  # executes query for global status
  def get_global_status(self):
    query = "SHOW GLOBAL STATUS"
    self.cursor.execute(query) 
    self.global_status = self.cursor.fetchall()
 
  def rows_to_list(self, rows):
    i = 0
    values = [] 
    values.append(time.strftime("%Y-%m-%d %H:%H:%S", time.gmtime()))
    for row in rows:
      values.append(row[1])              
    return values
   
  def start_monitor(self):
    self.get_global_status()
    self.get_global_variables()
    self.global_status = self.rows_to_list(self.global_status)
    self.global_variables = self.rows_to_list(self.global_variables)
    self.write_log_file(self.global_status, "status.csv")
    self.write_log_file(self.global_variables, "variables.csv")
    thread = threading.Timer(1 , self.start_monitor).start()
  

# Defaults file argument parser
parser = argparse.ArgumentParser(description="This script captures the GLOBAL STATUS & VARIABLES of a MySQl Server.")
parser.add_argument("--defaults-file", help="The options file where YaTMyM would find options")
args = parser.parse_args()
x = GetOptions(defaults_file = args.defaults_file)
x.read_options()
options = x.params
monitor = Monitor(u = options["user"], p = options["password"], h = options["host"], P = options["port"])
monitor.set_interval(options["interval"])
monitor.start_monitor()

