#!/usr/bin/env python
# -*- coding: utf-8 -*-

from time import sleep
from decimal import Decimal
import MySQLdb
import urllib3; urllib3.disable_warnings()
import sys,datetime,json,csv,requests,unicodedata
import re
import sys
import getopt
import os
import pprint

ALLOWED_UNITS_BODY = '''{{"allowed_units": {0} }}'''
UNIT = '{{ "unit":"{0}","default":"{1}" }}'

def generateUnit(value, default):
  return UNIT.format(value, default)

def generateAllowedUnits(unitsArray):
  return ALLOWED_UNITS_BODY.format(unitsArray)  

def log(msg,should_print=False):
  '''Crea un log con le mensaje "msg" en el archivo de logs. Tambien imprime el mensaje si 'should_print' == True'''
  if should_print:
    print(msg+"\n")
  logFile.write(msg + '\n')
  logFile.flush()

def loadAttributesFromCSV():
  '''Carga los atributos del csv y genera los curl. Tambien los guarda en los archivos'''
  fecha = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
  log(fecha + " - Comenzando carga de atributos desde archivo",True)

  archivo = open(file, 'rb')
  reader = csv.DictReader(archivo)

  unitsArray = []
  catalog_domain, attributeid, value, default, rootCategory = [""]*5
  currentCatalog_domain, currentAttributeid, currentValue, currentDefault, currentRootCategory = [""]*5
  allowed_values_body = ''
  try:
    for row in reader:
      try:
        catalog_domain = row['catalog_domain'].strip()
        attributeid = row['attributeid'].strip()
        default = row['default'].strip()
        value = row['value'].strip()
        rootCategory = row['root_category'].strip()
        
        if (catalog_domain != currentCatalog_domain or currentAttributeid != attributeid or currentRootCategory != rootCategory):
          if len(unitsArray) > 0:
            allowed_values_body = createAllowedUnits(unitsArray)
            dbSave(currentCatalog_domain, allowed_values_body, currentAttributeid, currentRootCategory)
            unitsArray = []
          createUnit(value, default, unitsArray)
          
        else:
          if currentValue != value:
            createUnit(value, default, unitsArray)
        currentCatalog_domain = catalog_domain
        currentAttributeid = attributeid
        currentDefault = default
        currentValue = value
        currentRootCategory = rootCategory
      except:
        error = True
        errorMessage = "exc " + str(sys.exc_info())
        log(errorMessage,True)
  except:
    error = True
    errorMessage = "exc " + str(sys.exc_info())
    log(errorMessage,True)

  if len(unitsArray) > 0:
    allowed_values_body = createAllowedUnits(unitsArray)
    dbSave(currentCatalog_domain, allowed_values_body, currentAttributeid, currentRootCategory)
    unitsArray = []

def createUnit(value, default, unitsArray):
  unit = generateUnit(value, default)
  unitsArray.append(unit)  

def createAllowedUnits(unitsArray):
  allowed_units = generateAllowedUnits(unitsArray)
  allowed_units = allowed_units.replace("'{", "{")
  allowed_units = allowed_units.replace("}'", "}")  
  return allowed_units

def dbSave(currentCatalogDomain, allowed_units, currentAttributeid, currentRootCategory):
  # Guardo con los otros POSTs
  attribute = "SELECT * FROM attributes WHERE attribute_id LIKE '%s' AND catalog_domain LIKE '%s'" % (currentAttributeid, currentCatalogDomain)
  cursor.execute(attribute)

  if(cursor.rowcount == 0):
    print "Guardando Atributo: " + currentAttributeid
    log("Guardando " + allowed_units)
    add_allowed_units = "INSERT INTO attributes (catalog_domain, attribute_id, allowed_values, root_category) VALUES ('%s','%s', '%s', '%s')" % (currentCatalogDomain, currentAttributeid, allowed_units, currentRootCategory)
  else:
    print "Actualizando Atributo: " + currentAttributeid
    log("Actualizando " + allowed_units)
    add_allowed_units = "UPDATE attributes SET allowed_values = '%s', root_category='%s' WHERE attribute_id LIKE '%s' AND catalog_domain LIKE '%s'" % (allowed_units, currentRootCategory, currentAttributeid, currentCatalogDomain)
  cursor.execute(add_allowed_units) 

def main(argv):
  global logFile
  global cursor
  global enviroment
  global file

  try:
    opts, args = getopt.getopt(argv, "hef:d", ["enviroment","help", "file"])
     
    for opt, arg in opts:
      if opt in ('-h', '--help'):
         print 'python unit_restrictions.py -f archivo_con_restricciones.csv -e development|production'
         sys.exit()
      if opt in ('-e', '--enviroment'):
         enviroment = arg
      if opt in ('-f', '--file'):
         file = arg

    filename, file_extension = os.path.splitext(file)
    if file_extension != '.csv':
      log(fecha + " - ERROR: El archivo debe estar en formato .csv\n", True)
      return False      

    logFile = open(filename + ".log", 'wb')
    if enviroment == 'production':
      conn = MySQLdb.connect(host="172.16.125.57", port=6612,
                  user="classimig_WPROD",
                  passwd="yUYaq39yWo",
                  db="classimig")
      
    else:
      conn = MySQLdb.connect(host="localhost",
                  user="root",
                  passwd="megasitio",
                  db="classimig")

    cursor = conn.cursor()
    loadAttributesFromCSV()
    
    conn.commit()
    conn.close()
    logFile.close()


  except:
    errorMessage = "exc " + str(sys.exc_info())
    log(errorMessage)
    print errorMessage
    sys.exit(2)                     
    if conn:
      conn.close()
    if logFile:
      logFile.close()
    print errorMessage


if __name__ == "__main__":
  main(sys.argv[1:])
