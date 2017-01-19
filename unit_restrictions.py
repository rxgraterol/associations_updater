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


ASSOCIATION_BODY = '''{{"catalog_domain" : "{1}", "attributes" : {2} }}'''
ATTRIBUTE_TO_POST = '''{{"id": "{0}", "tags": [{1}], "groupId": "{2}" }}'''
ATTRIBUTE_TO_POST_FIXED_VALUES = '''{{"id": "{0}", "tags": [{1}], "fixed_values":{2}, "groupId": "{3}" }}'''
FIXED_VALUES_BODY = '{{ "value_id":"{0}","value_name":"{1}","fixed_categories":{2} }}'

def generateBody(categoryId, catalogDomain, attributes):
  '''Genera el post para relacionar categoría con  dominio'''
  return ASSOCIATION_BODY.format(categoryId, catalogDomain, attributes)

def generateAttribute(attributeid, tags, groupId):
  '''Genera el json para un atributo del array "attributes" en el cuerpo del curl'''
  return ATTRIBUTE_TO_POST.format(attributeid, tags, groupId)

def generateAttribute_fixed(attributeid, tags, groupId, fixedValues):
  '''Genera el json para un atributo del array "attributes" en el cuerpo del curl. Incluye el campo "fixed_values"'''
  return ATTRIBUTE_TO_POST_FIXED_VALUES.format(attributeid, tags, fixedValues, groupId)

def generateFixedValues(valueId, valueName, fixedCategories):  
  fixedCategories = str([str(n).strip() for n in fixedCategories.replace("[","").replace("]","").split(",")]).replace("'",'"')
  return FIXED_VALUES_BODY.format(valueId, valueName, fixedCategories)

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
  try:
    archivo = open(file, 'rb')
    reader = csv.DictReader(archivo)
  except:
    log(fecha + " - Archivo debe ser un .csv valido",True)
    print "Comenzando carga de atributos desde archivo"

  catalog_domain, attributeid, value, default = [""]*4

  try:
    for row in reader:
      try:
        print row
      except:
        error = True
        errorMessage = "exc " + str(sys.exc_info())
        log(errorMessage,True)
  except:
    error = True
    errorMessage = "exc " + str(sys.exc_info())
    log(errorMessage,True)
  


def dbSave(currentCategory, post_to_make):
  match = re.match(r"([a-z]+)([0-9]+)", currentCategory, re.I)
  if match:
    siteId = match.groups()[0]
  else:
    siteId = 'MLB'
  # Guardo con los otros POSTs
  category = "SELECT * FROM categories WHERE category_id LIKE '%s'" % currentCategory
  cursor.execute(category)

  if(cursor.rowcount == 0):
    print "Guardando Categoria: " + currentCategory
    log("Guardando " + post_to_make)
    add_association = "INSERT INTO categories (site_id, category_id, association_body) VALUES ('%s', '%s', '%s')" % (siteId, currentCategory, post_to_make)
  else:
    print "Actualizando Categoria: " + currentCategory
    log("Actualizando " + post_to_make)
    add_association = "UPDATE categories SET association_body = '%s' WHERE category_id LIKE '%s'" % (post_to_make, currentCategory)
  cursor.execute(add_association) 

def createAttribute(tagArray, attributeArray, categoryId, catalog_domain, attributeid, Required, Hidden, Allow_variations, Fixed, Variation_attribute, groupId, fixedValues):
  '''Crea un atributo y lo agrega a "attributeArray".'''
  #Comparo con los posibles TAGS que pueda tener ese atributo para la relación y aumento el array de tags 
  if(attributeid):
    if Required == 'Required':
      tagArray = tagArray + """ "required","""
    if Hidden == 'Hidden':
      tagArray = tagArray + """ "hidden","""
    if Allow_variations == 'Allow_variations':
      tagArray = tagArray + """ "allow_variations","""
    if Fixed == 'Fixed':
      tagArray = tagArray + """ "fixed","""
    if Variation_attribute == 'Variation_attribute':
      tagArray = tagArray + """ "variation_attribute","""

    tagArray = tagArray[:-1]
    tagArray = tagArray.replace('[ "', '["')

    
    if Fixed == 'Fixed' and fixedValues :
      attribute = generateAttribute_fixed(attributeid, tagArray, groupId, fixedValues)
    else:
      attribute = generateAttribute(attributeid, tagArray, groupId)
    
    attribute = attribute.replace("'{", "{")
    attribute = attribute.replace("}'", "}")
    log("creando " + attribute)
    attributeArray.append(attribute)

def createFixedValue(fixedArray, value_name, value_id, fixed_categories):
  value = generateFixedValues(value_id, value_name, fixed_categories)
  fixedArray.append(value)

def main(argv):
  global logFile
  global cursor
  global enviroment
  global file

  try:
    opts, args = getopt.getopt(argv, "hef:d", ["enviroment","help", "file"])
     
    for opt, arg in opts:
      if opt in ('-h', '--help'):
         print 'python unit_restrictions.py -f restricciones.csv -e development|production'
         sys.exit()
      if opt in ('-e', '--enviroment'):
         enviroment = arg
      if if opt in ('-f', '--file'):
         file = arg

    logFile = open("curlProductIdentifiers-Info.log", 'wb')
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
