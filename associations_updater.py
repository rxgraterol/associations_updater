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
ATTRIBUTE_TO_POST_FIXED_VALUES = '{{"id": "{0}", "tags": [{1}], "fixed_values": [{{ "value_id":"{2}","value_name":"{3}","fixed_categories":{4} }}], "groupId": "{5}" }}'

def generateBody(categoryId, catalogDomain, attributes):
  '''Genera el post para relacionar categoría con  dominio'''
  return ASSOCIATION_BODY.format(categoryId, catalogDomain, attributes)

def generateAttribute(attributeid, tags, groupId):
  '''Genera el json para un atributo del array "attributes" en el cuerpo del curl'''
  return ATTRIBUTE_TO_POST.format(attributeid, tags, groupId)

def generateAttribute_fixed(attributeid, tags, groupId, valueName, valueId,fixedCategories):
  '''Genera el json para un atributo del array "attributes" en el cuerpo del curl. Incluye el campo "fixed_values"'''
  fixedCategories = str([str(n).strip() for n in fixedCategories.replace("[","").replace("]","").split(",")]).replace("'",'"')
  return ATTRIBUTE_TO_POST_FIXED_VALUES.format(attributeid, tags, valueId, valueName, fixedCategories,groupId)

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
  archivo = open("ProductIdentifiers.csv", 'rb')
  reader = csv.DictReader(archivo)

  attributeArray = []
  error = False
  currentCategory, currentCatalogDomain, tagArray, categoryId, catalog_domain, attributeid, Required, Hidden, Allow_variations, Fixed, Variation_attribute, groupId, value_name, value_id, fixed_categories=[""]*15

  try:
    for row in reader:
      try:
        categoryId = row['categoryId'].strip()
        catalog_domain = row['catalog_domain'].strip()
        attributeid = row['attributeid'].strip()
        Required = row['Required'].strip()
        Hidden = row['Hidden'].strip()
        Allow_variations = row['Allow_variations'].strip()
        Fixed = row['Fixed'].strip()
        Variation_attribute = row['Variation_attribute'].strip()
        groupId = row['groupId'].strip()
        value_name = row['fixedValueName'].strip()
        value_id = row['fixedValueId'].strip()
        fixed_categories = row['fixedCategs'].strip()

        # Si me cambia la categoría que tengía o el catalog domain, entonces genero un POST con la información que tenía
        if currentCategory != categoryId or currentCatalogDomain != catalog_domain: 
          # cuando termina la combinacion categoría / atributo, guardo en archivo el curl a ejecutar.
          if len(attributeArray) > 0:
            # Genero el post para guardar en un archivo
            post_to_make = generateBody(currentCategory, currentCatalogDomain, attributeArray)
            post_to_make = post_to_make.replace("'{", "{")
            post_to_make = post_to_make.replace("}'", "}")
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
          attributeArray = [] 
          tagArray = ""
          currentCategory = categoryId
          currentCatalogDomain = catalog_domain
          createAttribute(tagArray, attributeArray, categoryId, catalog_domain, attributeid, Required, Hidden, Allow_variations, Fixed, Variation_attribute, groupId, value_name, value_id, fixed_categories)

        else:
          tagArray = ""
          createAttribute(tagArray, attributeArray, categoryId, catalog_domain, attributeid, Required, Hidden, Allow_variations, Fixed, Variation_attribute, groupId, value_name, value_id, fixed_categories)
      except:
        error = True
        errorMessage = "exc " + str(sys.exc_info())
        log(errorMessage,True)

  except:
    error = True
    errorMessage = "exc " + str(sys.exc_info())
    log(errorMessage,True)

  #Guardo el último curl cuando ya recorrí todas las filas del csv
  if len(attributeArray) > 0:
    # Genero el post para guardar en un archivo
    post_to_make = generateBody(currentCategory, currentCatalogDomain, attributeArray)
    post_to_make = post_to_make.replace("'{", "{")
    post_to_make = post_to_make.replace("}'", "}")
    log("guardando " + post_to_make)

    #print post_to_make
    # Guardo con los otros POSTs
    #curlFile.write(post_to_make)
  
  if not error:
    fecha = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log(fecha + " - Finaliza La subida de bodys de asociacion. Ambiente: " + enviroment + "\n",True)
  else:
    log("ERROR En la carga de atributos y generación de curls\n",True)

  attributeArray = []
  tagArray = "" 

def createAttribute(tagArray, attributeArray, categoryId, catalog_domain, attributeid, Required, Hidden, Allow_variations, Fixed, Variation_attribute, groupId, value_name, value_id, fixed_categories):
  '''Crea un atributo y lo agrega a "attributeArray".'''
  #Comparo con los posibles TAGS que pueda tener ese atributo para la relación y aumento el array de tags 
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

  
  if Fixed == 'Fixed' and fixed_categories and fixed_categories != "-" and ( (value_name and value_name != "-") or (value_id and value_id != "-") ):
    attribute = generateAttribute_fixed(attributeid, tagArray, groupId, value_name, value_id, fixed_categories)
  else:
    attribute = generateAttribute(attributeid, tagArray, groupId)

  log("creando " + attribute)
  attributeArray.append(attribute)

def main(argv):
  global logFile
  global cursor
  global enviroment

  try:
    opts, args = getopt.getopt(argv, "he:d", ["enviroment=","help"])
     
    for opt, arg in opts:
      if opt in ('-h', '--help'):
         print 'python updateProductIdentifiers.py -e development|production'
         sys.exit()
      elif opt in ('-e', '--enviroment'):
         enviroment = arg

    logFile = open("curlProductIdentifiers-Info.log", 'wb')
    if enviroment == 'production':
      conn = MySQLdb.connect()
      
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
