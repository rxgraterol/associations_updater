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
  archivo = open("ProductIdentifiers.csv", 'rb')
  reader = csv.DictReader(archivo)

  attributeArray = []
  fixedArray = []
  error = False
  currentCategory, currentCatalogDomain, tagArray, categoryId, currentAttributeId, catalog_domain, attributeid, Required, Hidden, Allow_variations, Fixed, Variation_attribute, groupId, value_name, value_id, fixed_categories=[""]*16
  currentRequired, currentHidden, currentFixed, currentTags, currentgroupId, fixedValues, currentAllow_variations, currentVariation_attribute = [""]*8

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
            dbSave(currentCategory, post_to_make)
          createAttribute(tagArray, attributeArray, categoryId, catalog_domain, attributeid, Required, Hidden, Allow_variations, Fixed, Variation_attribute, groupId, fixedArray)
          attributeArray = [] 
          tagArray = ""
          fixedArray = []
          currentCategory = categoryId
          currentCatalogDomain = catalog_domain
        else:          
          if(currentAttributeId != attributeid):
            createAttribute(currentTags, attributeArray, currentCategory, currentCatalogDomain, currentAttributeId, currentRequired, currentHidden, currentAllow_variations, currentFixed, currentVariation_attribute, currentgroupId, fixedArray)
            fixedArray = []
          else:
            createFixedValue(fixedArray, value_name, value_id, fixed_categories) 
        currentAttributeId = attributeid
        currentTags = tagArray
        currentgroupId = groupId 
        currentVariation_attribute = Variation_attribute
        currentRequired = Required
        currentHidden = Hidden
        currentFixed = Fixed
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
    dbSave(currentCategory, post_to_make)
  
  if not error:
    fecha = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log(fecha + " - Finaliza La subida de bodys de asociacion. Ambiente: " + enviroment + "\n",True)
  else:
    log("ERROR En la carga de atributos y generación de curls\n",True)

  attributeArray = []
  fixedArray = []
  tagArray = "" 


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
    log("Guardando " + post_to_make, True)
    add_association = "INSERT INTO categories (site_id, category_id, association_body) VALUES ('%s', '%s', '%s')" % (siteId, currentCategory, post_to_make)
  else:
    log("Actualizando " + post_to_make, True)
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

    
    if Fixed == 'Fixed' and len(fixedValues) > 0 :
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

  try:
    opts, args = getopt.getopt(argv, "he:d", ["enviroment=","help"])
     
    for opt, arg in opts:
      if opt in ('-h', '--help'):
         log('python updateProductIdentifiers.py -e development|production', True)
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
    log(errorMessage, True)
    sys.exit(2)                     
    if conn:
      conn.close()
    if logFile:
      logFile.close()


if __name__ == "__main__":
  main(sys.argv[1:])
