

import re
import math
import requests
import json
from rdflib import Graph
from datetime import datetime
from bson.objectid import ObjectId
import traceback
from rdflib import Graph
from fastapi import BackgroundTasks, File, UploadFile, HTTPException, APIRouter
from pydantic import BaseModel
from typing import Optional
from datetime import datetime
from bson.objectid import ObjectId
import requests
from pymongo_get_database import get_database
from mqa_calculators import *
from minio_manager import *
import aiohttp

submitRouter = APIRouter()

# converts the metric to a string containing just the name of the metric, ex: dct:title
def str_metric(val, g):
  valStr=str(val)
  for prefix, ns in g.namespaces():
    if val.find(ns) != -1:
      metStr = valStr.replace(ns,prefix+":")
      return metStr

# find the nth occurrence of a substring in a string
def find_nth(haystack: str, needle: str, n: int) -> int:
    start = haystack.find(needle)
    while start >= 0 and n > 1:
        start = haystack.find(needle, start+len(needle))
        n -= 1
    return start

# Base model
class Options(BaseModel):
    xml: Optional[str] = None
    file_url: Optional[str] = None
    url: Optional[str] = None
    id: Optional[str] = None
# api to start a new analisys and save on db the results for both case catalogue and dataset
# accept only rdf files, as string or by url, or by file in the submit/file api
# can specify the id of the catalogue or dataset if it was already created before
# the analisys can be long, so it is sent to the user a message that the request has been accepted and if new analisys it also returns the id of the new catalogue or dataset
@submitRouter.post("/")
async def useCaseConfigurator(options: Options, background_tasks: BackgroundTasks):
    try:
        configuration_inputs = options
    except Exception as e:
        print(traceback.format_exc())
        raise HTTPException(status_code=400, detail="Inputs not valid")
    try:
      if configuration_inputs.xml == None and configuration_inputs.file_url == None:
        return HTTPException(status_code=400, detail="Inputs not valid")
      elif configuration_inputs.xml != None:
        xml = configuration_inputs.xml
      else:
        async with aiohttp.ClientSession() as session:
            async with session.get(configuration_inputs.file_url) as response:
                if response.status != 200:
                    raise HTTPException(status_code=400, detail="Failed to fetch file from URL")
                xml = await response.text()

# sort the datasets and distributions tags to avoid problems with the rdf parser
      dataset_start = [m.start() for m in re.finditer('(?=<dcat:Dataset)', xml)]
      dataset_finish = [m.start() for m in re.finditer('(?=</dcat:Dataset>)', xml)]
      if len(dataset_start) != len(dataset_finish):
        return HTTPException(status_code=400, detail="Could not sort datasets")
      
      distribution_start = [m.start() for m in re.finditer('(?=<dcat:distribution>)', xml)]
      distribution_finish = [m.start() for m in re.finditer('(?=</dcat:distribution>)', xml)]
      if len(distribution_start) != len(distribution_finish):
        return HTTPException(status_code=400, detail="Could not sort distributions")
      
      # on rdf files the xml tag is not always present, so it is necessary to check if it is present and if it is not
      # the rdf files is always present, and need to be added for parsing, even with xml tag if present
      # if xml tag is present, the closing of rdf tag is the second '>' present in the file otherwise it is the first one (closing_index)
      closing_index = 2
      if xml.rfind('<?xml', None, 10) == -1:
        closing_index = 1

      pre = xml[:find_nth(xml,'>',closing_index) ] + '>'

      # check if the xml is valid
      test_string = pre + xml[dataset_start[0]:dataset_finish[0]+15] + '</rdf:RDF>'
      dt_copy = xml

      if xml.rfind('<dcat:Catalog ') != -1:
        # cut off all the tags on catalogue level, and leave just the tags on dataset level to analyze them separately
        for index, item in enumerate(dataset_start):
          dataset_Tag = xml[dataset_start[index]:dataset_finish[index]+15]
          # create a copy with just the catalogue tags to analyze them separately
          dt_copy = dt_copy.replace(dataset_Tag, '')
      else:
        # cut off all the tags on datasets level, and leave just the tags on distribution level to analyze them separately
        for index, item in enumerate(dataset_start):
          distr_tag = xml[distribution_start[index]:distribution_finish[index]+20]
          # cut off the distribution tag from the dataset string to obtain just the dataset properties to analyze them separately
          dt_copy = dt_copy.replace(distr_tag, '')
        dt_copy = dt_copy.replace(dt_copy[dt_copy.rfind('<adms:identifier>'):dt_copy.rfind('</adms:identifier>')+18], '')
      try:
        g = Graph()
        g.parse(data = test_string)
        g = Graph()
        g.parse(data = dt_copy)
      except:
        print(traceback.format_exc())
        return HTTPException(status_code=400, detail="Could not parse xml")
      
      title = ""
      # gets the title of the catalogue
      for sub, pred, obj in g:
        met = str_metric(pred, g)
        if met == "dct:title":
          title = obj
          break

      # Get the database
      try:
        dbname = get_database()
        collection_name = dbname["mqa"]
        now = datetime.now()
        # print(configuration_inputs.id)
        # check if the id is present, if it is not, it creates a new item in the db
        if configuration_inputs.id == None:
          if xml.rfind('<dcat:Catalog ') != -1:
            type = "catalogue"
          else: 
            type = "dataset"
          new_item = {
            "creation_date" : now.strftime("%d/%m/%Y %H:%M:%S"),
            "last_modified" : now.strftime("%d/%m/%Y %H:%M:%S"),
            "type": type,
            "title": title,
            "history": []
          }
          inserted_item = collection_name.insert_one(new_item)
          id = str(inserted_item.inserted_id)
        else:
          id = configuration_inputs.id
          # take the element in db by id and check if types correspond
          type = collection_name.find_one({'_id': ObjectId(id)})["type"]
          if xml.rfind('<dcat:Catalog ') != -1 and type == "dataset":
            return HTTPException(status_code=400, detail="The file is a catalogue, but the id is from a dataset")
          elif xml.rfind('<dcat:Catalog ') == -1 and type == "catalogue":
            return HTTPException(status_code=400, detail="The file is a dataset, but the id is from a catalogue")
          # check if in the db there are already 5 analisys, if yes, it deletes the oldest one
          if collection_name.find_one({'_id': ObjectId(id)})["history"] != None and len(collection_name.find_one({'_id': ObjectId(id)})["history"]) > 4:
            collection_name.update_one({'_id': ObjectId(id)},  {'$pop': {"history": -1}})
          collection_name.update_one({'_id': ObjectId(id)},  {'$set': {"last_modified": now.strftime("%d/%m/%Y %H:%M:%S")}})
      except:
        print(traceback.format_exc())
        id = None
        collection_name = None

      # start the analisys in background
      background_tasks.add_task(main, xml, pre, dataset_start, dataset_finish, configuration_inputs.url, collection_name, id)
      # send the response to the user
      if configuration_inputs.id != None:
        return {"message": "The request has been accepted"}
      else:
        return {"message": "The request has been accepted", "id" : id}
    except Exception as e:
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal Server Error" + str(e))
    


# Auth model
class Options(BaseModel):
    file_url: str = None
    url: Optional[str] = None
    id: Optional[str] = None
# api to start a new analisys and save on db the results for both case catalogue and dataset
# accept only rdf files, as string or by url, or by file in the submit/file api
# can specify the id of the catalogue or dataset if it was already created before
# the analisys can be long, so it is sent to the user a message that the request has been accepted and if new analisys it also returns the id of the new catalogue or dataset
@submitRouter.post("/auth")
async def useCaseConfigurator(options: Options, background_tasks: BackgroundTasks):
    try:
        configuration_inputs = options
    except Exception as e:
        print(traceback.format_exc())
        raise HTTPException(status_code=400, detail="Inputs not valid")
    try:
      if configuration_inputs.file_url == None:
        return HTTPException(status_code=400, detail="Inputs not valid")
      else:
        async with aiohttp.ClientSession() as session:
          try:
            async with session.get(configuration_inputs.file_url) as response:
              if response.status != 200:
                raise HTTPException(status_code=401, detail="Authentication error")
              xml = await response.text()
          except Exception as e:
            print(traceback.format_exc())
            raise HTTPException(status_code=401, detail="Authentication error" + str(e))

# sort the datasets and distributions tags to avoid problems with the rdf parser
      dataset_start = [m.start() for m in re.finditer('(?=<dcat:Dataset)', xml)]
      dataset_finish = [m.start() for m in re.finditer('(?=</dcat:Dataset>)', xml)]
      if len(dataset_start) != len(dataset_finish):
        return HTTPException(status_code=400, detail="Could not sort datasets")
      
      distribution_start = [m.start() for m in re.finditer('(?=<dcat:distribution>)', xml)]
      distribution_finish = [m.start() for m in re.finditer('(?=</dcat:distribution>)', xml)]
      if len(distribution_start) != len(distribution_finish):
        return HTTPException(status_code=400, detail="Could not sort distributions")
      
      # on rdf files the xml tag is not always present, so it is necessary to check if it is present and if it is not
      # the rdf files is always present, and need to be added for parsing, even with xml tag if present
      # if xml tag is present, the closing of rdf tag is the second '>' present in the file otherwise it is the first one (closing_index)
      closing_index = 2
      if xml.rfind('<?xml', None, 10) == -1:
        closing_index = 1

      pre = xml[:find_nth(xml,'>',closing_index) ] + '>'

      # check if the xml is valid
      test_string = pre + xml[dataset_start[0]:dataset_finish[0]+15] + '</rdf:RDF>'
      dt_copy = xml

      if xml.rfind('<dcat:Catalog ') != -1:
        # cut off all the tags on catalogue level, and leave just the tags on dataset level to analyze them separately
        for index, item in enumerate(dataset_start):
          dataset_Tag = xml[dataset_start[index]:dataset_finish[index]+15]
          # create a copy with just the catalogue tags to analyze them separately
          dt_copy = dt_copy.replace(dataset_Tag, '')
      else:
        # cut off all the tags on datasets level, and leave just the tags on distribution level to analyze them separately
        for index, item in enumerate(dataset_start):
          distr_tag = xml[distribution_start[index]:distribution_finish[index]+20]
          # cut off the distribution tag from the dataset string to obtain just the dataset properties to analyze them separately
          dt_copy = dt_copy.replace(distr_tag, '')
        dt_copy = dt_copy.replace(dt_copy[dt_copy.rfind('<adms:identifier>'):dt_copy.rfind('</adms:identifier>')+18], '')
      try:
        g = Graph()
        g.parse(data = test_string)
        g = Graph()
        g.parse(data = dt_copy)
      except:
        print(traceback.format_exc())
        return HTTPException(status_code=400, detail="Could not parse xml")
      
      title = ""
      # gets the title of the catalogue
      for sub, pred, obj in g:
        met = str_metric(pred, g)
        if met == "dct:title":
          title = obj
          break

      # Get the database
      try:
        dbname = get_database()
        collection_name = dbname["mqa"]
        now = datetime.now()
        # print(configuration_inputs.id)
        # check if the id is present, if it is not, it creates a new item in the db
        if configuration_inputs.id == None:
          if xml.rfind('<dcat:Catalog ') != -1:
            type = "catalogue"
          else: 
            type = "dataset"
          new_item = {
            "creation_date" : now.strftime("%d/%m/%Y %H:%M:%S"),
            "last_modified" : now.strftime("%d/%m/%Y %H:%M:%S"),
            "type": type,
            "title": title,
            "history": []
          }
          inserted_item = collection_name.insert_one(new_item)
          id = str(inserted_item.inserted_id)
        else:
          id = configuration_inputs.id
          # take the element in db by id and check if types correspond
          type = collection_name.find_one({'_id': ObjectId(id)})["type"]
          if xml.rfind('<dcat:Catalog ') != -1 and type == "dataset":
            return HTTPException(status_code=400, detail="The file is a catalogue, but the id is from a dataset")
          elif xml.rfind('<dcat:Catalog ') == -1 and type == "catalogue":
            return HTTPException(status_code=400, detail="The file is a dataset, but the id is from a catalogue")
          # check if in the db there are already 5 analisys, if yes, it deletes the oldest one
          if collection_name.find_one({'_id': ObjectId(id)})["history"] != None and len(collection_name.find_one({'_id': ObjectId(id)})["history"]) > 4:
            collection_name.update_one({'_id': ObjectId(id)},  {'$pop': {"history": -1}})
          collection_name.update_one({'_id': ObjectId(id)},  {'$set': {"last_modified": now.strftime("%d/%m/%Y %H:%M:%S")}})
      except:
        print(traceback.format_exc())
        id = None
        collection_name = None

      # start the analisys in background
      background_tasks.add_task(main, xml, pre, dataset_start, dataset_finish, configuration_inputs.url, collection_name, id)
      # send the response to the user
      if configuration_inputs.id != None:
        return {"message": "The request has been accepted"}
      else:
        return {"message": "The request has been accepted", "id" : id}
    except Exception as e:
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal Server Error" + str(e))
    

  
# api to start a new analisys and save on db the results for both case catalogue and single dataset
# accept only rdf files as file (format-data). Can be sent as string or by url in the /submit api
# can specify the id of the catalogue or dataset if it was already created before
# the analisys can be long, so it is sent to the user a message that the request has been accepted and if new analisys it also returns the id of the new catalogue or dataset
@submitRouter.post("/file")
async def useCaseConfigurator(background_tasks: BackgroundTasks, file: UploadFile = File(...), url: Optional[str] = None, id: Optional[str] = None):
  try:
    xml = file.file.read()
    xml = xml.decode("utf-8")
    file.file.close()
    
# sort the datasets and distributions tags to avoid problems with the rdf parser
    try:
      dataset_start = [m.start() for m in re.finditer('(?=<dcat:Dataset)', xml)]
      dataset_finish = [m.start() for m in re.finditer('(?=</dcat:Dataset>)', xml)]
      if len(dataset_start) != len(dataset_finish):
        return HTTPException(status_code=400, detail="Could not sort datasets")
      
      distribution_start = [m.start() for m in re.finditer('(?=<dcat:distribution>)', xml)]
      distribution_finish = [m.start() for m in re.finditer('(?=</dcat:distribution>)', xml)]
      if len(distribution_start) != len(distribution_finish):
        return HTTPException(status_code=400, detail="Could not sort distributions")
      
      # on rdf files the xml tag is not always present, so it is necessary to check if it is present and if it is not
      # the rdf files is always present, and need to be added for parsing, even with xml tag if present
      # if xml tag is present, the closing of rdf tag is the second '>' present in the file otherwise it is the first one (closing_index)
      closing_index = 2
      if xml.rfind('<?xml', None, 10) == -1:
        closing_index = 1

      pre = xml[:find_nth(xml,'>',closing_index) ] + '>'

      # check if the xml is valid
      test_string = pre + xml[dataset_start[0]:dataset_finish[0]+15] + '</rdf:RDF>'
      dt_copy = xml


      if xml.rfind('<dcat:Catalog ') != -1:
        # cut off all the tags on catalogue level, and leave just the tags on dataset level to analyze them separately
        for index, item in enumerate(dataset_start):
          dataset_Tag = xml[dataset_start[index]:dataset_finish[index]+15]
          # create a copy with just the catalogue tags to analyze them separately
          dt_copy = dt_copy.replace(dataset_Tag, '')
      else:
        # cut off all the tags on datasets level, and leave just the tags on distribution level to analyze them separately
        for index, item in enumerate(dataset_start):
          distr_tag = xml[distribution_start[index]:distribution_finish[index]+20]
          # cut off the distribution tag from the dataset string to obtain just the dataset properties to analyze them separately
          dt_copy = dt_copy.replace(distr_tag, '')
        dt_copy = dt_copy.replace(dt_copy[dt_copy.rfind('<adms:identifier>'):dt_copy.rfind('</adms:identifier>')+18], '')
        
      try:
        g = Graph()
        g.parse(data = test_string)
        g = Graph()
        g.parse(data = dt_copy)
      except:
        print(traceback.format_exc())
        return HTTPException(status_code=400, detail="Could not parse xml")
      
      title = ""
      # gets the title of the catalogue
      for sub, pred, obj in g:
        met = str_metric(pred, g)
        if met == "dct:title":
          title = obj
          break
      
      # Get the database
      try:
        # check if the id is present, if it is not, it creates a new item in the db
        dbname = get_database()
        collection_name = dbname["mqa"]
        now = datetime.now()
        if id == None:
          if xml.rfind('<dcat:Catalog ') != -1:
            type = "catalogue"
          else: 
            type = "dataset"
          new_item = {
            "creation_date" : now.strftime("%d/%m/%Y %H:%M:%S"),
            "last_modified" : now.strftime("%d/%m/%Y %H:%M:%S"),
            "type": type,
            "title": title,
            "history": []
          }
          inserted_item = collection_name.insert_one(new_item)
          new_id = str(inserted_item.inserted_id)
        else:
          new_id = id
          # take the element in db by id and check if types correspond
          type = collection_name.find_one({'_id': ObjectId(new_id)})["type"]
          if xml.rfind('<dcat:Catalog ') != -1 and type == "dataset":
            return HTTPException(status_code=400, detail="The file is a catalogue, but the id is from a dataset")
          elif xml.rfind('<dcat:Catalog ') == -1 and type == "catalogue":
            return HTTPException(status_code=400, detail="The file is a dataset, but the id is from a catalogue")
          
          # check if in the db there are already 5 analisys, if yes, it deletes the oldest one
          if collection_name.find_one({'_id': ObjectId(new_id)})["history"] != None and len(collection_name.find_one({'_id': ObjectId(new_id)})["history"]) > 4:
            collection_name.update_one({'_id': ObjectId(new_id)},  {'$pop': {"history": -1}})
          collection_name.update_one({'_id': ObjectId(new_id)},  {'$set': {"last_modified": now.strftime("%d/%m/%Y %H:%M:%S")}})
      except:
        print(traceback.format_exc())
        new_id = None
        collection_name = None


      # start the analisys in background
      background_tasks.add_task(main, xml, pre, dataset_start, dataset_finish, url, collection_name, new_id)
      # send the response to the user
      if id != None:
        return {"message": "The request has been accepted"}
      else:
        return {"message": "The request has been accepted", "id" : new_id}
    except Exception as e:
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal Server Error" + str(e))
  except Exception as e:
      print(traceback.format_exc())
      return {"message": "There was an error uploading the file" + str(e)}
  
  
# main function, 
async def main(xml, pre, dataset_start, dataset_finish, url, collection_name, id):

# if the file is a catalogue, it needs to be analyzed on catalogue level, otherwise it needs to be analyzed just on dataset level
  if xml.rfind('<dcat:Catalog ') != -1:

    class Object(object):
      pass
    response = Object()
    response.datasets = []
    response.title = ''

    dt_copy = xml
    # cut off all the tags on catalogue level, and leave just the tags on dataset level to analyze them separately
    for index, item in enumerate(dataset_start):
      # variable pre is always required from rdf files and it contains at least the rdf tag: <rdf:RDF ...> and can also contain the xml tag: <?xml version="1.0"?> 
      dataset = pre + xml[dataset_start[index]:dataset_finish[index]+15] + '</rdf:RDF>'
      result = dataset_calc(dataset, pre)
      response.datasets.append(result)
      dataset_Tag = xml[dataset_start[index]:dataset_finish[index]+15]
      # create a copy with just the catalogue tags to analyze them separately
      dt_copy = dt_copy.replace(dataset_Tag, '')
      
    g = Graph()
    g.parse(data = dt_copy)

# gets the title of the catalogue
    for sub, pred, obj in g:
      met = str_metric(pred, g)
      if met == "dct:title":
        response.title = obj
        break
    
    # initial values to avoid some properties are missing
    response.issued = 0
    response.modified = 0
    response.keyword = 0
    response.theme = 0
    response.spatial = 0
    response.temporal = 0
    response.contactPoint = 0
    response.publisher = 0
    response.accessRights = 0
    response.accessRightsVocabulary = 0
    response.accessURL = []
    response.accessURL_Perc = 0
    response.downloadURL = 0
    response.downloadURLResponseCode = []
    response.downloadURLResponseCode_Perc = 0
    response.format = 0
    response.dctFormat_dcatMediaType = 0
    response.formatMachineReadable = 0
    response.formatNonProprietary = 0
    response.license = 0
    response.licenseVocabulary = 0
    response.mediaType = 0
    response.rights = 0
    response.byteSize = 0
    response.shacl_validation = 0
    response.score = {}

    countDataset = 0
    countDistr = 0
    tempArrayDownloadUrl = []
    tempArrayAccessUrl = []
    # iterate over the datasets metrics to count positive values
    for dataset in response.datasets:
      countDataset += 1
      if dataset.issuedDataset == True:
        response.issued += 1
      del dataset.issuedDataset
      if dataset.modifiedDataset == True:
        response.modified += 1
      del dataset.modifiedDataset
      if dataset.accessRights == True:
        response.accessRights += 1
      if dataset.accessRightsVocabulary == True:
        response.accessRightsVocabulary += 1
      if dataset.contactPoint == True:
        response.contactPoint += 1
      if dataset.publisher == True:
        response.publisher += 1
      if dataset.keyword == True:
        response.keyword += 1
      if dataset.theme == True:
        response.theme += 1
      if dataset.spatial == True:
        response.spatial += 1
      if dataset.temporal == True:
        response.temporal += 1
      if dataset.shacl_validation == True:
        response.shacl_validation += 1
      for distr in dataset.distributions:
        countDistr += 1
        if distr.issued == True:
          response.issued += 1
        if distr.modified == True:
          response.modified += 1
        if distr.byteSize == True:
          response.byteSize += 1
        if distr.rights == True:
          response.rights += 1
        if distr.license == True:
          response.license += 1
        if distr.licenseVocabulary == True:
          response.licenseVocabulary += 1
        if distr.downloadURL == True:
          response.downloadURL += 1
        tempArrayDownloadUrl.append(distr.downloadURLResponseCode)
        tempArrayAccessUrl.append(distr.accessURL)
        if distr.format == True:
          response.format += 1
        if distr.formatMachineReadable == True:
          response.formatMachineReadable += 1
        if distr.formatNonProprietary == True:
          response.formatNonProprietary += 1
        if distr.mediaType == True:
          response.mediaType += 1
        if distr.dctFormat_dcatMediaType == True:
          response.dctFormat_dcatMediaType += 1

    # percentage calculations, based on distributions counts
    if(countDistr > 0):
      response.byteSize = round(response.byteSize / countDistr * 100)
      response.rights = round(response.rights / countDistr * 100)
      response.license = round(response.license / countDistr * 100)
      response.downloadURL = round(response.downloadURL / countDistr * 100)
      list_unique = (list(set(tempArrayDownloadUrl)))
      for el in list_unique:
        if el in range(200, 399):
          response.downloadURLResponseCode_Perc += round(tempArrayDownloadUrl.count(el) / countDistr * 100)
        response.downloadURLResponseCode.append({"code": el, "percentage": round(tempArrayDownloadUrl.count(el) / countDistr * 100)})
      list_unique = (list(set(tempArrayAccessUrl)))
      for el in list_unique:
        if el in range(200, 399):
          response.accessURL_Perc += round(tempArrayAccessUrl.count(el) / countDistr * 100)
        response.accessURL.append({"code": el, "percentage": round(tempArrayAccessUrl.count(el) / countDistr * 100)})
      response.format = round(response.format / countDistr * 100)
      response.formatMachineReadable = round(response.formatMachineReadable / countDistr * 100)
      response.formatNonProprietary = round(response.formatNonProprietary / countDistr * 100)
      response.mediaType = round(response.mediaType / countDistr * 100)
      response.dctFormat_dcatMediaType = round(response.dctFormat_dcatMediaType / (countDistr*2) * 100)

    if(countDataset > 0):
      response.accessRights = round(response.accessRights / countDataset * 100)
      response.contactPoint = round(response.contactPoint / countDataset * 100)
      response.publisher = round(response.publisher / countDataset * 100)
      response.keyword = round(response.keyword / countDataset * 100)
      response.theme = round(response.theme / countDataset * 100)
      response.spatial = round(response.spatial / countDataset * 100)
      response.temporal = round(response.temporal / countDataset * 100)
      response.shacl_validation = round(response.shacl_validation / countDataset * 100)
    
    if(countDistr + countDataset > 0):
      response.issued = round(response.issued / (countDataset + countDistr) * 100)
      response.modified = round(response.modified / (countDataset + countDistr) * 100)

    if(response.license > 0):
      response.licenseVocabulary = round(response.licenseVocabulary / response.license * 100)

    if(response.accessRights > 0):
      response.accessRightsVocabulary = round(response.accessRightsVocabulary / response.accessRights * 100)


    weights = Object()
    # weights
    # full list of weight can be found https://data.europa.eu/mqa/methodology?locale=en
    weights.keyword_Weight = math.ceil(30 / 100 * response.keyword)
    weights.theme_Weight = math.ceil(30 / 100 * response.theme)
    weights.spatial_Weight = math.ceil(20 / 100 * response.spatial)
    weights.temporal_Weight = math.ceil(20 / 100 * response.temporal)
    weights.contactPoint_Weight = math.ceil(20 / 100 * response.contactPoint)
    weights.publisher_Weight = math.ceil(10 / 100 * response.publisher)
    weights.accessRights_Weight = math.ceil(10 / 100 * response.accessRights)
    weights.accessRightsVocabulary_Weight = math.ceil(5 / 100 * response.accessRightsVocabulary)
    weights.accessURL_Weight = math.ceil(50 / 100 * response.accessURL_Perc)
    weights.downloadURL_Weight = math.ceil(20 / 100 * response.downloadURL)
    weights.downloadURLResponseCode_Weight = math.ceil(30 / 100 * response.downloadURLResponseCode_Perc)
    weights.format_Weight = math.ceil(20 / 100 * response.format)
    weights.dctFormat_dcatMediaType_Weight = math.ceil(10 / 100 * response.dctFormat_dcatMediaType)
    weights.formatMachineReadable_Weight = math.ceil(20 / 100 * response.formatMachineReadable)
    weights.formatNonProprietary_Weight = math.ceil(20 / 100 * response.formatNonProprietary)
    weights.license_Weight = math.ceil(20 / 100 * response.license)
    weights.licenseVocabulary_Weight = math.ceil(10 / 100 * response.licenseVocabulary)
    weights.mediaType_Weight = math.ceil(10 / 100 * response.mediaType)
    weights.rights_Weight = math.ceil(5 / 100 * response.rights)
    weights.byteSize_Weight = math.ceil(5 / 100 * response.byteSize)
    weights.issued_Weight = math.ceil(5 / 100 * response.issued)
    weights.modified_Weight = math.ceil(5 / 100 * response.modified)
    weights.shacl_validation_Weight = math.ceil(30 / 100 * response.shacl_validation)

    weights.findability = weights.keyword_Weight + weights.theme_Weight + weights.spatial_Weight + weights.temporal_Weight
    weights.accessibility = weights.accessURL_Weight + weights.downloadURL_Weight + weights.downloadURLResponseCode_Weight
    weights.interoperability = weights.format_Weight + weights.dctFormat_dcatMediaType_Weight + weights.formatMachineReadable_Weight + weights.formatNonProprietary_Weight + weights.mediaType_Weight + weights.shacl_validation_Weight
    weights.reusability = weights.license_Weight + weights.licenseVocabulary_Weight + weights.contactPoint_Weight + weights.publisher_Weight + weights.accessRights_Weight + weights.accessRightsVocabulary_Weight 
    weights.contextuality = weights.rights_Weight + weights.byteSize_Weight + weights.issued_Weight + weights.modified_Weight

    weights.overall = weights.findability + weights.accessibility + weights.interoperability + weights.reusability + weights.contextuality

    response.score = weights.__dict__

  else:
    # if the file is a dataset, it needs to be analyzed on dataset level
    response = dataset_calc(xml, pre)

  class EmployeeEncoder(json.JSONEncoder): 
        def default(self, o):
            return o.__dict__

  # if the file is a catalogue and id is provided, it updates the catalogue history
  # id should not be none because if user did not provide it, it is generated by the system before calling main function
  if id != None and xml.rfind('<dcat:Catalog ') != -1:
    now = datetime.now()
    collection_name.update_one({'_id': ObjectId(id)},  {'$push': {"history": { "created_at": now.strftime("%d/%m/%Y %H:%M:%S"),"catalogue":json.loads(json.dumps(response, indent=4, cls=EmployeeEncoder)) } }}) 
    minio_saveFile(id, json.dumps(response, indent=4, cls=EmployeeEncoder))
  # if the file is a dataset and id is provided, it updates the dataset history
  elif id != None and xml.rfind('<dcat:Catalog ') == -1:
    now = datetime.now()
    collection_name.update_one({'_id': ObjectId(id)},  {'$push': {"history": { "created_at": now.strftime("%d/%m/%Y %H:%M:%S"),"dataset":json.loads(json.dumps(response, indent=4, cls=EmployeeEncoder)) } }})
    minio_saveFile(id, json.dumps(response, indent=4, cls=EmployeeEncoder))

# if url is provided, it sends the results of analisys to the url
  if url != None:
    # print("Sending request to", url)
    
    async with aiohttp.ClientSession() as session:
      async with session.post(url, json=json.dumps(response, indent=4, cls=EmployeeEncoder)) as res:
        # print("Status Code", res.status)
        return res
    
    # print("Status Code", res.status_code)
    return res
  else:
    return response
