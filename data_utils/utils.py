import pickle
import pymongo
import os
from os import listdir
import json
from pprint import pprint
import sys
import io


# CHECK_ARGV FUNCTION
def check_argv(name):
  for a in sys.argv:
    if name == a:
      return True
  return False


# LOAD AND SAVE OBJECT FUNCTIONS:
class Pickle:
  @staticmethod
  def save_obj(obj, name):
    with open(name + '.pkl', 'wb') as f:
      pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)

  @staticmethod
  def load_obj(name):
    with open(name + '.pkl', 'rb') as f:
      return pickle.load(f)


# INTERACT WITH MONGODB
class MongoDB:
  @staticmethod
  def get_database(database_name):
    # Database = Database
    # Collection = tables
    # Doc = row, each doc may have its own ID
    from pymongo import MongoClient
    client = MongoClient('mongodb://admin:admin123@localhost:27017/')
    db = client[database_name]
    return db

  @staticmethod
  def get_collection(database_name, collection_name):
    db = MongoDB.get_database(database_name)
    return db[collection_name]

  @staticmethod
  def get_documents(database_name, collection_name, find_one=False, condition=None):
    # return cursor can be iterated
    # condition like : {'authors': 'vinhsuhi'}
    collection = MongoDB.get_collection(database_name, collection_name)
    if find_one:
      return collection.find_one(condition)
    return collection.find(condition)

  @staticmethod
  def insert_documents(database_name, collection_name, doc, insert_one=True, insert_id=False):
    collection = MongoDB.get_collection(database_name, collection_name)
    if insert_one:
      if insert_id:
        post_id = collection.insert_one(doc).inserted_id
        return post_id
      collection.insert_one(doc)
      return None
    else:
      result = collection.insert_many(doc)
      return result.inserted_ids


# INTERACT WITH JSON:
class JsonObject:
  @staticmethod
  def load_json_data(path):
    # INPUT: direct path of the json file (e.g. ./lol/data.json)
    # OUTPUT: dictionary
    with open(path) as f:
      data = json.load(f)
    return data

  @staticmethod
  def save_json_data(data, path):
    # INPUT: path as .txt file (e.g. ./lol/data.txt)
    with io.open(path, 'w', encoding='utf-8') as outfile:
      outfile.write(json.dumps(data, ensure_ascii=False))


# READ BINARY FILE
def read_bi(path):
  import struct
  with open(path, 'rb') as f:
    # data = f.read()
    i = 0
    for line in f:
      i += 1
      print(i)
      data = line.decode("utf-8")
      json_data = json.loads(data)
      print(json_data)
  # return data.decode("utf-8")


