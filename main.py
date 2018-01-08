from __future__ import print_function

import os
import time
from multiprocessing import Process
from threading import Timer
import urllib.request
import pickle
import uuid
import numpy as np
from bluelens_spawning_pool import spawning_pool
from stylelens_object.objects import Objects
from stylelens_image.images import Images
from stylelens_index.indexes import Indexes
from stylelens_search_vector.vector_search import VectorSearch
import redis

from bluelens_log import Logging


AWS_OBJ_IMAGE_BUCKET = 'bluelens-style-object'
AWS_MOBILE_IMAGE_BUCKET = 'bluelens-style-mainimage'

HEALTH_CHECK_TIME = 60*10
TMP_MOBILE_IMG = 'tmp_mobile_full.jpg'
TMP_MOBILE_THUMB_IMG = 'tmp_mobile_thumb.jpg'

SPAWN_ID = os.environ['SPAWN_ID']
REDIS_SERVER = os.environ['REDIS_SERVER']
REDIS_PASSWORD = os.environ['REDIS_PASSWORD']
RELEASE_MODE = os.environ['RELEASE_MODE']
AWS_ACCESS_KEY = os.environ['AWS_ACCESS_KEY'].replace('"', '')
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY'].replace('"', '')

MAX_PROCESS_NUM = int(os.environ['MAX_PROCESS_NUM'])
VERSION_ID = os.environ['VERSION_ID']

DB_INDEX_HOST = os.environ['DB_INDEX_HOST']
DB_INDEX_PORT = os.environ['DB_INDEX_PORT']
DB_INDEX_NAME = os.environ['DB_INDEX_NAME']
DB_INDEX_USER = os.environ['DB_INDEX_USER']
DB_INDEX_PASSWORD = os.environ['DB_INDEX_PASSWORD']

DB_IMAGE_HOST = os.environ['DB_IMAGE_HOST']
DB_IMAGE_PORT = os.environ['DB_IMAGE_PORT']
DB_IMAGE_NAME = os.environ['DB_IMAGE_NAME']
DB_IMAGE_USER = os.environ['DB_IMAGE_USER']
DB_IMAGE_PASSWORD = os.environ['DB_IMAGE_PASSWORD']

DB_OBJECT_HOST = os.environ['DB_OBJECT_HOST']
DB_OBJECT_PORT = os.environ['DB_OBJECT_PORT']
DB_OBJECT_NAME = os.environ['DB_OBJECT_NAME']
DB_OBJECT_USER = os.environ['DB_OBJECT_USER']
DB_OBJECT_PASSWORD = os.environ['DB_OBJECT_PASSWORD']

REDIS_IMAGE_INDEX_QUEUE = 'bl:image:index:queue'

options = {
  'REDIS_SERVER': REDIS_SERVER,
  'REDIS_PASSWORD': REDIS_PASSWORD
}
log = Logging(options, tag='bl-image-indexer')
rconn = redis.StrictRedis(REDIS_SERVER, decode_responses=True, port=6379, password=REDIS_PASSWORD)


heart_bit = True
object_api = Objects()
search_api = VectorSearch()
search_api = VectorSearch()
image_api = Images()
index_api = Indexes()

def indexing(object_id):
  # log.info('indexing: ' + object_id)
  # log.info('VERSION_ID: ' + VERSION_ID)

  try:
    object = object_api.get_object(object_id, VERSION_ID)
    # log.debug('object:')
    # log.debug(object)

    objects = get_similar_objects(object)
    images = get_similar_images(objects)

    if images is None:
      return

    object['images'] = images
    object['image_indexed'] = True

    update_object_to_db(object)
    add_object_to_index_db(object)

    if 'is_main' in object:
      if object['is_main'] is True:
        image_id = object['image_id']
        image = image_api.get_image(image_id)
        # log.debug(image)
        image['images'] = images
        update_image_to_db(image)
        objects_in_image = []
        for obj_id in image['objects']:
          o = object_api.get_object(obj_id, VERSION_ID)
          objects_in_image.append(o)

        image['objects'] = objects_in_image
        add_image_to_index_db(image)
  except Exception as e:
    log.error(str(e))

def add_object_to_index_db(object):
  try:
    res = index_api.add_object(object)
  except Exception as e:
    log.error(str(e))

def add_image_to_index_db(image):
  try:
    res = index_api.add_image(image)
  except Exception as e:
    log.error(str(e))

def update_object_to_db(object):
  try:
    res = object_api.update_object(object)
  except Exception as e:
    log.error(str(e))

def update_image_to_db(image):
  try:
    res = image_api.update_image(image)
  except Exception as e:
    log.error(str(e))

def get_similar_objects(object):

  vector = object['feature']
  limit = 30
  try:
    vector_d, vector_i = search_api.search(vector, limit)
    distances = np.fromstring(vector_d, dtype=np.float32)
    ids = np.fromstring(vector_i, dtype=np.int)

    objects = []
    for i in ids:
      obj = object_api.get_object_by_index(int(i), VERSION_ID)
      obj.pop('feature')
      objects.append(obj)

    return objects
  except Exception as e:
    log.error(str(e))

def get_similar_images(objects):
  images = []
  limit = 10
  try:
    i = 0
    for object in objects:
      if i < limit:
        if 'is_main' in object:
          if object['is_main'] is True:
            image_id = object['image_id']
            image = image_api.get_image(image_id, VERSION_ID)
            images.append(image)
            i = i + 1

    return images
  except Exception as e:
    log.error(str(e))

  return images

def check_health():
  global  heart_bit
  log.info('check_health: ' + str(heart_bit))
  if heart_bit == True:
    heart_bit = False
    Timer(HEALTH_CHECK_TIME, check_health, ()).start()
  else:
    delete_pod()

def delete_pod():
  log.info('exit: ' + SPAWN_ID)

  data = {}
  data['namespace'] = RELEASE_MODE
  data['key'] = 'SPAWN_ID'
  data['value'] = SPAWN_ID
  spawn = spawning_pool.SpawningPool()
  spawn.setServerUrl(REDIS_SERVER)
  spawn.setServerPassword(REDIS_PASSWORD)
  spawn.delete(data)

def dispatch_job(rconn):
  log.info('Start dispatch_job')
  Timer(HEALTH_CHECK_TIME, check_health, ()).start()

  count = 0
  while True:
    key, value = rconn.blpop([REDIS_IMAGE_INDEX_QUEUE])
    start_time = time.time()
    indexing(value)

    elapsed_time = time.time() - start_time
    # log.info('image-indexing time: ' + str(elapsed_time))

    # if count > MAX_PROCESS_NUM:
    #   delete_pod()

    global  heart_bit
    heart_bit = True

if __name__ == '__main__':
  try:
    log.info('Start bl-image-indexer:3')
    dispatch_job(rconn)
  except Exception as e:
    log.error(str(e))
    delete_pod()
