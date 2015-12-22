from google.appengine.api import users
import webapp2
import logging
import base64
from model import Model
# google data store access
from google.appengine.ext import ndb
import re
import datetime
import random
import string
from google.appengine.ext.db import stats
from google.appengine.api import taskqueue



class BaseHandler(webapp2.RequestHandler):
    def handle_exception(self, exception, debug):
        # Log the error.
        
        logging.exception(exception)

        # Set a custom message.
        self.response.write('An error occurred.')

        # If the exception is a HTTPException, use its error code.
        # Otherwise use a generic 500 error code.
        if isinstance(exception, webapp2.HTTPException):
            self.response.set_status(exception.code)
        else:
            self.response.set_status(500)


#from looking at app stats http://localhost:8080/_ah/stats/ it can be seen that this query constitutes a single DATASTORE_READ cost
def does_code_exist(code_to_find):
        search_key = ndb.Key(Model.__name__, code_to_find)        
        found_entity = search_key.get()
        if found_entity == None:
            return False
        else:
            return True


def WriteRecords(entity_write_count, entity_write_batch_size, prefix ='', fromTaskQueue = False):
    start = datetime.datetime.now()

    counter = 0
    while entity_write_count > 0:
        entity_write_list = []

        if entity_write_count < entity_write_batch_size:
            batch_size = entity_write_count
        else:
            batch_size = entity_write_batch_size
        
        entity_write_count -= entity_write_batch_size

        if(batch_size > 1):
            for x in range(0, batch_size):  
                k = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(6))
                entity_write_list.append(Model( code = k, attr1 = '12345', id=k))
            #put entities to datastore
            ndb.put_multi(entity_write_list)
        else:
            #batch size of 1
            while True:
                k = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(6))
                k = prefix + k
                if does_code_exist(k) == False:
                    break
                else:
                    logging.info('code conflict ' + k + ' already exists.')
                break;    

            #logging.info('writing new code (#%d) %s' % (counter, k))
            ds = Model( code = k, attr1 = '12345', id=k)
            counter=counter+1
            ds.put()

    end = datetime.datetime.now()
    if(fromTaskQueue):
        logging.info ('written %d entities to datastore in taskqueue in %s mS' % (counter, str((end-start).total_seconds() * 1000)))

class DataStoreTest(BaseHandler):

   
    def get(self):

        #path param 1  = total number of data store writes
        #path param 2  = batch size used for multi-writes
        #path param3  = batch size used for page fetch
        #path param4  = number of writes to do using the task queue

        #E.g. http://localhost:8080/?writetotal=1000&writebatch=1000&fetchbatch=1000&taskwrite=1

        path = re.sub('^/', '', self.request.path)
        path = re.sub('/$', '', path)

                
        split = path.split('/')

        logging.info(split)
                 
        countDataStoreWriteTotalParam = 0
        countDataStoreWriteBatchParam = 1000
        countDataStoreFetchBatchParam = 1000
        taskQueueWrites = 0;

        temp = self.request.get("writetotal")
        if temp != '':
            countDataStoreWriteTotalParam = int(temp)

        temp = self.request.get("writebatch")
        if temp != '':
            countDataStoreWriteBatchParam = int(temp)

        temp = self.request.get("fetchbatch")
        if temp != '':
            countDataStoreFetchBatchParam = int(temp)

        temp = self.request.get("taskwrite")
        if temp != '':
            taskQueueWrites = int(temp)

        processing_start = datetime.datetime.now()
        responseMsg = '<p>Data store test starting (timestamp = %s).....countDataStoreWriteTotalParam = %d, countDataStoreWriteBatchParam = %d, countDataStoreFetchBatchParam = %d, taskQueueWrites = %d' % (str(processing_start), countDataStoreWriteTotalParam, countDataStoreWriteBatchParam, countDataStoreFetchBatchParam, taskQueueWrites)
        if True:
            #1 batch put to the datastore
            
            entity_write_count = countDataStoreWriteTotalParam
            entity_write_batch_size = countDataStoreWriteBatchParam
            responseMsg += '<p>#1 starting datastore writes %d in batch size of %d' % (entity_write_count, entity_write_batch_size)
            

            start = datetime.datetime.now()
            if True:
                WriteRecords(entity_write_count, entity_write_batch_size, 'ur')

            end = datetime.datetime.now()
            responseMsg += '<p style="text-indent: 2em;">data store writes processing time (mS) = %s' % str((end-start).total_seconds() * 1000)
            
        
        if False:

            #2 read all keys using single fetch
            responseMsg += '<p>#2 starting datastore read all keys using single fetch'
            
            start = datetime.datetime.now()
            keys = Model.query().fetch(keys_only=True)
            entity_count = len(keys)        

            end = datetime.datetime.now()
            responseMsg += '<p style="text-indent: 2em;">entity count %i, processing time (mS) = %s' % (entity_count, str((end-start).total_seconds() * 1000))


        

        key_in_dataset = None
        if False:
            #3 read all keys from data store using paged fetch

            fetch_page_size = countDataStoreFetchBatchParam
            if fetch_page_size <= 0:
                fetch_page_size = 1000

            responseMsg += '<p>#3 starting datastore read all keys using paged fetch using batch size of ' + str(fetch_page_size)
            
            start = datetime.datetime.now()
            pages_fetched = 0
            entities_fetched = 0

            #for the while loop
            has_more = True
            cursor = None
            while has_more:
                entities, cursor, more = Model.query().fetch_page(fetch_page_size,
                                                    start_cursor=cursor, keys_only=True)
                pages_fetched += 1
                entities_fetched += len(entities)        
                has_more = cursor and more

                if has_more == False:
                    key_in_dataset = entities[0]

            end = datetime.datetime.now()
            responseMsg += '<p style="text-indent: 2em;">Fetch page called %i times, entities fetched %i, processing time (mS) = %s' % (pages_fetched, entities_fetched, str((end-start).total_seconds() * 1000))      

        single_entity = None
        if False and key_in_dataset != None:

            #4 get single key using urlsafe key string

            responseMsg += '<p>#4 starting datastore get single key. Key = %s, does key exist = %s' % (str(key_in_dataset.id()), does_code_exist(key_in_dataset.id()))
            start = datetime.datetime.now()
            entity_key = ndb.Key(urlsafe=key_in_dataset.urlsafe())
            single_entity = entity_key.get()
            end = datetime.datetime.now()
            responseMsg += '<p style="text-indent: 2em;">Entity retrieved %s, processing time (mS) = %s' % (str(single_entity), str((end-start).total_seconds() * 1000))  

        if False and single_entity != None:
            responseMsg += '<p>#5 starting datastore get single entity from query %s ' % str(single_entity.code)
            start = datetime.datetime.now()
            qryResult = Model.query(Model.code == single_entity.code).fetch()

            results = None
            
            end = datetime.datetime.now()
            responseMsg += '<p style="text-indent: 2em;">Entities retrieved %d, processing time (mS) = %s' % (len(qryResult), str((end-start).total_seconds() * 1000))  

        if True and taskQueueWrites > 0:

            #push item onto queue to create records in datastore
            responseMsg += '<p>#5 Enqued task queue item to write %d,entities' % taskQueueWrites   
            taskqueue.add(url='/taskEnq', params={'countDataStoreWriteTotalParam': taskQueueWrites})


        end = datetime.datetime.now()
        responseMsg += '<p style="text-indent: 2em;">total processing time (mS) = %s' % str((end-processing_start).total_seconds() * 1000)
                               

        self.response.write(responseMsg)

class TaskEnqHandler(BaseHandler):

    #from experimentation  
        # time taken to write 1000 entries = 84s
        # time tkane to write 1,000 enties > 10mins => exceeds the 10min cut off for a task associated to an automatically scaled module.
   
    def post(self):
        writeTotal = int(self.request.get('countDataStoreWriteTotalParam'))
        logging.info('TaskEnqHandler writeTotal = %d' % writeTotal)
        if(writeTotal > 0):
            WriteRecords(writeTotal, 1, 'tq', True)

  
class CronReplenishEnqHandler(BaseHandler):

    #from experimentation  
        # time taken to write 1000 entries = 84s
        # time tkane to write 1,000 enties > 10mins => exceeds the 10min cut off for a task associated to an automatically scaled module.
   
    def get(self):
        kind_stats = stats.KindStat().all().filter("kind_name =", "Model").get()
        
        if(kind_stats == None):
            count = 0
        else:
            count = kind_stats.count
        logging.info('CronReplenishEnqHandler invoked count of records = %d' % count)
 


logging.getLogger().setLevel(logging.DEBUG)

app = webapp2.WSGIApplication([
    ('/taskEnq', TaskEnqHandler),
    ('/cron_replenish', CronReplenishEnqHandler),
    ('/.*', DataStoreTest)    
    
], debug=True)

