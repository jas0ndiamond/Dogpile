import logging
import queue
import time
import timeit

from threading import Thread, Lock

from pprint import pprint, pformat
from pymongo import MongoClient
from bson import ObjectId

from JobManager import JobManager

logFile = "run.log"

MAX_INTAKE_SIZE = 2 * 1000 * 1000

#On-disk management of job state data
#TODO: refactor naming of functions like 'addJob' to 'addState' and similar
class MongoDBJobManager(JobManager):
    
    def __init__(self, db_host, db_port, db_user, db_pass, db_name, collection_name, job_serializer):
        
        logging.basicConfig(filename=logFile, format='%(asctime)s [%(levelname)s] -- [%(name)s]-[%(funcName)s]: %(message)s')
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel( logging.DEBUG )
        
        self.client = MongoClient(host=db_host, port=db_port, username=db_user, password=db_pass, authSource='admin', authMechanism='SCRAM-SHA-1', w=0)
        
        #TODO default value for collection_name. cannot be empty string
        
        #expect db_user to be bound to db_name
        #require at least one named collection for the database
        self.database = self.client[db_name]
        self.dispy_work_collection = self.database[collection_name]
        
        self.logger.info("Building MongoDBJobManager for collection %s at %s:%d" % (collection_name, db_host, db_port) )
        
        #TODO checks on this. should be a function
        self.jobSerializer = job_serializer
        
        self.jobRetrievalLock = Lock()
        
        self.intake = queue.Queue(maxsize=MAX_INTAKE_SIZE)
        
        self.closed = False
        
        self.runningIntake = True
        self.intakeLock = Lock()
        
        self.intakeThread = Thread(target=self.processIntake)
                
        self.intakeThread.start()
        
    def getIntakeQueueSize(self):
        return self.intake.qsize()
        
    def addToIntake(self, job):
        
        # possible since this is a queue we don't need to lock
        
        self.intake.put(job)
        
        
    #process intake queue in background, so each external job add doesn't incur a db write and block the dispy job callback
    def processIntake(self):
        intakeSleep = 1
        #minClearSize = 100000
        
        intakeBlock = 20 * 1000
        
        while(self.runningIntake):         
            
            #intakeQueueSize = self.intake.qsize()
            
            #if queue is filled up faster than it can be cleared, then this loop may never exit
            #also need to allow first jobs to be processed or they might never make it to 
            #the database, and won't ever be available for retrieval
            #if( intakeQueueSize > minClearSize or intakeQueueSize < 100):
            
            #currently experimenting with async writes => not super helpful
            
            #copy and empty the queue contents, and write to database. break loop if we're shutting down
            
            i = 0
            while(self.intake.empty() == False and self.runningIntake and i < intakeBlock ):
                
                if self.logger.isEnabledFor(logging.DEBUG):
                    self.logger.debug("Intake queue size: %d" % self.getIntakeQueueSize() )
                
                
                
                #TODO: adding jobs in chunks, without explicit breaks, this can run for long periods
                #likely this is what causes random stalls as it gets resized
                
                self.addJob(self.intake.get())
                
                i += 1
                        
            # sleep so we're not spinning if there's a lull in intake work
            time.sleep(intakeSleep)
        
        self.logger.info("Intake Queue Thread exiting")
            
    def addJob(self, job):
        
        #TODO: instance check - job should be a subtype of ClusterJobResult
        #ClusterJobResult require toDict method?
        
        #result = self.dispy_work_collection.insert_one(job.toDict())
        
        #if(result.acknowledged == False):
        #    self.logger.error("Job submission failed")
        
        #self.addJobs([job])
        jobToInsert = job.toDict()
        
        # check hashcode existence in job before hitting the database
        
        if self.logger.isEnabledFor(logging.DEBUG):
            start_time = timeit.default_timer()
        
        
        
        #call to update_one to check if the hashcode is in the collection, and add it if it is not
        #if a match is found, the matched document is overwritten and likely gets a new objectid
        result = self.dispy_work_collection.update_one( { "hashCode": jobToInsert["hashCode"] }, { '$set': jobToInsert }, upsert=True )
         
         
        if self.logger.isEnabledFor(logging.DEBUG):
            elapsed = timeit.default_timer() - start_time
            self.logger.debug("addJob completed in time: %f ms" % (elapsed * 1000) )
         
        #TODO: handle conditions in raw result
         
        #result is a UpdateResult
        # match: {'n': 1, 'nModified': 0, 'ok': 1.0, 'updatedExisting': True} 
        # no match: {'n': 1, 'nModified': 0, 'ok': 1.0, 'updatedExisting': False, 'upserted': ObjectId('6493782981cade83cac63132')}

        #if self.logger.isEnabledFor(logging.DEBUG):
        #    self.logger.debug("addJob result: %s" % pformat(result.raw_result) )
                
        #if(result.acknowledged == False):
        #    self.logger.error("Job submission failed")
        
    def addJobs(self, jobs):
        
        # TODO benchmark logging at debug level
        
        # TODO: instance check - each job should be a subtype of ClusterJobResult
        
        # how to enforce uniqueness
        # a double-insert of the same new object should only add one document to the database
        # this function and addJob may be called a lot if a job completion begets other new jobs
        # without necessarily knowing job internals

        # hashcode as match condition - need to standardize

        #db.col.update( {match_condition}, {new_job}, {upsert:true} )
        

        #result = self.dispy_work_collection.insert_many(insert_jobs)
            
        #TODO: use bulk_write
            
        for jobToInsert in jobs:
            self.addJob(job)
            #result = self.dispy_work_collection.update( { "hashCode": jobToInsert["hashCode"] }, jobToInsert, {"upsert:true"} )
                
            #if(result.acknowledged == False):
            #    self.logger.error("Job submission failed")
            #else:
            #    self.logger.warn("addJobs invoked with zero jobs")
        
    def getJobs(self, condition={}, count=10000):
                                
        # need to atomically:
        # query a collection with the provided condition
        # serialize the returned documents into adhic cluster jobs
        ## note object id
        # delete documents from collection by id

        newJobs = []
        
        #TODO: plug this in elsewhere
        if(self.closed == True):
            self.logger.warn("getJobs was called, but the database client was shut down. Returning.")
            return newJobs
        
        # can't call this function in rapid succession and have duplicate work released to nodes
        with self.jobRetrievalLock:
            
            self.logger.info("Retrieving new jobs...")
            
            if self.logger.isEnabledFor(logging.DEBUG):
                start_time = timeit.default_timer()
            
            newJobIds = []
            
            #TODO sort?
            
            # get the next batch of jobs as documents
            for newJobDoc in self.dispy_work_collection.find(condition).limit(count):
             
                if self.logger.isEnabledFor(logging.DEBUG):
                    self.logger.debug("Document returned: %s" % pformat(newJobDoc))
                
                #serialize job doc to the adhoc cluster job
                #append to list of returned jobs
                
                newJobs.append( self.jobSerializer( newJobDoc ) )
                
                ####################
                #TODO: since inserting with upsert in addJobs may overwrite or otherwise change the doc's _id, consider discarding by hashCode instead
                ####################
                
                self.logger.info("New job for ObjectId: %s" % newJobDoc['_id'] )
                
                #append object id to our todiscard list
                newJobIds.append( newJobDoc['_id'] )

            self.logger.info("Retrieved %d new jobs" % len(newJobs) )

            if( len(newJobs) > 0 ):

                #delete jobids for the work we're committing to
                deletion_result = self.dispy_work_collection.delete_many(
                    {
                        "_id": { "$in": newJobIds }
                    }
                )
        
                #testing with client w=0, where these writes are not acknowledged
                #if deletion_result.deleted_count != len(newJobs):
                #    self.logger.error("Error deleting ids: %d" % deletion_result.deleted_count)
                    
                #self.logger.info("Deleted %d ids" % deletion_result.deleted_count)
            else:
                self.logger.info("getJobs did not find new jobs from database")
        
        self.logger.info("Retrieved new jobs: %d" % len(newJobs))
        
        if self.logger.isEnabledFor(logging.DEBUG):
            elapsed = timeit.default_timer() - start_time
            self.logger.debug("getJobs job retrieval completed in time: %f ms" % (elapsed * 1000) )
        
        return newJobs
        
    def trimJobs(self, condition):
        #TODO: defer conditional to caller? would have to know mongodb conditionals
        self.dispy_work_collection.delete_many(condition)
        
    def getJobCount(self):
        return self.dispy_work_collection.count_documents({})
        
    def clearJobs(self):
        self.dispy_work_collection.delete_many({})
        
    def dropCollection(self):
        self.dispy_work_collection.drop()
        
    def close(self):
        self.closed = True
        self.runningIntake = False
        
        self.logger.info("MongoDBJobManager closing")
        
        if(self.client is not None):
            self.client.close()
