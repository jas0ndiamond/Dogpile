import logging
import queue
import time
import timeit

from threading import Thread, Lock

from pprint import pprint, pformat
from pymongo import MongoClient, UpdateOne
from bson import ObjectId

from JobManager import JobManager

logFile = "run.log"

MAX_INTAKE_SIZE = 2 * 1000 * 1000
HASHCODE_FIELD = "hashCode"

# On-disk management of job state data
#TODO: refactor naming of functions like 'addJob' to 'addState' and similar
class MongoDBJobManager(JobManager):
    
    def __init__(self, db_host, db_port, db_user, db_pass, db_name, collection_name, job_serializer):
        
        logging.basicConfig(filename=logFile, format='%(asctime)s [%(levelname)s] -- [%(name)s]-[%(funcName)s]: %(message)s')
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel( logging.DEBUG )
        
        #TODO: look into having the job insertion and job retrieval set their own write concerns. 
        # possible we care less about job insertion results for the sake of performance.
        # possible we care more about job deletion because we don't want to doubly execute work
        self.client = MongoClient(host=db_host, port=db_port, username=db_user, password=db_pass, authSource='admin', authMechanism='SCRAM-SHA-1', w=0)
        #, w=0
        
        #TODO default value for collection_name. cannot be empty string
        
        #expect db_user to be bound to db_name
        #require at least one named collection for the database
        self.database = self.client[db_name]
        self.dispy_work_collection = self.database[collection_name]
        
        # clear the collection in case a sloppy shutdown did not
        self.dispy_work_collection.drop()
        
        # create our job field index. without this, inserts with uniqueness contraints are too slow
        self.dispy_work_collection.drop_index( HASHCODE_FIELD )
        self.dispy_work_collection.create_index( HASHCODE_FIELD )
        
        self.logger.info("Building MongoDBJobManager for collection %s at %s:%d" % (collection_name, db_host, db_port) )
        
        #TODO checks on this. should be a function
        self.jobSerializer = job_serializer
        
        self.jobRetrievalLock = Lock()
        
        self.intake = queue.Queue(maxsize=MAX_INTAKE_SIZE)
        
        self.closed = False
        
        self.runningIntake = True
        
        self.intakeThread = Thread(target=self.processIntake)
                
        # threshold of intake queue size, above which entries are written to the database
        self.intakeSubmissionThreshold = 0
        
        # number of jobs to write to the database at a time
        self.intakeBlockSize = 20 * 1000
        
        self.intakeProcessingSleep = 1
        
        self.intakeThread.start()
        
    def setIntakeSubmissionThreshold(self, threshold):
        if(threshold >= 0):
            self.intakeSubmissionThreshold = threshold
        else:
            self.logger.warning("Rejecting invalid intake submission threshold")
        
    def setIntakeBlockSize(self, size):
        if(size > 0):
            self.intakeBlockSize = size
        else:
            self.logger.warning("Rejecting invalid intake block size")
            
    def setIntakeProcessingSleep(self, sleepLen):
        if( sleepLen > 0 ):
            self.intakeProcessingSleep = sleepLen
        else:
            self.logger.warning("Rejecting invalid intake processing sleep")
        
    def getIntakeQueueSize(self):
        return self.intake.qsize()
        
    def addToIntake(self, job):
        
        # possible since this is a queue we don't need to lock
        
        self.intake.put(job)
        
        
    #process intake queue in background, so each external job add doesn't incur a db write and block the dispy job callback
    def processIntake(self):
        
        #need to ensure if queue is filled up faster than it can be cleared, then this loop may never exit
        
        #currently experimenting with async writes => not super helpful
        
        jobSubmissionBlock = []
        
        previousIntakeQueueSize = 0
        
        #also need to allow first jobs to be processed or they might never make it to 
        #the database, and won't ever be available for retrieval
        #if( intakeQueueSize > minClearSize or intakeQueueSize < 100):
        while(self.runningIntake):         
            jobSubmissionBlock.clear()
                
            queueSize = self.getIntakeQueueSize()
                
            #if we don't have a lower bound, nodes will starve as submitted jobs never make it to the database for retrieval
            #TODO: manage this parameter
            if(queueSize == previousIntakeQueueSize or queueSize > self.intakeSubmissionThreshold):
                
                if( self.logger.isEnabledFor(logging.DEBUG)):
                    self.logger.debug("Writing Intake Queue contents to database")

                
                # likely the intake queue resizing is what causes random stalls
                
                # this loop must terminate reasonably- if we're inundated with jobs it's possible they just get infinitely added 
                # to jobSubmissionBlock but never written to the database
                i = 0
                while( i < self.intakeBlockSize and self.intake.empty() == False):
                    jobSubmissionBlock.append(self.intake.get())
                    i += 1
                
                # only if we have jobs to submit, and we haven't seen a shutdown signal while compiling the job submission block
                if(jobSubmissionBlock and self.runningIntake):
                    
                    if( self.logger.isEnabledFor(logging.DEBUG)):
                        self.logger.debug("Writing %d new jobs to database from intake queue" % (len(jobSubmissionBlock) ) )
                    
                    self.addJobs(jobSubmissionBlock)
                    
                # no sleep. db write is probably enough of a time gap for the queue to replenish
            else:
                if( self.logger.isEnabledFor(logging.DEBUG)):
                    self.logger.debug("Intake Queue size under submission threshold")
                    
                previousIntakeQueueSize = queueSize
                    
                # sleep so we're not spinning if there's a lull in intake work
                time.sleep(self.intakeProcessingSleep)
        
        self.logger.info("Intake Queue Thread exiting")
         
    # add a single job to the job database. enforces uniqueness with job hashcodes. 
    def addJob(self, job):
        
        #TODO: instance check - each job should be a subtype of ClusterJobResult
        #TODO: check hashcode field existence in job before hitting the database       
        
        #ClusterJobResult require toDict method?
        
        #if(result.acknowledged == False):
        #    self.logger.error("Job submission failed")
        
        if( self.logger.isEnabledFor(logging.DEBUG)):
            start_time = timeit.default_timer()
        
        jobToInsert = job.toDict()
        
        #TODO: check hashcode field existence in job before hitting the database
                
        # call to update_one to check if the hashcode is in the collection, and add it if it is not
        # if a match is found, the matched document is overwritten and likely gets a new objectid
        # not the best...would be better if the overwrite was not executed for duplicates
        result = self.dispy_work_collection.update_one( { HASHCODE_FIELD: jobToInsert[HASHCODE_FIELD] }, { '$set': jobToInsert }, upsert=True )
         
         
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
        
    # add a list of jobs with unique hashcodes to the database. 
    def addJobs(self, jobs):
        
        
        # how to enforce uniqueness
        # a double-insert of the same new object should only add one document to the database
        # this function and addJob may be called a lot if a job completion begets other new jobs
        # without necessarily knowing job internals

        # hashcode as match condition - need to standardize

        #db.col.update( {match_condition}, {new_job}, {upsert:true} )
        
        if self.logger.isEnabledFor(logging.DEBUG):
            start_time = timeit.default_timer()

        # single job submission for reference:
        # result = self.dispy_work_collection.update_one( "hashCode": jobToInsert["hashCode"] }, { '$set': jobToInsert }, upsert=True ) )

        requests = []
        
        for job in jobs:
            
            #TODO: instance check - each job should be a subtype of ClusterJobResult
            #TODO: check hashcode field existence in job before hitting the database
            
            # get the document from the job
            jobToInsert = job.toDict()
            
            #TODO: see if possible with UpdateMany
            #TODO: upsert possible in call to bulk_write rather than in each call to UpdateOne?
            
            requests.append( UpdateOne( { HASHCODE_FIELD: jobToInsert[HASHCODE_FIELD] }, { '$set': jobToInsert }, upsert=True ) )

        self.dispy_work_collection.bulk_write(requests)
        
        if self.logger.isEnabledFor(logging.DEBUG):
            elapsed = timeit.default_timer() - start_time
            self.logger.debug("addJobs of %d completed in time: %f ms" % ( len(jobs), (elapsed * 1000) ) )
        
    
    def getJobs(self, condition={}, count=10000):
                         
        # TODO: rename to retrieveJobs
                                
        # need to atomically:
        # query a collection with the provided condition
        # serialize the returned documents into adhic cluster jobs
        ## note object id
        # delete documents from collection by id

        newJobs = []
        
        #TODO: plug this in in other places. the db connection can close randomly from external actions.
        if(self.closed == True):
            self.logger.warn("getJobs was called, but the database client was shut down. Returning.")
            return newJobs
        
        # can't call this function in rapid succession and have duplicate work released to nodes
        with self.jobRetrievalLock:
            
            self.logger.info("Retrieving new jobs...")
            
            if self.logger.isEnabledFor(logging.DEBUG):
                start_time = timeit.default_timer()
            
            newJobHashCodes = []
            
            #TODO sort?
            
            # get the next batch of jobs as documents
            for newJobDoc in self.dispy_work_collection.find(condition).limit(count):
             
                #trace
                #if self.logger.isEnabledFor(logging.DEBUG):
                #    self.logger.debug("Document returned: %s" % pformat(newJobDoc))
                
                #serialize job doc to the adhoc cluster job
                #append to list of returned jobs
                
                newJobs.append( self.jobSerializer( newJobDoc ) )
                
                ####################
                # since inserting with upsert in addJobs may overwrite or otherwise change the doc's _id, discard by hashCode instead
                ####################
                
                self.logger.info("New job for hashCodes: %s" % newJobDoc[HASHCODE_FIELD] )
                
                #append object id to our todiscard list
                #could use hashcode but that's most suited to comparing board states
                newJobHashCodes.append( newJobDoc[HASHCODE_FIELD] )

            self.logger.info("Retrieved %d new jobs from database" % len(newJobs) )

            # if we have new jobs to delete from the db after retrieval
            if( newJobs ):

                #delete jobids for the work we're committing to
                deletion_result = self.dispy_work_collection.delete_many(
                    {
                        HASHCODE_FIELD: { "$in": newJobHashCodes }
                    }
                )
        
                ##################
                # testing results of the deletion, requires a write_concern != 0. our client sets write_concern to 0 
                
                #if deletion_result.deleted_count != len(newJobs):
                #    self.logger.error("Error deleting ids: %d" % deletion_result.deleted_count)
                    
                #self.logger.info("Deleted %d jobs as part of retrieval" % deletion_result.deleted_count)
                
                ##################
            else:
                self.logger.info("getJobs did not find new jobs from database")
        
        if self.logger.isEnabledFor(logging.DEBUG):
            elapsed = timeit.default_timer() - start_time
            self.logger.debug("getJobs job retrieval completed in time: %f ms" % (elapsed * 1000) )
        
        return newJobs
        
    def trimJobs(self, condition):
        # trim the jobs db with a context-aware condition
        #TODO: defer conditional to caller? would have to know mongodb conditionals
        self.dispy_work_collection.delete_many(condition)
        
    def getJobCount(self):
        #this can take a long time for collection sizes in the millions
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
