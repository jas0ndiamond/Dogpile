import logging
import time
import dispy

from threading import Thread

from Config import Config
from ClusterFactory import ClusterFactory
from DogPileTask import DogPileTask
from ChessBoard import ChessBoard
from KnightsTour import KnightsTour
from MongoDBJobManager import MongoDBJobManager

#TODO: add all of these directives to Dogpile, as they apply for any cluster application

#TODO: add to config file
#connect db
DB_NAME = "dogpile_work"
DB_USER = "dogpile"
DB_PASS = "dogpile_pass"
DB_HOST = "mercury"
DB_PORT = 27017

# TODO: toggle test string
#DB_JOB_COLLECTION = "KT-BREADTH-FIRST_" + str(int(time.time()))
DB_JOB_COLLECTION = "KT-BREADTH-FIRST_TEST"

# worker_Q size threshold where we request a lot of work if worker_Q size is lower
NEW_JOB_RETRIEVAL_LOWER_THRESHOLD = 10 * 1000

# limit of worker_Q size. TODO: rename. beyond this value no new work will be retrieved
NEW_JOB_RETRIEVAL_UPPER_THRESHOLD = 500 * 1000

#large and small amounts. has to be tuned to the expected job expansion rate. 
#TODO: would be nice if this tuned itself
NEW_JOB_RETRIEVAL_AMOUNT_LARGE = 2 * 1000
NEW_JOB_RETRIEVAL_AMOUNT_SMALL = 50

INTAKE_SUBMISSION_THRESHOLD = 22 * 1000

NEW_WORK_POLL_SLEEP = 20 #seconds

#how many boards to submit per job
JOB_SUBMISSION_BLOCK_SIZE = 1

# Essentially the context-aware workload management behind a Dogpile task
# Abstract the dispy and cluster operation
# Invoker controls how it works given their cluster layout
class KnightsTourTask(DogPileTask):
    
    def __init__(self, confFile, startBoard):
        super().__init__(confFile, enableRetryQueue=False)
        
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel( logging.DEBUG )
        
        #TODO check instanceof Chessboard
        self.startBoard = startBoard
               
        self.solutionBoard = None
       
        self.newWorkPollingThread = None

        self.pollingForNewWork = True
        self.submittingJobs = False
        
        self.clusterJobDB = None
      
    #overrides superclass method
    def initializeCluster(self, clusterJobStatusCallback):
        self.logger.info("Initializing cluster for knights tour")
        
        clusterFactory = super().getClusterFactory()
        
        #supply the dispy lambda and the status callback. 
        #seeing issues trying to combine this statement with the clusterFactory retrieval abo
        #super().clusterStatusCallback is generic enough for most cases, but subclasses may want otherwise 
        
        super()._setCluster( clusterFactory.buildCluster( KnightsTour.expandBoards, job_status_callback=clusterJobStatusCallback ) )
        
        #TODO: reallocate nodes in case we previously had a sloppy shutdown?
        
        # TODO try/catch around this since the db host may not be up
        self.clusterJobDB = MongoDBJobManager(DB_HOST, DB_PORT, DB_USER, DB_PASS, DB_NAME, DB_JOB_COLLECTION, ChessBoard.serializeFromDict)
        
        self.clusterJobDB.setIntakeSubmissionThreshold(INTAKE_SUBMISSION_THRESHOLD)
        
        self.newWorkPollingThread = Thread(target=self._pollForNewWork)
        
    def start(self):
        #TODO: check initializeCluster was called
        
        #does not block
        self.logger.info("Starting DogPile task")
        
        #do we have a valid cluster?
        if(super().getClusterFactory() == None):
            raise Exception("Cluster was not built successfully. Bailing")
            
        #TODO: read from config
        self.enableHttpServer = True
        
        if(self.enableHttpServer):
            super().startDispyHttpServer()
            
        ################################################
        #create a chessboard start state, and send it to the cluster for expansion
        #expand the results
        
        self.logger.debug("Starting board:\n%s" % self.startBoard.dump())
        
        self.submitNewJob( self.startBoard )
        
        #start job polling thread 
        self.pollingForNewWork = True
        self.newWorkPollingThread.start()

    #states are stored in the database, and DogpileTasks have states loaded into them, and are submitted to the cluster for workload execution
    def submitNewJob(self, newState):
        
        # TODO type check ChessBoard object. or defer this to db manager
        self.logger.info("Adding new job:\n%s\n" % newState.dump() )

        self.clusterJobDB.addToIntake(newState)
        
# TODO: implement
#    def submitNewJobs(self, newStates):
#        
#        
#        self.logger.info("Adding new jobs: %d\n" % len(newStates) )
#
#        self.clusterJobDB.addToIntake(newStates)
        
    def stop(self):
        
        self.logger.info("Stopping dogpile knightstour task")

        super().stop()
        
        self.pollingForNewWork = False
        
        if(self.clusterJobDB != None):
            # recovery likely not possible for KT without persisting the whole compute state and history to db
            # couldn't determine a starting state for the next execution
            self.clusterJobDB.close()
            
    #TODO: implement in case we want a result other than a completed tour
    def setTargetBoard(self, board):
        pass
        
    def setStartBoard(self, board):
        #TODO check instanceof Chessboard
        self.startBoard = board
        
    def setSolutionBoard(self, board):
        self.solutionBoard = board
        
    def getSolutionBoard(self):
        return self.solutionBoard
        
    # given a pile of boards/work units, submit new cluster tasks for blocks of these work units
    # submitting blocks/groups of shorter-running compute jobs cuts down on network overhead
    def submitWork(self, work_units):

        if( len(work_units) == 0):
            self.logger.warn("submitWork called with no work units, bailing")
            return

        self.submittingJobs = True

        newWorkloadSubmissions = 0
        toSubmit = []
        toSubmitCount = 0
        
        # for each work unit in the incoming work unit array, fill another array to capacity and submit compute job to cluster
        for work_unit in work_units:
            toSubmit.append(work_unit)
            toSubmitCount += 1 
            
            #we've filled a block with jobs, submit to cluster
            if(toSubmitCount >= JOB_SUBMISSION_BLOCK_SIZE):
                #previously
                #super().submitClusterJob( KnightsTour( newBoard.getBoardStateStr(), newBoard.getXDim(), newBoard.getYDim(), newBoard.getTurnCount() ) )
                
                #submit kt cluster compute job with an array of inputs to expand
                super().submitClusterJob( KnightsTour( toSubmit ) )
                toSubmit.clear()
                            
                newWorkloadSubmissions += toSubmitCount
                toSubmitCount = 0
        
        # if the submission blocks dont evenly divide the new board count, or the number of work_units is less than JOB_SUBMISSION_BLOCK_SIZE
        if( toSubmitCount > 0):
            
            #submit kt cluster compute job with an array of inputs to expand
            super().submitClusterJob( KnightsTour( toSubmit ) )
            toSubmit.clear()
                            
            newWorkloadSubmissions += toSubmitCount
            toSubmitCount = 0

        self.submittingJobs = False

        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug("Submitting jobs for new work units: %d" % newWorkloadSubmissions)
        
            
    #function to monitor cluster-submitted jobs and retrieve new work
    #if we run out of work, this loop must be stopped or paused externally
    def _pollForNewWork(self):
        
        #TODO: move bulk to DogpileTask, though subclass will still need to be able to specify a retrieval condition
        #and other config around the process
        
        ########
        # there are several queues to manage to prevent local memory exhaustion for large workloads
        # dispy's worker_Q - a queue of incoming responses from nodes
        ## grows when work results cannot be processed faster than their arrival rate
        # dispy's pending jobs queue - a queue of jobs submitted to the dispy cluster waiting on node availibility
        ## grows when many jobs are submitted to the cluster faster than nodes can accept and complete them
        # dogpile's JobManager intake queue - a queue of pending jobs destined for insertion in external storage
        ## grows when insertions into external storage are outpaced by insertions of new jobs
        
        # the goal is to manage release of jobs to cluster nodes to keep queue sizes under control, and allow
        # the external job storage to grow as large as necessary
        
        # these queues are not pre-allocated, so when they re-size themselves, pileups can occur during long resizes
        
        # database insertions with constraints on uniqueness typically increase in duration for larger datasets
        
        # most of the time, we should avoid letting nodes sit idle. exceptions for extremely fast-growing problems
        
        # dispy will always have a worker_Q
        # a job manager will always have an intake queue
        ########
        
        # TODO: need to fully consider pending jobs
        
        # pending cluster work is in the remote db
        # each work job generates a bunch of callback invocations that get enqueued into dispy's worker_Q
        # have to manage size of worker_Q or else memory will get exhausted
        
        #TODO: make this configurable
        # breadth first
        retrieve_condition = {}
        
        # depth first
        # retrieve_condition = {}
                
        while(self.pollingForNewWork == True):
                        
            #TODO: also consider pending job count super().getPendingJobCount()
            
            #TODO: detect work exhaustion and log
            
            workerQSize = super().getDispyWorkerQSize()
            intakeQueueSize = self.clusterJobDB.getIntakeQueueSize()
            pendingJobCount = super().getPendingJobCount()
            
            
            # TODO: different thresholds for each
            # TODO: threshold vals loaded from config
            if(workerQSize < NEW_JOB_RETRIEVAL_LOWER_THRESHOLD and intakeQueueSize < NEW_JOB_RETRIEVAL_LOWER_THRESHOLD and pendingJobCount < NEW_JOB_RETRIEVAL_LOWER_THRESHOLD):
                
                # if active work is beneth thresholds, get more work to do
                
                self.logger.info("Active work beneath threshold. Retrieving large work amount.")
                
                #opportunity here to change getJobs condition intelligently
                #highest/lowest turncount
                #knight position closest to middle of board
                #etc
                
                #TODO: benchmark and log submissions
                
                #####################################
                
                #TODO strategy for submitting cluster jobs in blocks. would need mechanism to flush incomplete blocks
                #might be best to leave this the same and add a queuing mechanism to super.submitClusterJob
                
                #####################################
                
                if( self.clusterJobDB.hasJobs() ):
                    #retrieve a lot of jobs
                    #getJobs will return a lot of jobs, submit KT compute jobs with groups of jobs

                    self.submitWork( 
                        self.clusterJobDB.getJobs( retrieve_condition, NEW_JOB_RETRIEVAL_AMOUNT_LARGE ) 
                    )
                else:
                    if self.logger.isEnabledFor(logging.DEBUG):
                        self.logger.debug("Cluster DB had no jobs to retrieve")
                    #longer sleep since there's nothing to retrieve from the database
                    time.sleep(5)
                    
            elif ( workerQSize > NEW_JOB_RETRIEVAL_UPPER_THRESHOLD or intakeQueueSize > NEW_JOB_RETRIEVAL_UPPER_THRESHOLD or pendingJobCount > NEW_JOB_RETRIEVAL_UPPER_THRESHOLD):
                self.logger.info("Active work above upper threshold.")
                
                #sleeps later
            elif (self.clusterJobDB.hasJobs()):
                
                self.logger.info("Active work within thresholds. Retrieving small work amount.")
                
                #TODO: reconsider retrieving new work at this point. seeing worker_q size hover around NEW_JOB_RETRIEVAL_UPPER_THRESHOLD => possibly resolved with consideration of other queues
                
                #TODO: benchmark and log submissions
                
                # retrieve a small amount of work and submit to cluster
                self.submitWork( 
                    self.clusterJobDB.getJobs( retrieve_condition, NEW_JOB_RETRIEVAL_AMOUNT_SMALL ) 
                ) 
            else:
                # cluster does not have jobs and queues are beneath thresholds
                # possible we're exhausting workload, computation is nearing end
                if self.logger.isEnabledFor(logging.DEBUG):
                    self.logger.debug("Active work processing")
            
            ####################
            # workload stats
            
            workerQSize = super().getDispyWorkerQSize()
            intakeQueueSize = self.clusterJobDB.getIntakeQueueSize()
            pendingJobsCount = super().getPendingJobCount()
            
            if self.logger.isEnabledFor(logging.DEBUG):
                self.logger.debug("================ Intake queue size: %d, worker_Q size: %d, pending jobs count: %d" % (intakeQueueSize, workerQSize, pendingJobsCount) )
            
            ####################
            
            # sleep to let the retrieved work get processed. 
            # cut sleep short if we run out of work or are terminating the application
            # cut sleep short if we're running low on work
            # don't block external termination of this thread
            i = 0
            while( i < NEW_WORK_POLL_SLEEP and 
                self.pollingForNewWork == True and 
                (
                    super().getDispyWorkerQSize() > NEW_JOB_RETRIEVAL_LOWER_THRESHOLD or 
                    self.clusterJobDB.getIntakeQueueSize() > NEW_JOB_RETRIEVAL_LOWER_THRESHOLD or
                    super().getPendingJobCount() > NEW_JOB_RETRIEVAL_LOWER_THRESHOLD
                )
            ):
                time.sleep(1)
                i += 1
                
    #TODO: to superclass
    def isFinished(self):
        # either there's a solution or we've run out of work, or the solution space has been exhausted
        return self.solutionBoard != None or self.isWorkAvailable() == False
        
    #TODO: to superclass
    #TODO: it may be possible that all the queues and the database is empty, but callbacks are still processing
    def isWorkAvailable(self):
        # check the queue empty functions first
        # check the db job count last because it requires a query
        return (super().isDispyWorkerQEmpty() == False or 
            self.clusterJobDB.isIntakeQueueEmpty() == False or 
            super().getPendingJobCount() > 0 or 
            self.clusterJobDB.hasJobs() == True or
            self.submittingJobs == True )
