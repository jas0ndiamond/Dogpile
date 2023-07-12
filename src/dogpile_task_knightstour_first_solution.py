import sys
import os
import logging
import time
import dispy
import signal
import timeit

from threading import Thread

#################################
#knights tour that stops at the first solution it finds
#board expansion is random so re-runs may generate different 
#results if multiple knights tours possible for a given position and board

#################################

#src/app => src
#TODO: move to constructor?
srcDir = os.path.dirname( os.path.realpath(__file__) )

from Config import Config
from ClusterFactory import ClusterFactory
from DogPileTask import DogPileTask
from ChessBoard import ChessBoard
from KnightsTour import KnightsTour

from MongoDBJobManager import MongoDBJobManager

logFile = "run.log"

#init logging outside of constructor so constructed objects can access
logging.basicConfig(filename=logFile, format='%(asctime)s [%(levelname)s] -- [%(name)s]-[%(funcName)s]: %(message)s')
logger = logging.getLogger(__name__)

logger.setLevel( logging.DEBUG )


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

#TODO: large and small amounts. has to be tuned to job expansion rate. would be nice if this tuned itself
NEW_JOB_RETRIEVAL_AMOUNT_LARGE = 2 * 1000
NEW_JOB_RETRIEVAL_AMOUNT_SMALL = 50

NEW_WORK_POLL_SLEEP = 20 #seconds

def jobStatusCallback(job):
    
    if(job == None):
        logger.warn("Received a null job: %s" % job)
        return
        
    #quit if we're done or stopped. nodes will keep sending results
    global ktTaskDone
    if(ktTaskDone == True):
        logger.info("job status callback function invoked, but task was done. Returning...")
        return
        
        
    # TODO: check if we're quitting or otherwise done
        
    if job.status == dispy.DispyJob.Finished:
    
        if(job.result == None):
            logger.warning("Received a null job result for job id: %s" % job.id)
            return
        
        #trace
        #if(logger.isEnabledFor(logging.DEBUG)):
        #    logger.debug('job finished for %s: %s' % (job.id, job.result))

        #at this point we've received a result board, assume coherent
        
        #are we done?
        #yes => signal that we are done, cancel remaining or in-progress work
        #no => add new cluster job for it
        
        #########
        #TODO remove array slice below
        #########
        
        if logger.isEnabledFor(logging.DEBUG):
            start_time = timeit.default_timer()
        
        #job.result is an array of ChessBoard
        for expandedBoard in job.result:
            
            if(expandedBoard == None):
                logger.error("Received a null board expansion result for job id: %s" % job.id)
            
            elif(isTourComplete(expandedBoard)):
                
                # is this the solution state?
                
                logger.info("*********Tour complete:\n%s\n" % expandedBoard.dump())
                #write final result
                #cancel in-progress work
                
                #global ktTaskDone
                ktTaskDone = True
                
                global ktFirstSolutionBoard
                ktFirstSolutionBoard = expandedBoard.dump()
                
            else:                
                
                #submit job for new board
                #don't have to worry about rexpanding a board we've already seen. KnightsTour.expandBoard checks for previous turns
                #knightsTourTask.submitClusterJob( KnightsTour( expandedBoard.getBoardStateStr(), expandedBoard.getXDim(), expandedBoard.getYDim(), expandedBoard.getTurnCount() ) )
                knightsTourTask.submitNewJob( expandedBoard )
                
                #trace
                #if(logger.isEnabledFor(logging.DEBUG)):
                #    logger.debug("Expanding board:\n%s\n" % expandedBoard.dump())
                
                #adds to record of known boards as side effect to avoid copying board state more than once
    #                if(checkIfNewBoard(expandedBoard)):
    #                    
    #                    if(logger.isEnabledFor(logging.DEBUG)):
    #                        logger.debug("Found new board:\n%s\n" % expandedBoard.dump())
    #                    
    #                    #submit new job
    #                    knightsTourTask.submitClusterJob( KnightsTour( expandedBoard.getBoardStateStr(), expandedBoard.getXDim(), expandedBoard.getYDim(), expandedBoard.getTurnCount() ) )
    #                else:
    #                    #ignore - we don't need to expand the board again
    #                    if(logger.isEnabledFor(logging.DEBUG)):
    #                        logger.debug("Skipping expansion of old board:\n%s\n" % expandedBoard.dump())

        
        #trace
        #if logger.isEnabledFor(logging.DEBUG):
        #    elapsed = timeit.default_timer() - start_time
        #    logger.debug("Submission of expanded boards completed in time: %f ms" % (elapsed * 1000) )
        
        #TODO: signal callback work is finished - different from counting 

    elif job.status == dispy.DispyJob.Terminated or job.status == dispy.DispyJob.Cancelled or job.status == dispy.DispyJob.Abandoned:
        logger.error("job failed for %s failed: %s" % (job.id, job.exception))

        #TODO: signal callback work is finished

        #TODO: remove id from result mapping?
    else:  # don't need explicit handling of other status messages
        logger.warn("Unexpected job status: %d" % job.status )
        #if we're not logging warnings above
        pass

#implementation where a result can generate new cluster jobs, and cancel in-progress work if we're done
def clusterStatusCallback(status, node, job):

    #"self" is problematic in this function
    #this is effectively a static method, and we can't access the return value
    #originally implementation worked with the retry queue as global, and the writeResult function also as global

    #if(logger.isEnabledFor(logging.DEBUG)):
    #    logger.debug("=============cluster_status_cb===========")

    global ktTaskDone

    if(ktTaskDone):
        logger.debug("cluster status callback function invoked, but task was done. Returning...")
        return

    if status == dispy.DispyNode.Initialized:
        logger.debug ("node %s with %s CPUs available" % (node.ip_addr, node.avail_cpus))
    elif status == dispy.DispyNode.Closed:
        logger.debug ("node %s closing" % node.ip_addr)
    elif status == dispy.DispyJob.Created or status == dispy.DispyJob.Running or status == dispy.DispyJob.Finished:
        #inherited from job statuses. normal operation. ignore
        pass
    else: 
        logger.warn("Unexpected node status: %d" % status )
        
#TODO: move utility methods somewhere reusable?
def isTourComplete(board):
    return board.getBoardStateStr().find(ChessBoard.BLANK) == -1
    
def signal_handler(sig, frame):
    print('SIGINT Caught, shutting down. Nodes may take a while to dump their pending work.')
    logger.info("SIGINT Caught, shutting down. Nodes may take a while to dump their pending work.")
    
    global ktTaskDone
    ktTaskDone = True
    
    knightsTourTask.stop()
    
    time.sleep(10)
    
    logger.info("Exiting")
    sys.exit(1)
    
def checkIfNewBoard(board):
    retval = False
    
    boardState = board.getBoardStateStr()
    if( (boardState in knownBoards) is False):
        retval = True
        knownBoards[boardState] = 1
        
        if(logger.isEnabledFor(logging.DEBUG)):
            logger.debug("Known boards count: %d" % len(knownBoards)) 
    
    return retval
    
        
##############################

class KnightsTourTask(DogPileTask):
    
    # TODO: accept a chessboard as a start state
    def __init__(self, confFile, startx=4, starty=4):
        super().__init__(confFile, enableRetryQueue=False)
        
        self.startx = startx
        self.starty = starty
      
        self.newWorkPollingThread = None
        self.pollingForNewWork = True
        
        self.clusterJobDB = None
      
    #overrides superclass method
    def initializeCluster(self):
        logger.info("Initializing cluster for knights tour")
        
        clusterFactory = super().getClusterFactory()
        
        #supply the dispy lambda and the status callback. 
        #seeing issues trying to combine this statement with the clusterFactory retrieval abo
        #super().clusterStatusCallback is generic enough for most cases, but subclasses may want otherwise 
        
        super()._setCluster( clusterFactory.buildCluster( KnightsTour.expandBoard, clusterStatusCallback, jobStatusCallback ) )
        
        #TODO: reallocate nodes in case we previously had a sloppy shutdown?
        
        self.clusterJobDB = MongoDBJobManager(DB_HOST, DB_PORT, DB_USER, DB_PASS, DB_NAME, DB_JOB_COLLECTION, ChessBoard.serializeFromDict)
        
        #constant for this value
        self.clusterJobDB.setIntakeSubmissionThreshold(22 * 1000)
        
        self.newWorkPollingThread = Thread(target=self._pollForNewWork)
        
    def start(self):
        #TODO: check initializeCluster was called
        
        #does not block
        logger.info("Starting DogPile task")
        
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
        
        startBoard = ChessBoard(ChessBoard.STANDARD_BOARD_DIM, ChessBoard.STANDARD_BOARD_DIM)
        startBoard.setSpace(self.startx, self.starty, ChessBoard.KNIGHT)
        
        logger.debug("Starting board:\n%s" % startBoard.dump())
        
        self.submitNewJob( startBoard )
        #self.submitClusterJob( KnightsTour( startBoard.getBoardStateStr(), startBoard.getXDim(), startBoard.getYDim() ) )       
        
        #start job polling thread 
        self.pollingForNewWork = True
        self.newWorkPollingThread.start()

    #states are stored in the database, and DogpileTasks have states loaded into them, and are submitted to the cluster for workload execution
    def submitNewJob(self, newState):
        
        # TODO type check ChessBoard object
        logger.info("Adding new job:\n%s\n" % newState.dump()

        self.clusterJobDB.addToIntake(newState)
        
    def stop(self):
        
        logger.info("Stopping dogpile knightstour task")

        super().stop()
        
        self.pollingForNewWork = False
        
        if(self.clusterJobDB != None):
            #TODO: recovery based on what's left in the database and the application state?
            self.clusterJobDB.close()
            
    #function to monitor cluster-submitted jobs and retrieve new work
    #if we run out of work, this loop must be stopped or paused externally
    def _pollForNewWork(self):
        
        #TODO: move bulk to DogpileTask, though subclass will still need to be able to specify a retrieval condition
        
        ########
        # there are several queues to manage to prevent memory exhaustion on the dispy server node:
        # dispy's worker_Q - a queue of incoming responses from nodes
        ## grows when work results cannot be processed faster than their arrival rate
        # dispy's pending jobs queue - a queue of jobs submitted to the dispy cluster waiting on node availibility
        ## grows when many jobs are submitted to the cluster faster than nodes can accept and complete them
        # dogpile's JobManager intake queue - a queue of pending jobs destined for insertion in external storage
        ## grows when insertions into external storage are outpaced by insertions of new jobs
        
        # the goal is to manage release of jobs to cluster nodes to keep queue sizes under control, and allow
        # the external job storage to grow as large as necessary
        
        # these queues are not pre-allocated, so when they re-size themselves, pileups can occur
        
        # database insertions with constraints on uniqueness typically increase in duration for larger datasets
        
        # most of the time, we should avoid letting nodes sit idle. exceptions for extremely fast-growing problems
        
        # dispy will always have a worker_Q
        # a job manager will always have an intake queue
        ########
        
        
        # pending cluster work is in the remote db
        # each work job generates a bunch of callback invocations that get enqueued into dispy's worker_Q
        # have to manage size of worker_Q or else memory will get exhausted
        
        while(self.pollingForNewWork == True):
            
            #while(self.pollingPaused == True):
            #    time.sleep(30)
            
            #TODO: also consider pending job count super().getPendingJobCount()
            
            workerQSize = super().getDispyWorkerQSize()
            intakeQueueSize = self.clusterJobDB.getIntakeQueueSize()
            
            if(workerQSize < NEW_JOB_RETRIEVAL_LOWER_THRESHOLD and intakeQueueSize < NEW_JOB_RETRIEVAL_LOWER_THRESHOLD):
                
                # if the worker_Q has shrunk, release more work
                
                self.logger.info("Pending job count beneath threshold. Retrieving large work amount.")
                #TODO: threshold vals loaded from config
                
                #opportunity here to change getJobs condition intelligently
                #highest/lowest turncount
                #knight position closest to middle of board
                #etc
                
                #retrieve a lot of jobs
                newSubmissions = 0
                for newBoard in self.clusterJobDB.getJobs( {}, NEW_JOB_RETRIEVAL_AMOUNT_LARGE ):
                    #newState is a ChessBoard, because the JobManager is configured with a serializer function 
                    super().submitClusterJob( KnightsTour( newBoard.getBoardStateStr(), newBoard.getXDim(), newBoard.getYDim(), newBoard.getTurnCount() ) )
                    newSubmissions += 1
                
                #if self.logger.isEnabledFor(logging.DEBUG):
                #    self.logger.debug("Submitting new jobs for compute: %d" % newSubmissions)
                    
            elif ( workerQSize > NEW_JOB_RETRIEVAL_UPPER_THRESHOLD or intakeQueueSize > NEW_JOB_RETRIEVAL_UPPER_THRESHOLD):
                self.logger.info("Pending job count above upper threshold.")
            else:
                
                self.logger.info("Pending job count within thresholds. Retrieving small work amount.")
                
                #TODO: reconsider retrieving new work at this point. seeing worker_q size hover around NEW_JOB_RETRIEVAL_UPPER_THRESHOLD
                
                # retrieve a few jobs
                newSubmissions = 0
                for newBoard in self.clusterJobDB.getJobs( {}, NEW_JOB_RETRIEVAL_AMOUNT_SMALL ):
                    #newState is a ChessBoard, because the JobManager is configured with a serializer function 
                    super().submitClusterJob( KnightsTour( newBoard.getBoardStateStr(), newBoard.getXDim(), newBoard.getYDim(), newBoard.getTurnCount() ) )
                    newSubmissions += 1
            
                #if self.logger.isEnabledFor(logging.DEBUG):
                #    self.logger.debug("Submitting new jobs for compute: %d" % newSubmissions)
            
            
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
            while( i < NEW_WORK_POLL_SLEEP and self.pollingForNewWork == True and (super().getDispyWorkerQSize() > NEW_JOB_RETRIEVAL_LOWER_THRESHOLD or self.clusterJobDB.getIntakeQueueSize() > NEW_JOB_RETRIEVAL_LOWER_THRESHOLD )):
                time.sleep(1)
                i += 1
                
            
         
    def isFinished(self):
        global ktFirstSolutionBoard
        return ktFirstSolutionBoard != None

################
def main(args):

    if(len(args) < 1):
        print("Usage: dogpile_task_knightstour_first_solution.py conf_file")
        exit(1);
    
    signal.signal(signal.SIGINT, signal_handler)
    
    #global so the dispy callback can reference
    
    global knownBoards
    knownBoards = {}
    
    global ktTaskDone    
    ktTaskDone = False
    
    global ktFirstSolutionBoard
    ktFirstSolutionBoard = None
    
    global knightsTourTask 
    knightsTourTask = KnightsTourTask(args[1])
    
    knightsTourTask.initializeCluster()
        
    #blocks    
    knightsTourTask.start()
    
    #wait for cluster operation to complete
    knightsTourTask.waitForWorkloadCompletion()
    
    logger.info("Work completed. Shutting down.")
    
    #shutdown
    knightsTourTask.stop()
    
    #save results
    logger.info("KT First Solution discovered: %s" % ktFirstSolutionBoard)

    logger.info("Exiting")

###############################
if __name__ == "__main__":
    main(sys.argv)
