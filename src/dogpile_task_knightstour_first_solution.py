import sys
import os
import logging
import time
import dispy
import signal

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

logFile = "run.log"

#init logging outside of constructor so constructed objects can access
logging.basicConfig(filename=logFile, format='%(asctime)s [%(levelname)s] -- [%(name)s]-[%(funcName)s]: %(message)s')
logger = logging.getLogger(__name__)

logger.setLevel( logging.DEBUG )

#implementation where a result can generate new cluster jobs, and cancel in-progress work if we're done
def clusterStatusCallback(status, node, job):

    #"self" is problematic in this function
    #this is effectively a static method, and we can't access the return value
    #originally implementation worked with the retry queue as global, and the writeResult function also as global

    # Created = 5
    # Running = 6
    # ProvisionalResult = 7
    # Cancelled = 8
    # Terminated = 9
    # Abandoned = 10
    # Finished = 11

    #if(logger.isEnabledFor(logging.DEBUG)):
    #    logger.debug("=============cluster_status_cb===========")

    global ktTaskDone

    if(ktTaskDone):
        logger.debug("cluster status callback invoked, but task was done")
        return

    if(job == None):
        logger.warn("Received a null job: %s" % job)
        return

    if status == dispy.DispyJob.Finished:
        
        if(job.result == None):
            logger.warning("Received a null job result for job id: %s" % job.id)
            return
        
        if(logger.isEnabledFor(logging.DEBUG)):
            #TODO: better log statement for board state and turn count
            logger.debug('job finished for %s: %s' % (job.id, job.result))

        #at this point we've received a result board, assume coherent
        
        #are we done?
        #yes => signal that we are done, cancel remaining or in-progress work
        #no => add new cluster job for it
        
        #job.result is an array of ChessBoard
        for expandedBoard in job.result:
            
            if(expandedBoard == None):
                logger.error("Received a null board expansion result for job id: %s" % job.id)
            
            elif(isTourComplete(expandedBoard)):
                
                logger.info("Tour complete:\n%s\n" % expandedBoard.dump())
                #write final result
                #cancel in-progress work
                
                ktTaskDone = True
                
            else:                
                #TODO: add to queue rather than just submit
                
                #submit job for new board
                #don't have to worry about rexpanding a board we've already seen. KnightsTour.expandBoard checks for previous turns
                knightsTourTask.submitClusterJob( KnightsTour( expandedBoard.getBoardStateStr(), expandedBoard.getXDim(), expandedBoard.getYDim(), expandedBoard.getTurnCount() ) )
                
                logger.debug("Expanding board:\n%s\n" % expandedBoard.dump())
                
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

            
        #TODO: signal callback work is finished

    elif status == dispy.DispyJob.Terminated or status == dispy.DispyJob.Cancelled or status == dispy.DispyJob.Abandoned:
        logger.error("job failed for %s failed: %s" % (job.id, job.exception))

        #TODO: signal callback work is finished

        #TODO: remove id from result mapping?

    elif status == dispy.DispyNode.Initialized:
        logger.debug ("node %s with %s CPUs available" % (node.ip_addr, node.avail_cpus))
    # elif status == dispy.DispyNode.Created:
    #     print("created job with id %s" % job.id)
    # elif status == dispy.DispyNode.Running:
    #     #do nothing. running is a good thing
    #     pass
    else:  # ignore other status messages
        #if we're not logging warnings above
        pass
        
def isTourComplete(board):
    return board.getBoardStateStr().find(ChessBoard.BLANK) == -1
    
def signal_handler(sig, frame):
    print('SIGINT Caught, shutting down')
    logger.info("SIGINT Caught, shutting down")
    
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
    def __init__(self, confFile, startx=4, starty=4):
        super().__init__(confFile, enableRetryQueue=False)
        
        self.startx = startx
        self.starty = starty
      
    #overrides superclass method
    def initializeCluster(self):
        logger.info("initializing cluster for knights tour")
        
        clusterFactory = super().getClusterFactory()
        
        #supply the dispy lambda and the status callback. 
        #seeing issues trying to combine this statement with the clusterFactory retrieval abo
        #super().clusterStatusCallback is generic enough for most cases, but subclasses may want otherwise 
        
        taskCluster = clusterFactory.buildCluster( KnightsTour.expandBoard, clusterStatusCallback )
        
        #TODO: reallocate nodes in case we previously had a sloppy shutdown?
        
        super()._setCluster( taskCluster )
        
    def start(self):
        #blocks
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
        
        self.submitClusterJob( KnightsTour( startBoard.getBoardStateStr(), startBoard.getXDim(), startBoard.getYDim() ) )        
        
        #sleep for a bit to let expansions arrive, otherwise the waitFor will not block       
        super().waitForCompletion(10)

        ################################################
        
        logger.info("Job queue exhausted. Beginning shutdown...")

    def stop(self):
        
        logger.info("Stopping dogpile knightstour task")

        super().stop()

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
    
    global knightsTourTask 
    knightsTourTask = KnightsTourTask(args[1])
    
    knightsTourTask.initializeCluster()
        

    
    #blocks    
    knightsTourTask.start()
    
    print ("STOPPING")
    
    #shutdown
    knightsTourTask.stop()
    
    #save results
    logger.info("Writing results")
    #knightsTourTask.writeResults()

    logger.info("Exiting")

###############################
if __name__ == "__main__":
    main(sys.argv)
