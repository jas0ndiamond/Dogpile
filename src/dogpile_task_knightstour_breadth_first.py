import sys
import os
import logging
import time
import dispy
import signal
import timeit

from KnightsTourTask import KnightsTourTask
from ChessBoard import ChessBoard

#################################
#knights tour that stops at the first solution it finds
#board expansion is random so re-runs may generate different 
#results if multiple knights tours possible for a given position and board

####################################################
# log and logger configuration

#TODO: move to main method? maybe just the level config? global logger? logger may have to be declared here for the callback

logFile = "run.log"

#init logging outside of constructor so constructed objects can access
logging.basicConfig(filename=logFile, format='%(asctime)s [%(levelname)s] -- [%(name)s]-[%(funcName)s]: %(message)s')
logger = logging.getLogger(__name__)

logger.setLevel( logging.DEBUG )

#custom log levels
logging.getLogger("DogPileTask").setLevel(logging.DEBUG)
logging.getLogger("MongoDBJobManager").setLevel( logging.DEBUG )

####################################################

# callback for a node reporting job status
def jobStatusCallback(job):
    
    # dispy appears to execute this callback synchronously-
    # for the happy case of job finished without problem, and it's
    # not a solution, get to knightsTourTask.submitNewJob asap
    
    #quit if we're done or stopped. nodes will keep sending results so don't process
    global ktTaskDone
    if(ktTaskDone == True):
        logger.info("job status callback function invoked, but task was done. Returning...")
        return
        
    if(job == None):
        logger.warn("Received a null job: %s" % job)
        return
        
    if job.status == dispy.DispyJob.Finished:
    
        if(job.result == None):
            logger.warning("Received a null job result for job id: %s" % job.id)
            return
        
        #at this point we've received a result board, assume coherent
        
        #are we done?
        #yes => signal that we are done, cancel remaining or in-progress work
        #no => add new cluster job for it
        
        if logger.isEnabledFor(logging.DEBUG):
            start_time = timeit.default_timer()
            
            logger.debug("Received new boards: %d" % len(job.result))
        
        
        #job.result is an array of ChessBoard
        for expandedBoard in job.result:
            
            if(expandedBoard == None):
                logger.error("Received a null board expansion result for job id: %s" % job.id)
            elif(isTourComplete(expandedBoard)):
                
                # is this the solution state?
                
                logger.info("*********Tour complete:\n%s\n" % expandedBoard.dump())
                
                #write final result
                #cancel in-progress work
                
                ktTaskDone = True
                
                knightsTourTask.setSolutionBoard(expandedBoard)
                
                break
            else:                
                #submit job for new board
                #don't have to worry about rexpanding a board we've already seen. KnightsTour.expandBoard checks for previous turns
                knightsTourTask.submitNewJob( expandedBoard )
    elif job.status == dispy.DispyJob.Terminated or job.status == dispy.DispyJob.Cancelled or job.status == dispy.DispyJob.Abandoned:
        logger.error("job failed for %s failed: %s" % (job.id, job.exception))

        #TODO: signal callback work is finished

    else:  # don't need explicit handling of other status messages
        logger.warn("Unexpected job status: %d" % job.status )
        #if we're not logging warnings above
        pass
        
#TODO: move utility methods somewhere reusable?
def isTourComplete(board):
    return board.getBoardStateStr().find(ChessBoard.BLANK) == -1
    
def signal_handler(sig, frame):
    sig_str = "SIGINT Caught, shutting down. Nodes may take a while to dump their pending work."
    print(sig_str)
    logger.info(sig_str)
    
    global ktTaskDone
    ktTaskDone = True
    
    knightsTourTask.stop()
    
    time.sleep(10)
    
    logger.info("Exiting")
    sys.exit(1)
        
##############################

def main(args):

    if(len(args) < 1):
        print("Usage: dogpile_task_knightstour_first_solution.py conf_file")
        exit(1)
    
    signal.signal(signal.SIGINT, signal_handler)
    
    #global so the dispy callback can reference
    global ktTaskDone    
    ktTaskDone = False
    
    global ktFirstSolutionBoard
    ktFirstSolutionBoard = None
    
    #######################
    #super basic board - quick and easy solution
    #donut board
    #should NOT hit the database
    
    #startBoard = ChessBoard(3, 3)
    #startBoard.setSpace(0, 0, ChessBoard.KNIGHT)
    #startBoard.setSpace(1, 1, ChessBoard.DEAD)
    #######################
    
    #######################
    #super basic board - quick and easy NO solution
    #should NOT hit the database
    
    #startBoard = ChessBoard(3, 3)
    #startBoard.setSpace(0, 0, ChessBoard.KNIGHT)
    #######################
    
    #######################
    #basic board
    # easy solution from 
    #hits the database
    #takes about an 100 min with 20 cores
    
    startBoard = ChessBoard(5, 5)
    #startBoard.setSpace(0, 0, ChessBoard.KNIGHT)
    startBoard.setSpace(2, 2, ChessBoard.KNIGHT)
    #######################
    
    global knightsTourTask 
    knightsTourTask = KnightsTourTask(args[1], startBoard)
    
    knightsTourTask.initializeCluster(jobStatusCallback)
           
    knightsTourTask.start()
    
    #wait for cluster operation to complete
    knightsTourTask.waitForWorkloadCompletion()
    
    output_str = "Work completed. Shutting down..."
    
    #both to logger and stdout
    logger.info(output_str)
    print(output_str)
    
    #shutdown
    knightsTourTask.stop()
    
    solutionBoard = knightsTourTask.getSolutionBoard()
    if( solutionBoard == None):
        output_str = "KT Depth-First No Solution Discovered"
    else:
        output_str = "KT Depth-First Solution discovered:\n%s" % solutionBoard.dump()
    
    #both to logger and stdout
    print(output_str)
    logger.info(output_str)

    #both to logger and stdout
    output_str = "Exiting"
    print(output_str)
    logger.info(output_str)

###############################
if __name__ == "__main__":
    main(sys.argv)
