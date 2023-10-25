import sys
import logging
import time
import signal

#only really used for job.status int compares
#TODO:  would be nice to drop the import
import dispy

from GrayscaleImageTask import GrayscaleImageTask

####################################################
# log and logger configuration

#TODO: move to main method? maybe just the level config? global logger?

logFile = "run.log"

#init logging outside of constructor so constructed objects can access
logging.basicConfig(filename=logFile, format='%(asctime)s [%(levelname)s] -- [%(name)s]-[%(funcName)s]: %(message)s')
logger = logging.getLogger(__name__)

#debug testing the callbacks
#logging.getLogger("DefaultClusterStatusCallback").setLevel(logging.DEBUG)

#TODO possibly get this from a section in config
logger.setLevel( logging.DEBUG )

#custom log levels
logging.getLogger("DogPileTask").setLevel(logging.DEBUG)

####################################################


#TODO: sure would be nice to move this to its own file
#simple generic implementation. listen for job statuses
#jobs do not generate other jobs for this problem
def jobStatusCallback(job):
    # Created = 5
    # Running = 6
    # ProvisionalResult = 7
    # Cancelled = 8
    # Terminated = 9
    # Abandoned = 10
    # Finished = 11

    logger.debug("=============job_status_cb===========")

    if(job == None):
        logger.warn("Received a null job: %s" % job)
        return
        
    #quit if we're done or stopped. nodes will keep sending results
    global grayscaleImageTaskDone
    if(grayscaleImageTaskDone == True):
        logger.info("job status callback function invoked, but task was done. Returning...")
        return

    if job.status == dispy.DispyJob.Finished:
        logger.debug("job finished for %s: %s" % (job.id, job.result))

        #a block is finished transforming

        #lookup resultcontainer for job.id and attempt to write to it

        #"self" is problematic here *******************
        #since this is a callback invoked from dispy, and we can't access the return value
        #originally implementation worked with the retry queue as global, and the writeResult function also as global

        #search all images for image.hasJobId
        if grayscaleImageTask.writeClusterJobResult(job) == False:
            logger.debug("writing result for job %d failed" % job.id )
        
        #signal job itself is finished
        grayscaleImageTask.countJobCompleted()
        
        #TODO: signal callback work is finished
        #could push a job id onto a callback_running collection and remove it when the callback returns
    elif job.status == dispy.DispyJob.Terminated or job.status == dispy.DispyJob.Cancelled or job.status == dispy.DispyJob.Abandoned:
        logger.error("job failed for %s failed: %s" % (job.id, job.exception))

        #TODO: here it's possible a node doesn't have a required library or module installed. 
        #resubmits need to be careful and enforce a maximum retry count for a job
        
        #possible a failure should abort the dispy work distribution
        
        #expose a function in DogPileTask to allow direct adding of jobs to retry queue
        #either way the job param to the callback is technically done
        
        grayscaleImageTask.countJobFailed()
    else:  # don't need explicit handling of other status messages
        logger.warn("Unexpected job status: %d" % job.status )
        #if we're not logging warnings above
        pass

def signal_handler(sig, frame):
    sig_str = "SIGINT Caught, shutting down. Nodes may take a while to dump their pending work."
    print(sig_str)
    logger.info(sig_str)
    
    global grayscaleImageTaskDone
    grayscaleImageTaskDone = True
    
    grayscaleImageTask.stop()
    
    time.sleep(10)
    
    logger.info("Exiting")
    sys.exit(1)

################
def main(args):

    if(len(args) < 3):
        print("Usage: dogpile_task_grayscale_image.py conf_file file1 file2 file3...")
        exit(1);

    signal.signal(signal.SIGINT, signal_handler)

    #global so the job status callback and cluster status callback can reference
    
    #TODO: deliberately get config file
    
    global grayscaleImageTask 
    grayscaleImageTask = GrayscaleImageTask(args[1])
        
    # add our input images
    for imageFile in args[2:]:
        grayscaleImageTask.addImageFile(imageFile)
    
    global grayscaleImageTaskDone
    grayscaleImageTaskDone = False
    
    grayscaleImageTask.initializeCluster(jobStatusCallback)
        
    #does not block. must context-aware wait 
    grayscaleImageTask.start()
    
    #wait for cluster operation to complete
    grayscaleImageTask.waitForWorkloadCompletion()
    
    logger.info("Work completed. Shutting down.")

    ################################################
    
    #shutdown
    grayscaleImageTask.stop()
    
    #save results
    logger.info("Writing results")
    print("Writing results...")

    grayscaleImageTask.writeResults()


    logger.info("Exiting")

###############################
if __name__ == "__main__":
    main(sys.argv)
