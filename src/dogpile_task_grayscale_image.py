import sys
import os
import logging
import time
import dispy


#src/app => src
#TODO: move to constructor?
srcDir = os.path.dirname( os.path.realpath(__file__) )

from Config import Config
from ClusterFactory import ClusterFactory
from DogPileTask import DogPileTask
from TransformableImage import TransformableImage
from Grayscaler import Grayscaler
from JobCompletionCounter import JobCompletionCounter
from DefaultClusterStatusCallback import DefaultClusterStatusCallback

####################################################
# log and logger configuration

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

class GrayScaleImageTask(DogPileTask):
    def __init__(self, confFile):
        super().__init__(confFile, enableRetryQueue=False)
        
        #TODO: read this from config
        self.imageOutputDir = srcDir + "/../output"

        self.jobCounter = JobCompletionCounter()

        #try to make the output directory if it doesn't exist
        #TODO move to utils class
        if(os.path.exists(self.imageOutputDir) == False):
            try:
                os.makedirs(self.imageOutputDir, 0o755)
            except OSError:
                logger.warn ("Creation of output directory %s failed" % imageOutputDir)
                exit(1)
        else:
            logger.debug("Using existing output directory")
            
        self.jobsSubmitted = 0
         
        #TODO: read from config. try to handle in DogPileTask
        self.enableHttpServer = True
         
        #the collection of image files that we'll operate on
        self.sourceImageFiles = []
        
        #the collection of result containers. 
        #entities for storing results of the cluster work
        #allocated when jobs are submitted
        #TODO: better name? move to superclass? all tasks will have result containers
        self.workloadImages = []
      
    #overrides superclass method
    def initializeCluster(self):
        logger.info("initializing cluster for grayscaling image")
        
        clusterFactory = super().getClusterFactory()
        
        #supply the dispy lambda and our job status callback. use default cluster callback.
        #TODO: can we require Dogpile subclasses to override a specific method?
        super()._setCluster( clusterFactory.buildCluster( Grayscaler.grayscaleImage, job_status_callback=jobStatusCallback ) )
      
    def countJobCompleted(self):
        self.jobCounter.signalCompletedJob()
        
    def countJobFailed(self):
        self.jobCounter.signalFailedJob()
        
    def addImageFile(self, imageFile):
        self.sourceImageFiles.append(imageFile)
        
    def start(self):
        # do not block. waiting for completion and handling errors and retries should be handled externally
        
        logger.info("Starting DogPile task")
        
        #do we have a valid cluster?
        if(super().getClusterFactory() == None):
            raise Exception("Cluster was not built successfully. Bailing")
        
        if(self.enableHttpServer == True):
            super().startDispyHttpServer()
            
        ################################################
        #for each file in sourceImageFiles, create jobs for the job manager
            
        jobsToSubmit = []
            
        # TODO: if submitting cluster jobs with a specified job id is viable, then generate all ids before submitting all jobs
            
        # TODO: override this so different grayscale image tasks can generate jobs in their own way
        # by row
        # by col
        # by pixel
        # by random pixel group
        newJobID = 1
        for sourceImageFile in self.sourceImageFiles:
            #need image object that has map of job ids to result rows
            #map of job ids to images
            #array of images each with has_id function
            #for an image, a map of job ids to rows
            
            #for an image file, map the resultant job id to a result matrix
            
            #result container for a cluster job
            sourceImage = TransformableImage(sourceImageFile, self.imageOutputDir)
            
            #TODO: rename, won't always be a row. need more generic
            rowNum = 0
            
            for row in sourceImage.getPixelRows():
            
                logger.debug ("Row: %s" % row )
                
                #submit job to the dispy cluster
                #bind job id to result container, unfortunately we can only get
                #the job id after submitting it to the cluster, and sometimes the 
                #result arrives in the cluster status callback before the 
                #result binding below
                #0 may not be acceptable as an id
                
                
                logger.debug("Binding job id %s to row num %d" % (newJobID, rowNum))
                
                # result bind has to happen before job submission
                sourceImage.bindRow(newJobID, rowNum)
                
                jobsToSubmit.append( ( newJobID, Grayscaler( row ) ) )
                
                rowNum += 1
                newJobID += 1
                
            
            self.workloadImages.append(sourceImage)
            
            
        #######################
        # submit jobs from job manager
        
        for job in jobsToSubmit:
            # ( newJobID, Grayscaler( row ) )
            
            newJobID = job[0]
            
            newJob = self.submitClusterJobWithID( job[1], newJobID  )
                
            if(newJob):
                if( newJob.id != newJobID ):
                    logger.error("Cluster job submission returned an unexpected job id: %d vs %d" % (newJobID, newJob.id) )
                    #raise Exception("Cluster job submission returned an unexpected job id. Bailing")
                else:

                    self.jobsSubmitted += 1
            else:
                logger.error("Failed creating job")
                #TODO: fail out gracefully. signal not ready. do not want partial set of jobs submitted to cluster. check exception flow
                #raise Exception("Error during job submission. Bailing")
        
          
        logger.info("All jobs submitted: %d" % self.jobsSubmitted)

    def stop(self):
        
        logger.info("Stopping dogpile grayscale task")

        super().stop()
     
    def isFinished(self):
        
        # this task is considered done when the number of completed jobs equals the number of submitted jobs
        
        logger.debug("task isFinished invoked")
        
        #detect if there's any pending results for submitted work
        #inspect our completed jobs count
        #submitted jobs compared to completed(and written successfully) jobs
        
        retval = False
        completedJobs = self.jobCounter.getTotalCompletedJobCount()
        
        unfinishedJobs = self.jobsSubmitted - completedJobs
        if( unfinishedJobs == 0 ):
            logger.info("Application workload finished")
            retval = True
        elif (unfinishedJobs > 0):
            logger.info("Application workload not finished. Remaining jobs: %d" % unfinishedJobs)
        else:
            logger.error("Error counting completed jobs. Total: %d, completed: %d" % (self.jobsSubmitted, completedJobs) )
            
        #results check. opportunity to detect problems and potentially resubmit jobs to correct
        #are there any gaps in any images?
        #problematic outcome but not fixable by more work outside of the currently enabled reentrant work
        #don't try to fix buggy result consolidation here, just point it out
            
            
        logger.debug("task isFinished returning")
            
        return retval
     
    def writeResults(self):
        #TODO: move to thread and write alongside main processing. 
        #for applications like a camera feed, the input never stops coming,
        #so we can't wait to the end or we'll run out of memory
        for transformedImage in self.workloadImages:
            transformedImage.writeImage()

    #used by DogPileTask.writeClusterJobResult
    def getClusterJobResultByJobId(self, id):
        
        result = None

        for image in self.workloadImages:
            if(image.hasJobId(id)):
                result = image
                break

        return result
        
    def removeResultMapping(self, id):
        #TODO remove result mapping from our pile of workloadImages if possible
        pass

################
def main(args):

    if(len(args) < 3):
        print("Usage: dogpile_task_grayscale_image.py conf_file file1 file2 file3...")
        exit(1);

    #global so the job status callback and cluster status callback can reference
    global grayscaleImageTask 
    grayscaleImageTask = GrayScaleImageTask(args[1])
        
    for imageFile in args[2:]:
        grayscaleImageTask.addImageFile(imageFile)
    
    
    grayscaleImageTask.initializeCluster()
        
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
