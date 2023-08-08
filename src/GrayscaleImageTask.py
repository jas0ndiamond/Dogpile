import logging
import os
import time
import dispy

from Config import Config
from ClusterFactory import ClusterFactory
from DogPileTask import DogPileTask
from TransformableImage import TransformableImage
from Grayscaler import Grayscaler
from JobCompletionCounter import JobCompletionCounter
from DefaultClusterStatusCallback import DefaultClusterStatusCallback

class GrayscaleImageTask(DogPileTask):
    def __init__(self, confFile):
        super().__init__(confFile, enableRetryQueue=False)
        
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel( logging.DEBUG )
        
        self.srcDir = os.path.dirname( os.path.realpath(__file__) )
        
        #TODO: read this from config or expose method
        self.imageOutputDir = self.srcDir + "/../output"

        self.jobCounter = JobCompletionCounter()

        #try to make the output directory if it doesn't exist
        #TODO move to utils class
        if(os.path.exists(self.imageOutputDir) == False):
            try:
                os.makedirs(self.imageOutputDir, 0o755)
            except OSError:
                self.logger.warn ("Creation of output directory %s failed" % imageOutputDir)
                exit(1)
        else:
            self.logger.debug("Using existing output directory")
            
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
    def initializeCluster(self, clusterJobStatusCallback):
        self.logger.info("initializing cluster for grayscaling image")
        
        #TODO: check parameter is a function
        
        clusterFactory = super().getClusterFactory()
        
        #supply the dispy lambda and our job status callback. use default cluster callback.
        #TODO: can we require Dogpile subclasses to override a specific method?
        super()._setCluster( clusterFactory.buildCluster( Grayscaler.grayscaleImage, job_status_callback=clusterJobStatusCallback ) )
      
    def countJobCompleted(self):
        self.jobCounter.signalCompletedJob()
        
    def countJobFailed(self):
        self.jobCounter.signalFailedJob()
        
    def addImageFile(self, imageFile):
        self.sourceImageFiles.append(imageFile)
        
    def start(self):
        # do not block. waiting for completion and handling errors and retries should be handled externally
        
        self.logger.info("Starting DogPile task")
        
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
            
                self.logger.debug ("Row: %s" % row )
                
                #submit job to the dispy cluster
                #bind job id to result container, unfortunately we can only get
                #the job id after submitting it to the cluster, and sometimes the 
                #result arrives in the cluster status callback before the 
                #result binding below
                #0 may not be acceptable as an id
                
                
                self.logger.debug("Binding job id %s to row num %d" % (newJobID, rowNum))
                
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
                    self.logger.error("Cluster job submission returned an unexpected job id: %d vs %d" % (newJobID, newJob.id) )
                    #raise Exception("Cluster job submission returned an unexpected job id. Bailing")
                else:

                    self.jobsSubmitted += 1
            else:
                self.logger.error("Failed creating job")
                #TODO: fail out gracefully. signal not ready. do not want partial set of jobs submitted to cluster. check exception flow
                #raise Exception("Error during job submission. Bailing")
        
          
        self.logger.info("All jobs submitted: %d" % self.jobsSubmitted)

    def stop(self):
        
        self.logger.info("Stopping dogpile grayscale task")

        super().stop()
     
    def isFinished(self):
        
        # this task is considered done when the number of completed jobs equals the number of submitted jobs
        
        self.logger.debug("task isFinished invoked")
        
        #detect if there's any pending results for submitted work
        #inspect our completed jobs count
        #submitted jobs compared to completed(and written successfully) jobs
        
        retval = False
        completedJobs = self.jobCounter.getTotalCompletedJobCount()
        
        unfinishedJobs = self.jobsSubmitted - completedJobs
        if( unfinishedJobs == 0 ):
            self.logger.info("Application workload finished")
            retval = True
        elif (unfinishedJobs > 0):
            self.logger.info("Application workload not finished. Remaining jobs: %d" % unfinishedJobs)
        else:
            self.logger.error("Error counting completed jobs. Total: %d, completed: %d" % (self.jobsSubmitted, completedJobs) )
            
        #results check. opportunity to detect problems and potentially resubmit jobs to correct
        #are there any gaps in any images?
        #problematic outcome but not fixable by more work outside of the currently enabled reentrant work
        #don't try to fix buggy result consolidation here, just point it out
            
            
        self.logger.debug("task isFinished returning")
            
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
