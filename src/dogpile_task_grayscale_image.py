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

logFile = "run.log"

#init logging outside of constructor so constructed objects can access
logging.basicConfig(filename=logFile, format='%(asctime)s [%(levelname)s] -- [%(name)s]-[%(funcName)s]: %(message)s')
logger = logging.getLogger(__name__)

#logger.setLevel( logging.getLogger().getEffectiveLevel() )
logger.setLevel( logging.DEBUG )

#default generic implementation. listen for job statuses, add to retry queue if there's no result mapping yet
def clusterStatusCallback(status, node, job):

    # Created = 5
    # Running = 6
    # ProvisionalResult = 7
    # Cancelled = 8
    # Terminated = 9
    # Abandoned = 10
    # Finished = 11

    #self.logger.debug("=============cluster_status_cb===========")

    if status == dispy.DispyJob.Finished:
        #DogPileTask._clusterStatusCallbackLog('job finished for %s: %s' % (job.id, job.result))
        print('job finished for %s: %s' % (job.id, job.result))

        #a block is finished transforming


        #lookup resultcontainer for job.id and attempt to write to it

        #"self" is problematic here *******************
        #this is effectively a static method, and we can't access the return value
        #originally implementation worked with the retry queue as global, and the writeResult function also as global

        #search all images for image.hasJobId
        if grayscaleImageTask.writeClusterJobResult(job) == False:

            #DogPileTask._clusterStatusCallbackLog('writing result for job %d failed, adding to retry queue' % job.id )
            print('writing result for job %d failed, adding to retry queue' % job.id )

            #this is done in writeClusterJobResult
            #DogPileTask.addRetryJob(job)
            #retryQueue.addJob( job )

        #TODO: signal callback work is finished

    elif status == dispy.DispyJob.Terminated or status == dispy.DispyJob.Cancelled or status == dispy.DispyJob.Abandoned:
        #DogPileTask._getLogger.warn('job failed for %s failed: %s' % (job.id, job.exception))

        #DogPileTask._clusterStatusCallbackLog("job failed for %s failed: %s" % (job.id, job.exception))
        print("job failed for %s failed: %s" % (job.id, job.exception))

        #TODO: signal callback work is finished

        #TODO: remove id from result mapping?

    elif status == dispy.DispyNode.Initialized:
        #DogPileTask._clusterStatusCallbackLog("node %s with %s CPUs available" % (node.ip_addr, node.avail_cpus))
        print ("node %s with %s CPUs available" % (node.ip_addr, node.avail_cpus))
    # elif status == dispy.DispyNode.Created:
    #     print("created job with id %s" % job.id)
    # elif status == dispy.DispyNode.Running:
    #     #do nothing. running is a good thing
    #     pass
    else:  # ignore other status messages
        #DogPileTask._clusterStatusCallbackLog("Unexpected job status: %d" % status )
        #print("Unexpected job status: %d" % status )
        #if we're not logging warnings above
        pass

class GrayScaleImageTask(DogPileTask):
    def __init__(self, confFile):
        super().__init__(confFile)
        
        #TODO: read this from superclass config
        self.imageOutputDir = srcDir + "/../output"

        #try to make the output directory if it doesn't exist
        if(os.path.exists(self.imageOutputDir) == False):
            try:
                os.makedirs(self.imageOutputDir, 0o755)
            except OSError:
                logger.warn ("Creation of output directory %s failed" % imageOutputDir)
                exit(1)
        else:
            logger.debug("Using existing output directory")
         
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
        
        #supply the dispy lambda and the status callback. 
        #seeing issues trying to combine this statement with the clusterFactory retrieval abo
        #super().clusterStatusCallback is generic enough for most cases, but subclasses may want otherwise 
        super()._setCluster( clusterFactory.buildCluster( Grayscaler.grayscaleImage, clusterStatusCallback ) )
        
    def addImageFile(self, imageFile):
        self.sourceImageFiles.append(imageFile)
        
    def start(self):
        #blocks
        logger.info("Starting DogPile task")
        
        #do we have a valid cluster?
        if(super().getClusterFactory() == None):
            raise Exception("Cluster was not build successfully. Bailing")
            
        #TODO: read from config
        self.enableHttpServer = True
        
        super().startDispyHttpServer()
            
        ################################################
        #for each file in sourceImageFiles, create jobs and submit to cluster
            
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
                newJob = super().submitClusterJob( Grayscaler( row ) )
                
                if(newJob):
                    logger.debug("Binding job id %s to row num %d" % (newJob.id, rowNum))
                
                    sourceImage.bindRow(newJob.id, rowNum)
                
                    rowNum += 1
                else:
                    logger.warning("Failed creating job")
                
                #TODO: fail out gracefully
            
            self.workloadImages.append(sourceImage)
            
          
        print("entering wait loop")
        
        
        #TODO: possibly move to superclass so we're not worrying about dispy internals
        ################################################
        #wait for cluster operation to complete
        #cluster.wait does not wait for callbacks to finish
        
        super().waitForCompletion(10)

        ################################################
        
        logger.info("Job queue exhausted. Beginning shutdown...")

    def stop(self):
        
        logger.info("Stopping dogpile grayscale task")

        super().stop()
     
    def writeResults(self):
        #TODO: move to thread and write alongside main processing. 
        #for applications like a camera feed, the input never stops coming,
        #so we can't wait to the end or we'll run out of memory
        for transformedImage in self.workloadImages:
            transformedImage.writeImage()
        
        
################

    #TODO: make generic and push to superclass. support overloading or callback supplication




    def getClusterJobResultByJobId(self, id):
        
        print("Executing Subtask.getClusterJobResultByJobId")
        
        result = None

        for image in self.workloadImages:
            if(image.hasJobId(id)):
                result = image
                break

        return result

################
def main(args):

    if(len(args) < 3):
        print("Usage: grayscale_image.py conf_file file1 file2 file3...")
        exit(1);

    
    #global so the callback can reference
    global grayscaleImageTask 
    grayscaleImageTask = GrayScaleImageTask(args[1])
    
    #dogPileTask.setWriteNodeResultCallback(writeNodeResult)
    
    for imageFile in args[2:]:
        grayscaleImageTask.addImageFile(imageFile)
    
    
    grayscaleImageTask.initializeCluster()
        

    
    #blocks    
    grayscaleImageTask.start()
    
    print ("STOPPING")
    
    #shutdown
    grayscaleImageTask.stop()
    
    #save results
    logger.info("Writing results")
    grayscaleImageTask.writeResults()

    logger.info("Exiting")

###############################
if __name__ == "__main__":
    main(sys.argv)
