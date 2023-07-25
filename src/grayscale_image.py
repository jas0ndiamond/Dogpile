#######
#PoC of grayscaling an image with a dispy cluster
#######

import sys
import os
import logging
import time

import dispy
import dispy.httpd

#src/app => src
srcDir = os.path.dirname( os.path.realpath(__file__) )

from Config import Config
from Grayscaler import Grayscaler
from TransformableImage import TransformableImage
from ClusterFactory import ClusterFactory
from ResultRetryQueue import ResultRetryQueue
#TODO: ^^^ remove retry queue

#timeout after job submission and status report
####################
#doesn't seem to work, need to add timeout in dispy source
#TODO: figure this out
####################
dispy.config.MsgTimeout = 1200
dispy.MsgTimeout = 1200

logging.basicConfig(filename='run.log', format='%(asctime)s [%(levelname)s] -- [%(name)s]-[%(funcName)s]: %(message)s')
logger = logging.getLogger()


imageOutputDir = srcDir + "/../output"

#make the output directory if it doesn't exist
if(os.path.exists(imageOutputDir) == False):
    try:
        os.makedirs(imageOutputDir, 0o755)
    except OSError:
        logger.warn ("Creation of output directory %s failed" % imageOutputDir)
        exit(1)
else:
    logger.debug("Using existing output directory")

###############################

images = []

def writeNodeResult(job):
    imageToUpdate = getImageByJobId(job.id)
    retval = False

    if(imageToUpdate != None):
        logger.debug("Writing result from job %d to image %s" % (job.id, imageToUpdate.getFile()))
        
        imageToUpdate.writeResult(job.id, job.result)

        #TODO: remove id from result mapping?

        retval = True
    else:
        logger.warning("Could not find image for job id %d" % job.id)

    return retval



def getImageByJobId(id):
    result = None

    for image in images:
        if(image.hasJobId(id)):
            result = image
            break

    return result

#TODO: find a good spot to initialize this
#can't seem to add to retry queue with addjob in cluster_status_cb
#No logging for ResultRetryQueue but there is for Transformable Image
#need this initialized here so the dispy callback cluster_status_cb can reference it
#



###############################

def cluster_status_cb(status, node, job):

        # Created = 5
        # Running = 6
        # ProvisionalResult = 7
        # Cancelled = 8
        # Terminated = 9
        # Abandoned = 10
        # Finished = 11

        #self.logger.debug("=============cluster_status_cb===========")

        if status == dispy.DispyJob.Finished:
            logger.debug('job finished for %s: %s' % (job.id, job.result))

            #a block is finished transforming

            #search all images for image.hasJobId
            if writeNodeResult(job) == False:

                logger.debug('writing result for job %d failed, adding to retry queue' % job.id )

                retryQueue.addJob( job )

            #TODO: signal callback work is finished

        elif status == dispy.DispyJob.Terminated or status == dispy.DispyJob.Cancelled or status == dispy.DispyJob.Abandoned:
            logger.warn('job failed for %s failed: %s' % (job.id, job.exception))

            #TODO: signal callback work is finished

            #TODO: remove id from result mapping?

        elif status == dispy.DispyNode.Initialized:
            logger.debug('node %s with %s CPUs available' % (node.ip_addr, node.avail_cpus))
        # elif status == dispy.DispyNode.Created:
        #     print("created job with id %s" % job.id)
        # elif status == dispy.DispyNode.Running:
        #     #do nothing. running is a good thing
        #     pass
        else:  # ignore other status messages
            #print("ignoring status %d" % status)
            
            logger.warn("Unexpected job status: %d" % status )
            pass    

            



###########################3




def main(args):

    if(len(args) < 3):
        print("Usage: grayscale_image.py conf_file file1 file2 file3...")
        exit(1);

    conf = Config(args[1])

    conf.dump()

    logger.setLevel(conf.get_loglevel())

    factory = ClusterFactory(conf)


    cluster = factory.buildCluster(Grayscaler.grayscaleImage, cluster_status_cb)

    jobs = []

    #global so callback functions and utilities can reference
    global retryQueue 
    retryQueue = ResultRetryQueue(retryCallback=writeNodeResult)

    #cluster_dependencies = [ ("%s/Grayscaler.py" % srcDir) ]

    #logger.debug(("launching cluster with dependencies %s" % cluster_dependencies)

    #TODO: msgTimeout arg?
    #cluster = dispy.JobCluster(Grayscaler.grayscaleImage, cluster_status=cluster_status_cb, nodes=cluster_nodes, depends=cluster_dependencies, loglevel=loglevel_dispy,  ip_addr=client_ip, pulse_interval=pulse_interval, secret=node_secret)

    #TODO: cluster tostring log statement

    http_server = dispy.httpd.DispyHTTPServer(cluster)

    #sleep just in case a host is slow to respond
    logger.info("Sleeping, buying time for sluggish nodes to report...")
    time.sleep(5)

    #start retry queue thread
    retryQueue.start()

    #expand image files into a list of jobs

    logger.info("Submitting jobs")

    #a transformable image has a collection of job ids

    for file in args[2:]:


        #need image object that has map of job ids to result rows
        #map of job ids to images
            #array of images each with has_id function
        #for an image, a map of job ids to rows

        #for an image file, map the resultant job id to a result matrix


        myImage = TransformableImage(file, imageOutputDir)

        rowNum = 0
        for row in myImage.getPixelRows():

            logger.debug ("Row: %s" % row )

            newJob = cluster.submit( Grayscaler( row ) )

            if(newJob):
                logger.debug("Binding job id %s to row num %d" % (newJob.id, rowNum))

                myImage.bindRow(newJob.id, rowNum)

                rowNum += 1
            else:
                logger.warning("Failed creating job")

                #TODO: fail out gracefully

        images.append(myImage)

    #         # Created = 5
    #         # Running = 6
    #         # ProvisionalResult = 7
    #         # Cancelled = 8
    #         # Terminated = 9
    #         # Abandoned = 10
    #         # Finished = 11
    #


    ################################################
    #wait for cluster operation to complete
    #cluster.wait does not wait for callbacks to finish
    quit = False
    while( quit != True and cluster.wait(10) != True ):
        cluster.print_status()

    ################################################

    #TODO: compare finished job count to expected
    #is cluster.wait suitable for determining if we're done?

    logger.info("Job queue exhausted. Shutting down...")

    retryQueue.stop()

    logger.debug("Signal termination for Result Retry Queue thread")

#    resultRetryQueueRunning = False

    #wait for shutdowns
    time.sleep(5)

    if(http_server):
        http_server.shutdown()

    if(cluster):
        cluster.close()

    ###############

    #save results
    logger.info("Writing results")

    #TODO: move to thread and write alongside main processing. 
    #for applications like a camera feed, the input never stops coming,
    #so we can't wait to the end or we'll run out of memory
    for transformedImage in images:
        transformedImage.writeImage()

    logger.info("Exiting")


###############################
if __name__ == "__main__":
    main(sys.argv)
