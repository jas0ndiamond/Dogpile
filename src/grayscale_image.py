import sys
import os
import logging
import time

import numpy as np

from PIL import Image

import dispy
import dispy.httpd

#src/app => src
srcDir = os.path.dirname( os.path.realpath(__file__))

from Config import Config
from Grayscaler import Grayscaler
from TransformableImage import TransformableImage
from ClusterFactory import ClusterFactory

#timeout after job submission and status report
####################
#doesn't seem to work, need to add timeout in dispy source
#TODO: figure this out
####################
dispy.config.MsgTimeout = 1200
dispy.MsgTimeout = 1200

imageOutputDir = srcDir + "/../output"

logging.basicConfig(format='%(asctime)s [%(levelname)s] -- [%(name)s]-[%(funcName)s]: %(message)s')
logger = logging.getLogger()

#TODO: make directory if it doesn't exist

###############################

images = []

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

            #how to map the result back to a section of an image

            #search all images for image.hasJobId

            imageToUpdate = getImageByJobId(job.id)

            if(imageToUpdate != None):

                logger.debug("Writing result from job %d to image %s" % (job.id, imageToUpdate.getFile()))

                imageToUpdate.writeResult(job.id, job.result)
            else:
                logger.warning("Could not find image for job id %d" % job.id)

				#TODO: add to result queue

            #TODO: signal callback work is finished

        elif status == dispy.DispyJob.Terminated or status == dispy.DispyJob.Cancelled or status == dispy.DispyJob.Abandoned:
            logger.warn('job failed for %s failed: %s' % (job.id, job.exception))

            #TODO: signal callback work is finished

        elif status == dispy.DispyNode.Initialized:
            logger.debug('node %s with %s CPUs available' % (node.ip_addr, node.avail_cpus))
        # elif status == dispy.DispyNode.Created:
        #     print("created job with id %s" % job.id)
        # elif status == dispy.DispyNode.Running:
        #     #do nothing. running is a good thing
        #     pass
        else:  # ignore other status messages
            #print("ignoring status %d" % status)
            pass


def getImageByJobId(id):
    result = None

    for image in images:
        if(image.hasJobId(id)):
            result = image
            break

    return result

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


    #cluster_dependencies = [ ("%s/Grayscaler.py" % srcDir) ]

    #logger.debug(("launching cluster with dependencies %s" % cluster_dependencies)

    #TODO: msgTimeout arg?
    #cluster = dispy.JobCluster(Grayscaler.grayscaleImage, cluster_status=cluster_status_cb, nodes=cluster_nodes, depends=cluster_dependencies, loglevel=loglevel_dispy,  ip_addr=client_ip, pulse_interval=pulse_interval, secret=node_secret)

    #TODO: cluster tostring log statement

    http_server = dispy.httpd.DispyHTTPServer(cluster)

    #sleep just in case a host is slow to respond
    logger.info("Sleeping, buying time for sluggish nodes to report...")
    time.sleep(5)


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

    logger.info("Job queue exhausted. Shutting down...")
    time.sleep(5)

    if(http_server):
        http_server.shutdown()

    if(cluster):
        cluster.close()

    ###############

    #save results
    logger.info("Writing results")

    for transformedImage in images:
        transformedImage.writeImage()

    logger.info("Exiting")


###############################
if __name__ == "__main__":
    main(sys.argv)
