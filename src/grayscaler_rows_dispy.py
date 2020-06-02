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

#sys.path.insert(0, src_dir + "/transform")

from Grayscaler import Grayscaler
from TransformableImage import TransformableImage

#timeout after job submission and status report
####################
#doesn't seem to work, need to add timeout in dispy source
####################
dispy.config.MsgTimeout = 1200
dispy.MsgTimeout = 1200

imageOutputDir = srcDir + "/../output"

#TODO: make directory if it doesn't exist

###############################



images = []


def getImageByJobId(id):
    result = None

    for image in images:
        if(image.hasJobId(id)):
            result = image
            break

    return result

def cluster_status_cb(status, node, job):

    # Created = 5
    # Running = 6
    # ProvisionalResult = 7
    # Cancelled = 8
    # Terminated = 9
    # Abandoned = 10
    # Finished = 11

    print("=============cluster_status_cb===========")

    if status == dispy.DispyJob.Finished:
        print('job finished for %s: %s' % (job.id, job.result))

        #a row is finished transforming

        #how to map the result back to a section of an image

        #search all images for image.hasJobId

        imageToUpdate = getImageByJobId(job.id)

        if(imageToUpdate != None):

            print("Writing result from job %d to iamge %s" % (job.id, imageToUpdate.getFile()))

            imageToUpdate.writeResult(job.id, job.result)
        else:
            print("Could not find image for job id %d" % job.id)

        #TODO: signal callback work is finished

    elif status == dispy.DispyJob.Terminated or status == dispy.DispyJob.Cancelled or status == dispy.DispyJob.Abandoned:
        print('job failed for %s failed: %s' % (job.id, job.exception))

        #TODO: signal callback work is finished

    elif status == dispy.DispyNode.Initialized:
        print('node %s with %s CPUs available' % (node.ip_addr, node.avail_cpus))
    # elif status == dispy.DispyNode.Created:
    #     print("created job with id %s" % job.id)
    # elif status == dispy.DispyNode.Running:
    #     #do nothing. running is a good thing
    #     pass
    else:  # ignore other status messages
        #print("ignoring status %d" % status)
        pass


def main(args):

    if(len(args) < 2):
        print("Need at least 1 file")
        exit(1);

    #todo: config file
    cluster_nodes = ['192.168.1.22', '192.168.1.20', '192.168.1.6']
    client_ip = '192.168.1.20'
    pulse_interval = 300
    node_secret = "derpy"

    jobs = []


    cluster_dependencies = [ ("%s/Grayscaler.py" % srcDir) ]

    print("launching cluster with dependencies %s" % cluster_dependencies)


    cluster = dispy.JobCluster(Grayscaler.grayscaleImage, cluster_status=cluster_status_cb, nodes=cluster_nodes, depends=cluster_dependencies, loglevel=logging.DEBUG,  ip_addr=client_ip, pulse_interval=pulse_interval, secret=node_secret)



    http_server = dispy.httpd.DispyHTTPServer(cluster)

    #sleep just in case a host is slow to respond
    print("Sleeping, buying time for sluggish nodes to report...")
    time.sleep(5)


    #expand image files into a list of jobs

    print("Submitting jobs")



    #an image record is a tuple of
        # row index
        # filename
        # job id
        # data

    #a transformable image has a collection of job ids

    for file in args[1:]:


        #need image object that has map of job ids to result rows
        #map of job ids to images
            #array of images each with has_id function
        #for an image, a map of job ids to rows

        #for an image file, map the resultant job id to a result matrix


        myImage = TransformableImage(file, imageOutputDir)

        rowNum = 0
        for row in myImage.getPixelRows():

            print ("Row: %s" % row )

            newJob = cluster.submit( Grayscaler( row ) )

            if(newJob):
                print("Binding job id %s to row num %d" % (newJob.id, rowNum))

                myImage.bindRow(newJob.id, rowNum)

                rowNum += 1
            else:
                print("Failed creating job")

        images.append(myImage)

        #jobs.append(newjob)

#############################

    #print("Job submitted: %d" % len(jobs) )

    # finishedJobs =0
    # while( finishedJobs < len(jobs) ):
    #     print("Sleeping %d seconds before next status update" % 3)
    #     finishedJobs = 0
    #     time.sleep(3)
    #
    #     for job in jobs:
    #         #print( "Job status %d: %s" % job.id, job.status)
    #
    #         # Created = 5
    #         # Running = 6
    #         # ProvisionalResult = 7
    #         # Cancelled = 8
    #         # Terminated = 9
    #         # Abandoned = 10
    #         # Finished = 11
    #
    #         if(job.status == dispy.DispyJob.Cancelled or job.status == dispy.DispyJob.Terminated or job.status == dispy.DispyJob.Finished):
    #             #TODO block print of ids => results
    #             finishedJobs += 1
    #
    #         print("Job id: %s ==> status: %s" % (job.id, job.status) )
    #
    #
    #     cluster.print_status()
    #
    # print("Job loop finished")

    ################################################
    #wait for cluster operation to complete
    #cluster.wait does not wait for callbacks to finish
    quit = False
    while( quit != True and cluster.wait(10) != True ):
        cluster.print_status()

    ################################################

    #TODO: compare finished job count to expected

    print("Shutting down...")
    time.sleep(5)

    if(http_server):
        http_server.shutdown()

    if(cluster):
        cluster.close()

    ###############

    #save results
    print("Save loop")

    for transformedImage in images:
        transformedImage.writeImage()

    # for job in jobs:
    #     if(job.status != dispy.DispyJob.Finished):
    #         print('job %s failed: %s' % (job.id, job.exception))
    #     else:
    #         print('job %s finished: %s' % (job.id, job.result))
    #
    #         job.result.save( ("../output/output%d.jpg" % job.id), "JPEG")




###############################
if __name__ == "__main__":
    main(sys.argv)
