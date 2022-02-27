import os
import logging
import time

import dispy
import dispy.httpd

#src/app => src
srcDir = os.path.dirname( os.path.realpath(__file__) )

from Config import Config
from ClusterFactory import ClusterFactory
from ResultRetryQueue import ResultRetryQueue

#timeout after job submission and status report
####################
#doesn't seem to work, need to add timeout in dispy source
#TODO: figure this out
####################
dispy.config.MsgTimeout = 1200
dispy.MsgTimeout = 1200

class DogPileTask:
    
    def __init__(self, confFile):
        
        #logging.basicConfig(format='%(asctime)s [%(levelname)s] -- [%(name)s]-[%(funcName)s]: %(message)s')
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel( logging.getLogger().getEffectiveLevel() )
        
        self.conf = Config(confFile)

        self.conf.dump()

        self.logger.setLevel(self.conf.get_loglevel())
        
        self.logger.info("Building DogPileTask")
        
        self.clusterFactory = ClusterFactory(self.conf)
        
        self.logger.info("Built ClusterFactory with nodes: %s" % self.clusterFactory.getConfig().get_nodes())
        
        self.retryQueue = ResultRetryQueue(retryCallback=self.writeClusterJobResult)
        self.retryQueue.setRetrySleep(5)
        self.retryQueue.start()
        
        self.dispyHttpServer = None
        self.cluster = None
        
        #default callback that just whines about not being overriden
        #self.writeNodeResultCallback = self.defaultWriteNodeResultCallback
        
        #call the subclass that will define the dispy lambda, and the status callback
        #self.buildCluster()
        
        #sleep just in case a host is slow to respond
        #TODO: read sleep time from config
        self.logger.info("Sleeping, buying time for sluggish nodes to report...")
        time.sleep(5)
            
        self.logger.info("DogPileTask construction completed")

#    def setWriteNodeResultCallback(self, callback):
#        self.writeNodeResultCallback = callback

    def startDispyHttpServer(self):
        self.logger.info("Starting up dispy http server")
        self.dispyHttpServer = dispy.httpd.DispyHTTPServer(self.cluster)
    
    def getClusterFactory(self):
        return self.clusterFactory
    
    def writeClusterJobResult(self, job):
        
        #handle the retry queue add here?
        #retry queue will just recall this later
        
        #call subclass function that's required to be overwridden?
        
        #raise Exception("DogPileTask.writeClusterJobResult() not implemented in subclass")
        
        clusterJobResult = self.getClusterJobResultByJobId(job.id)
        retval = False

        if(clusterJobResult != None):
            self.logger.debug("Writing result from job %d to image %s" % (job.id, clusterJobResult.getFile()))
            
            clusterJobResult.writeResult(job.id, job.result)

            #TODO: remove id from result mapping? race condition?

            retval = True
        else:
            self.logger.warning("Could not find image for job id %d" % job.id)
            self.addRetryJob(job)

        return retval
        
        pass
        
    #subclass implements because they own the data structure
    def getClusterJobResultByJobId(self, id):
        raise Exception("DogPileTask.getClusterJobResultByJobId() not implemented in subclass")
        
    def initializeCluster(self):
        #implemented like this in subclass vvvv
        #cluster = factory.buildCluster(Grayscaler.grayscaleImage, super().clusterStatusCallback)
        raise Exception("DogPileTask.buildCluster() not implemented in subclass")

    def start(self):
        raise Exception("DogPileTask.start() not implemented in subclass")
        
    def stop(self):
        self.logger.info("Stopping DogPileTask")
        
        #signal the retry queue to terminate
        if(self.retryQueue):
            self.retryQueue.stop()
        
        #shutdown the dispy http server
        if(self.dispyHttpServer):
            self.dispyHttpServer.shutdown()
        
        #shut down the dispy cluster
        if(self.cluster):
            self.cluster.close()

        self.logger.info("DogPileTask stop completed")
    
    #submit our job to the dispy cluster
    def submitClusterJob(self, job):
        
        if(self.cluster == None):
            raise Exception("Dispy cluster not initialized. Cannot submit job.")
        
        return self.cluster.submit(job)
    
    #called by subclass once a cluster is built with node function and status callback
    def _setCluster(self, cluster):
        self.cluster = cluster
        
    #def _clusterStatusCallbackLog(message):
    #    #self.logger.debug(message)
    #    pass
        
    def addRetryJob(self, job):
        self.retryQueue.addJob(job)
    
    #def defaultWriteNodeResultCallback(job):
    #    raise Exception("writeNodeResultCallback not set by subclass")
    
    def waitForCompletion(self, interval):
        self.quit = False
        while( self.quit != True and self.cluster.wait(interval) != True ):
            
            print("waiting on something?")
            
            self.cluster.print_status()
 
