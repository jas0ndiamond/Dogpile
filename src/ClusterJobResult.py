import logging

class ClusterJobResult:
    def __init__(self):
        
        #set the log level explicitly. effective log level may not be available
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel( logging.INFO )
        
        self.jobIds = {}
        
        self.resultData = {}
        
        if(self.logger.isEnabledFor(logging.DEBUG)):
            self.logger.info("ClusterJobResult built")
        
        pass
        
    #store the result of the cluster work
    def writeResult(self, job_id, data):
        raise Exception("ClusterJobResult.writeResult() not implemented in subclass")

    def hasJobId(self, job_id):

        return (self.jobIds.get(job_id, None) != None)

    def toString(self):
        raise Exception("ClusterJobResult.toString() not implemented in subclass")
