import logging

class ClusterJobResult:
    def __init__(self):
        
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel( logging.getLogger().getEffectiveLevel() )
        
        self.jobIds = {}
        
        self.resultData = {}
        
        pass
        
    #store the result of the cluster work
    def writeResult(self, job_id, data):
        raise Exception("ClusterJobResult.writeResult() not implemented in subclass")

    def hasJobId(self, job_id):

        return (self.jobIds.get(job_id, None) != None)

    def toString(self):
        raise Exception("ClusterJobResult.toString() not implemented in subclass")
