class JobManager:
    def __init__(self):
        pass
        
    def addJob(self, job):
        raise Exception("JobManager.addJob() not implemented in subclass")
        
    def addJobs(self, jobs):
        raise Exception("JobManager.addJobs() not implemented in subclass")
        
    def getJobs(self):
        raise Exception("JobManager.getJobs() not implemented in subclass")
        
    def getJobCount(self):
        raise Exception("JobManager.getJobCount() not implemented in subclass")
        
    def clearJobs(self):
        raise Exception("JobManager.clearJobs() not implemented in subclass")

    def close(self):
        raise Exception("JobManager.close() not implemented in subclass")
