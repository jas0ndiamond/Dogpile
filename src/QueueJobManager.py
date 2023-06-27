import logging
import queue
from JobManager import JobManager

logFile = "run.log"

class QueueJobManager(JobManager):
    
    def __init__(self):
        logging.basicConfig(filename=logFile, format='%(asctime)s [%(levelname)s] -- [%(name)s]-[%(funcName)s]: %(message)s')
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel( logging.DEBUG )
        
        self.job_queue = queue.Queue()
        
    def addJob(self, job):
        self.job_queue.put(job, True)
        
    def addJobs(self, jobs):
        for job in jobs:
            self.job_queue.put(job, True)
        
    def getJobs(self, count=10000):
        jobs = []
        i=0
        
        while (self.job_queue.empty() == False and i < count):
            jobs.append(self.job_queue.get(True))
            i+=1
            
        self.logger.info("Retrieving %d new jobs" % i)
        
        return jobs
        
    def getJobCount(self):
        return self.job_queue.qsize()
        
    def clearJobs(self):
        self.job_queue.clear()
        
    def close(self):
        self.logger.info("QueueJobManager closing")
        self.job_queue.queue.clear()
