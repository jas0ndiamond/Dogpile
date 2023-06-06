import threading

# basic implementation of counting cluster job completions
class JobCompletionCounter:
    
    def __init__(self):
        self._completedJobLock = threading.Lock()
        self._failedJobLock = threading.Lock()
        
        self.reset()

    def reset(self):
        with self._completedJobLock:
            self.completedJobCount = 0
            
        with self._failedJobLock:
            self.failedJobCount = 0

    def signalCompletedJob(self):
        with self._completedJobLock:
            self.completedJobCount += 1
        
    def signalFailedJob(self):
        with self._failedJobLock:
            self.failedJobCount += 1

    def getCompletedJobCount(self):
        
        retval = None
        
        with self._completedJobLock:
            retval = self.completedJobCount
        
        return retval
        
    def getFailedJobCount(self):
        
        retval = None
        
        with self._failedJobLock:
            retval = self.failedJobCount
        
        return retval
        
    def getTotalCompletedJobCount(self):
        return self.getCompletedJobCount() + self.getFailedJobCount()
