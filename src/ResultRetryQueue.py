import queue
import time
import logging

from threading import Thread

class ResultRetryQueue:
    def __init__(self, retryCallback):
       
        #set the log level explicitly. effective log level may not be available
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel( logging.INFO  )
        
        self.logger.info("ResultRetryQueue initializing")
        
        #TODO: remove
        print("ResultRetryQueue constructor")
        
        self.retryCallback = retryCallback

        self.retryQueue = queue.Queue()
        self.failedQueue = queue.Queue()
        
        self.retryQueueSleep = 1

        self.runThread = None
        
        self.logger.info("ResultRetryQueue initialized")

    def setRetrySleep(self, sleepTime):
        self.retryQueueSleep = sleepTime

    def start(self):
        self.logger.info("ResultRetryQueue starting")
        
        self.running = True
        self.runThread = Thread(target=self._runRetries)
        self.runThread.start()

    def stop(self):
        self.logger.info("ResultRetryQueue stopping")
        
        self.running = False;

        #TODO: inventory pending retries

    def addJob(self, job):

        self.logger.debug("Adding new job to retry queue.")

        self.retryQueue.put(job)

        self.logger.debug("Adding new job %d to retry queue. current size: %d" %  (job.id, self.retryQueue.qsize())  )

    def finalRetry(self):
        #last ditch attempt to clear the retry queue
        
        maxAttempts = 10
        i = 0
        while( self.retryQueue.empty() == False and i < maxAttempts):
            self.logger.debug("Waiting on retry jobs: %i, attempt %d" % self.retryQueue.qsize(), i )

            time.sleep(self.retryQueueSleep)
            i += 1

        if(i == maxAttempts):
            self.logger.warning("Result Retry Queue couldn't clear pending results. size was: " % self.retryQueue.qsize() )

    def _runRetries(self):
        while(self.running):
            self.logger.debug('Result Retry cycle starting.' )
            
            #run the retryCallback on everything in the retry queue
            while(self.retryQueue.qsize() > 0):
                self.logger.debug("Entering retry cycle with queue size: %d" %  self.retryQueue.qsize() )

                #retry each item in the queue once
                retryJob = self.retryQueue.get()

                #returns true if the retry was successful or false if not successful
                if(self.retryCallback(retryJob)):
                    self.logger.info("Retry job result write was successful for %s: %s" % (retryJob.id, retryJob.result))
                else:
                    self.logger.warning("Retry job result write was NOT successful for %s: %s" % (retryJob.id, retryJob.result))

                    self.failedQueue.put(retryJob)

            #reload the retry queue with any results that failed to be processed by writeNodeResult
            while(self.failedQueue.empty() == False):
                self.logger.warning("Emptying retry failed queue: %d" % self.failedQueue.qsize() )
                self.retryQueue.put(self.failedQueue.get())
            
            self.logger.debug('Result Retry cycle finished.' )

            time.sleep(self.retryQueueSleep)

        #attempt to clear the retry queue
        self.finalRetry()

        self.logger.info('Retry Queue thread exiting' )
