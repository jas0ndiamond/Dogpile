import unittest

from threading import Thread

from MongoDBJobManager import MongoDBJobManager
from ChessBoard import ChessBoard

from pymongo import MongoClient

#connect db
DB_NAME = "dispy_work"
DB_USER = "dispy"
DB_PASS = "dispy_pass"
DB_HOST = "jupiter"
DB_PORT = 27017
DB_TEST_COLLECTION = "MongoDBJobManagerTest"

class TestMongoDBJobManager(unittest.TestCase):
    
    def setUp(self):
        
        self.mgr = None
        
        self.mgr = MongoDBJobManager(DB_HOST, DB_PORT, DB_USER, DB_PASS, DB_NAME, DB_TEST_COLLECTION, ChessBoard.serializeFromDict)
        
        # ensure we have an empty collection
        self.mgr.clearJobs()
        self.assertEqual( 0, self.mgr.getJobCount() )
                
    def tearDown(self):
        if(self.mgr is not None):
            #print("Closing manager")
            self.mgr.close()
    
    def test_add_and_retrieve_jobs(self):
        
        
        # add some test job objects
        self.mgr.addJob( ChessBoard(2,2) )
        self.mgr.addJob( ChessBoard(3,3) )
        self.mgr.addJob( ChessBoard(4,4) )
        self.mgr.addJob( ChessBoard(5,5) )
        self.mgr.addJob( ChessBoard(6,6) )
        self.mgr.addJob( ChessBoard(7,7) )
        self.mgr.addJob( ChessBoard(8,8) )

        self.assertEqual( 7, self.mgr.getJobCount() )
        
        firstJob = self.mgr.getJobs({}, 1)
        
        self.assertEqual( 2, firstJob[0].getXDim() )
        self.assertEqual( 2, firstJob[0].getYDim() )

        nextJobs = self.mgr.getJobs({}, 2)
        
        self.assertEqual( 2, len(nextJobs) )
        
        self.assertEqual( 3, nextJobs[0].getXDim() )
        self.assertEqual( 3, nextJobs[0].getYDim() )
        self.assertEqual( 4, nextJobs[1].getXDim() )
        self.assertEqual( 4, nextJobs[1].getYDim() )

        nextJobs.clear()
        self.assertEqual( 0, len(nextJobs) )
        
        nextJobs = self.mgr.getJobs({}, 20)
        self.assertEqual( 4, len(nextJobs) )
        
        self.assertEqual( 0, self.mgr.getJobCount() )
        
    def test_add_duplicate_jobs(self):
        
        # add indentical board states
        
        # adding n identical board states should result in only 1 new job
        self.mgr.addJob( ChessBoard(6,6) )
        self.mgr.addJob( ChessBoard(6,6) )
        self.mgr.addJob( ChessBoard(6,6) )
        self.mgr.addJob( ChessBoard(6,6) )
        self.mgr.addJob( ChessBoard(6,6) )
        self.mgr.addJob( ChessBoard(6,6) )
        
        self.assertEqual( 1, self.mgr.getJobCount() )
        
        #same board different turncount
        self.mgr.addJob( ChessBoard(6,6,2) )
        self.assertEqual( 2, self.mgr.getJobCount() )
        
    def test_job_retrieval_contention(self):
        
        # add some different jobs 
        # confirm job list size
        # start threads that call getJobs and store results seperately
        # start all threads
        # check that all result sets do not have any shared job hashcodes
        
        def task1():
            global results1
            results1 = self.mgr.getJobs({}, 3)
        
        def task2():
            global results2
            results2 = self.mgr.getJobs({}, 3)

        def task3():
            global results3
            results3 = self.mgr.getJobs({}, 3)
        
        def task4():
            global results4
            results4 = self.mgr.getJobs({}, 3)
        
        def task5():
            global results5
            results5 = self.mgr.getJobs({}, 3)
        
        # 15 jobs, 5 threads each requesting 3 jobs 
        self.mgr.addJob( ChessBoard(1,1) )
        self.mgr.addJob( ChessBoard(2,2) )
        self.mgr.addJob( ChessBoard(3,3) )
        self.mgr.addJob( ChessBoard(4,4) )
        self.mgr.addJob( ChessBoard(5,5) )
        self.mgr.addJob( ChessBoard(6,6) )
        self.mgr.addJob( ChessBoard(7,7) )
        self.mgr.addJob( ChessBoard(8,8) )
        self.mgr.addJob( ChessBoard(9,9) )
        self.mgr.addJob( ChessBoard(10,10) )
        self.mgr.addJob( ChessBoard(11,11) )
        self.mgr.addJob( ChessBoard(12,12) )
        self.mgr.addJob( ChessBoard(13,13) )
        self.mgr.addJob( ChessBoard(14,14) )
        self.mgr.addJob( ChessBoard(15,15) )
        
        self.assertEqual( 15, self.mgr.getJobCount() )
        
        thread1 = Thread(target=task1)
        thread2 = Thread(target=task2)
        thread3 = Thread(target=task3)
        thread4 = Thread(target=task4)
        thread5 = Thread(target=task5)
        
        threads = [thread1, thread2, thread3, thread4, thread5]
        
        #start the contention threads
        for t in threads:
            t.start()
            
        #wait for all to finish
        for t in threads:
            t.join()

        #inspect the results
        
        #no remaining jobs
        self.assertEqual( 0, self.mgr.getJobCount() )
        
        #each results list has 3 elements
        self.assertEqual( 3, len(results1) )
        self.assertEqual( 3, len(results2) )
        self.assertEqual( 3, len(results3) )
        self.assertEqual( 3, len(results4) )
        self.assertEqual( 3, len(results5) )
        
        #15 unique job hashcodes
        resultset = set()
        
        for j in results1:
            resultset.add(j.getHashCode())
            
        for j in results2:
            resultset.add(j.getHashCode())
            
        for j in results3:
            resultset.add(j.getHashCode())
            
        for j in results4:
            resultset.add(j.getHashCode())
            
        for j in results5:
            resultset.add(j.getHashCode())
            
        self.assertEqual( 15, len(resultset) )

if __name__ == '__main__':
    
    results1 = []
    results2 = []
    results3 = []
    results4 = []
    results5 = []
    
    unittest.main()
