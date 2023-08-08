import unittest

from threading import Thread

from MongoDBJobManager import MongoDBJobManager
from ChessBoard import ChessBoard

from pymongo import MongoClient

#connect db
DB_NAME = "dogpile_work"
DB_USER = "dogpile"
DB_PASS = "dogpile_pass"
DB_HOST = "mercury"
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

    def test_intakeSubmissionThreshold(self):
        
        #check that jobs are only submitted to the database if the intake queue size is above the threshold
        
        #TODO: implement
        pass

    def test_addJobs(self):
        # get a pile of jobs inserted
        # ensure uniqueness for duplicates
        
        board1 = ChessBoard(8,8)
        board1.setSpace(0, 0, ChessBoard.ROOK)
        board1.setSpace(1, 1, ChessBoard.ROOK)
        board1.setSpace(2, 2, ChessBoard.ROOK)
        board1.setSpace(3, 3, ChessBoard.ROOK)
        board1.setSpace(4, 4, ChessBoard.ROOK)
        
        board2 = ChessBoard(8,8)
        board2.setSpace(0, 0, ChessBoard.KNIGHT)
        board2.setSpace(1, 1, ChessBoard.KNIGHT)
        board2.setSpace(2, 2, ChessBoard.KNIGHT)
        board2.setSpace(3, 3, ChessBoard.KNIGHT)
        board2.setSpace(4, 4, ChessBoard.KNIGHT)
        
        board3 = ChessBoard(8,8)
        board3.setSpace(0, 0, ChessBoard.BISHOP)
        board3.setSpace(1, 1, ChessBoard.BISHOP)
        board3.setSpace(2, 2, ChessBoard.BISHOP)
        board3.setSpace(3, 3, ChessBoard.BISHOP)
        board3.setSpace(4, 4, ChessBoard.BISHOP)
        
        board4 = ChessBoard(8,8)
        board4.setSpace(0, 0, ChessBoard.QUEEN)
        board4.setSpace(1, 1, ChessBoard.QUEEN)
        board4.setSpace(2, 2, ChessBoard.QUEEN)
        board4.setSpace(3, 3, ChessBoard.QUEEN)
        board4.setSpace(4, 4, ChessBoard.QUEEN)
        
        board5 = ChessBoard(8,8)
        board5.setSpace(0, 0, ChessBoard.KING)
        board5.setSpace(1, 1, ChessBoard.KING)
        board5.setSpace(2, 2, ChessBoard.KING)
        board5.setSpace(3, 3, ChessBoard.KING)
        board5.setSpace(4, 4, ChessBoard.KING)        
        
        board6 = ChessBoard(8,8)
        board6.setSpace(7, 7, ChessBoard.ROOK)
        board6.setSpace(6, 6, ChessBoard.ROOK)
        board6.setSpace(5, 5, ChessBoard.ROOK)
        board6.setSpace(4, 4, ChessBoard.ROOK)
        board6.setSpace(5, 5, ChessBoard.ROOK) 
        
        board7 = ChessBoard(8,8)
        board7.setSpace(7, 7, ChessBoard.KNIGHT)
        board7.setSpace(6, 6, ChessBoard.KNIGHT)
        board7.setSpace(5, 5, ChessBoard.KNIGHT)
        board7.setSpace(4, 4, ChessBoard.KNIGHT)
        board7.setSpace(5, 5, ChessBoard.KNIGHT) 
        
        board8 = ChessBoard(8,8)
        board8.setSpace(7, 7, ChessBoard.BISHOP)
        board8.setSpace(6, 6, ChessBoard.BISHOP)
        board8.setSpace(5, 5, ChessBoard.BISHOP)
        board8.setSpace(4, 4, ChessBoard.BISHOP)
        board8.setSpace(5, 5, ChessBoard.BISHOP) 
        
        board9 = ChessBoard(8,8)
        board9.setSpace(7, 7, ChessBoard.QUEEN)
        board9.setSpace(6, 6, ChessBoard.QUEEN)
        board9.setSpace(5, 5, ChessBoard.QUEEN)
        board9.setSpace(4, 4, ChessBoard.QUEEN)
        board9.setSpace(5, 5, ChessBoard.QUEEN) 
        
        board10 = ChessBoard(8,8)
        board10.setSpace(7, 7, ChessBoard.KING)
        board10.setSpace(6, 6, ChessBoard.KING)
        board10.setSpace(5, 5, ChessBoard.KING)
        board10.setSpace(4, 4, ChessBoard.KING)
        board10.setSpace(5, 5, ChessBoard.KING) 
        
        ########
        
        board11 = ChessBoard(8,8)
        board11.setSpace(0, 0, ChessBoard.ROOK)
        board11.setSpace(0, 1, ChessBoard.ROOK)
        board11.setSpace(0, 2, ChessBoard.ROOK)
        board11.setSpace(0, 3, ChessBoard.ROOK)
        board11.setSpace(0, 4, ChessBoard.ROOK)
        
        board12 = ChessBoard(8,8)
        board12.setSpace(0, 0, ChessBoard.KNIGHT)
        board12.setSpace(0, 1, ChessBoard.KNIGHT)
        board12.setSpace(0, 2, ChessBoard.KNIGHT)
        board12.setSpace(0, 3, ChessBoard.KNIGHT)
        board12.setSpace(0, 4, ChessBoard.KNIGHT)
        
        board13 = ChessBoard(8,8)
        board13.setSpace(0, 0, ChessBoard.BISHOP)
        board13.setSpace(0, 1, ChessBoard.BISHOP)
        board13.setSpace(0, 2, ChessBoard.BISHOP)
        board13.setSpace(0, 3, ChessBoard.BISHOP)
        board13.setSpace(0, 4, ChessBoard.BISHOP)
        
        board14 = ChessBoard(8,8)
        board14.setSpace(0, 0, ChessBoard.QUEEN)
        board14.setSpace(0, 1, ChessBoard.QUEEN)
        board14.setSpace(0, 2, ChessBoard.QUEEN)
        board14.setSpace(0, 3, ChessBoard.QUEEN)
        board14.setSpace(0, 4, ChessBoard.QUEEN)
        
        board15 = ChessBoard(8,8)
        board15.setSpace(0, 0, ChessBoard.KING)
        board15.setSpace(0, 1, ChessBoard.KING)
        board15.setSpace(0, 2, ChessBoard.KING)
        board15.setSpace(0, 3, ChessBoard.KING)
        board15.setSpace(0, 4, ChessBoard.KING)
        
        board16 = ChessBoard(8,8)
        board16.setSpace(0, 0, ChessBoard.ROOK)
        board16.setSpace(1, 0, ChessBoard.ROOK)
        board16.setSpace(2, 0, ChessBoard.ROOK)
        board16.setSpace(3, 0, ChessBoard.ROOK)
        board16.setSpace(4, 0, ChessBoard.ROOK)
        
        board17 = ChessBoard(8,8)
        board17.setSpace(0, 0, ChessBoard.KNIGHT)
        board17.setSpace(1, 0, ChessBoard.KNIGHT)
        board17.setSpace(2, 0, ChessBoard.KNIGHT)
        board17.setSpace(3, 0, ChessBoard.KNIGHT)
        board17.setSpace(4, 0, ChessBoard.KNIGHT)
        
        board18 = ChessBoard(8,8)
        board18.setSpace(0, 0, ChessBoard.BISHOP)
        board18.setSpace(1, 0, ChessBoard.BISHOP)
        board18.setSpace(2, 0, ChessBoard.BISHOP)
        board18.setSpace(3, 0, ChessBoard.BISHOP)
        board18.setSpace(4, 0, ChessBoard.BISHOP)
        
        board19 = ChessBoard(8,8)
        board19.setSpace(0, 0, ChessBoard.QUEEN)
        board19.setSpace(1, 0, ChessBoard.QUEEN)
        board19.setSpace(2, 0, ChessBoard.QUEEN)
        board19.setSpace(3, 0, ChessBoard.QUEEN)
        board19.setSpace(4, 0, ChessBoard.QUEEN)
        
        board20 = ChessBoard(8,8)
        board20.setSpace(0, 0, ChessBoard.KING)
        board20.setSpace(1, 0, ChessBoard.KING)
        board20.setSpace(2, 0, ChessBoard.KING)
        board20.setSpace(3, 0, ChessBoard.KING)
        board20.setSpace(4, 0, ChessBoard.KING)
        
        # duplicates of boards 1-8
        board21 = ChessBoard(8,8)
        board21.setBoardStateFromString(board1.getBoardStateStr())
        
        board22 = ChessBoard(8,8)
        board22.setBoardStateFromString(board2.getBoardStateStr())
        
        board23 = ChessBoard(8,8)
        board23.setBoardStateFromString(board3.getBoardStateStr())
        
        board24 = ChessBoard(8,8)
        board24.setBoardStateFromString(board4.getBoardStateStr())
        
        board25 = ChessBoard(8,8)
        board25.setBoardStateFromString(board5.getBoardStateStr())
        
        board26 = ChessBoard(8,8)
        board26.setBoardStateFromString(board6.getBoardStateStr())
        
        board27 = ChessBoard(8,8)
        board27.setBoardStateFromString(board7.getBoardStateStr())
        
        board28 = ChessBoard(8,8)
        board28.setBoardStateFromString(board8.getBoardStateStr())
        
        board29 = ChessBoard(8,8)
        board29.setBoardStateFromString(board9.getBoardStateStr())
        
        board30 = ChessBoard(8,8)
        board30.setBoardStateFromString(board10.getBoardStateStr())

        # sanity check our set of duplicate boards
        self.assertEqual( board1.getHashCode(), board21.getHashCode() )
        self.assertEqual( board2.getHashCode(), board22.getHashCode() )
        self.assertEqual( board3.getHashCode(), board23.getHashCode() )
        self.assertEqual( board4.getHashCode(), board24.getHashCode() )
        self.assertEqual( board5.getHashCode(), board25.getHashCode() )
        self.assertEqual( board6.getHashCode(), board26.getHashCode() )
        self.assertEqual( board7.getHashCode(), board27.getHashCode() )
        self.assertEqual( board8.getHashCode(), board28.getHashCode() )        
        self.assertEqual( board9.getHashCode(), board29.getHashCode() )
        self.assertEqual( board10.getHashCode(), board30.getHashCode() )
        
        #######
        # insert the 20 unique boards
        
        toSubmit1 = [
            board1,
            board2,
            board3,
            board4,
            board5,
            board6,
            board7,
            board8,
            board9,
            board10,
            board11,
            board12,
            board13,
            board14,
            board15,                        
            board16,
            board17,
            board18,
            board19,
            board20   
        ]
        
        self.mgr.addJobs( toSubmit1 )
        
        self.assertEqual( 20, self.mgr.getJobCount() )
        
        # get the jobs from the db, clearing the collection
        newJobs = self.mgr.getJobs({}, 20)
        
        self.assertEqual( 0, self.mgr.getJobCount() )       
        
        # insert the 30 boards with some duplicates, expect only the uniques in result set
        
        toSubmit2 = [
            board1,
            board2,
            board3,
            board4,
            board5,
            board6,
            board7,
            board8,
            board9,
            board10,
            board11,
            board12,
            board13,
            board14,
            board15,                        
            board16,
            board17,
            board18,
            board19,
            board20,  
            board21,
            board22,
            board23,
            board24,
            board25,                        
            board26,
            board27,
            board28,
            board29,
            board30              
        ]
        
        self.mgr.addJobs( toSubmit2 )
        
        self.assertEqual( 20, self.mgr.getJobCount() )
        
        newJobs = self.mgr.getJobs({}, 20)
        
        self.assertEqual( 0, self.mgr.getJobCount() )
        
    def test_has_jobs(self):
        # basic tests around determining if a collection is empty
        
        # add some test job objects
        self.mgr.addJob( ChessBoard(2,2) )
        self.mgr.addJob( ChessBoard(3,3) )
        self.mgr.addJob( ChessBoard(4,4) )
        
        self.mgr.addJob( ChessBoard(5,5) )
        self.mgr.addJob( ChessBoard(6,6) )
        self.mgr.addJob( ChessBoard(7,7) )
        self.mgr.addJob( ChessBoard(8,8) )
        
        self.assertEqual( 7, self.mgr.getJobCount() )
        
        self.assertTrue( self.mgr.hasJobs() )
        
        self.mgr.clearJobs()
        
        self.assertEqual( 0, self.mgr.getJobCount() )
        
        self.assertFalse( self.mgr.hasJobs() )
                                        
    def test_has_jobs2(self):
        
        #test that the hasJobs determination doesn't modify the database
        
        # add some test job objects
        self.mgr.addJob( ChessBoard(2,2) )
        self.mgr.addJob( ChessBoard(3,3) )
        self.mgr.addJob( ChessBoard(4,4) )
        
        self.mgr.addJob( ChessBoard(5,5) )
        self.mgr.addJob( ChessBoard(6,6) )
        self.mgr.addJob( ChessBoard(7,7) )
        self.mgr.addJob( ChessBoard(8,8) )
        
        self.assertEqual( 7, self.mgr.getJobCount() )
        
        self.assertTrue( self.mgr.hasJobs() )
        
        self.assertEqual( 7, self.mgr.getJobCount() )
        
        self.assertTrue( self.mgr.hasJobs() )
        self.assertTrue( self.mgr.hasJobs() )
        self.assertTrue( self.mgr.hasJobs() )
        self.assertTrue( self.mgr.hasJobs() )
        self.assertTrue( self.mgr.hasJobs() )
        self.assertTrue( self.mgr.hasJobs() )
        self.assertTrue( self.mgr.hasJobs() )
        self.assertTrue( self.mgr.hasJobs() )
        self.assertTrue( self.mgr.hasJobs() )
        self.assertTrue( self.mgr.hasJobs() )
        
        self.assertEqual( 7, self.mgr.getJobCount() )
                                        
if __name__ == '__main__':
    
    results1 = []
    results2 = []
    results3 = []
    results4 = []
    results5 = []
    
    unittest.main()
