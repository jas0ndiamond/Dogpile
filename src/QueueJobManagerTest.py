import unittest

from QueueJobManager import QueueJobManager
from ChessBoard import ChessBoard

class TestQueueJobManager(unittest.TestCase):
    def test_add_and_retrieve_jobs(self):
        mgr = QueueJobManager()
        
        # add some test job objects
        mgr.addJob( ChessBoard(2,2) )
        mgr.addJob( ChessBoard(3,3) )
        mgr.addJob( ChessBoard(4,4) )
        mgr.addJob( ChessBoard(5,5) )
        mgr.addJob( ChessBoard(6,6) )
        mgr.addJob( ChessBoard(7,7) )
        mgr.addJob( ChessBoard(8,8) )

        self.assertEqual( 7, mgr.getJobCount() )
        
        firstJob = mgr.getJobs(1)
        
        self.assertEqual( 2, firstJob[0].getXDim() )
        self.assertEqual( 2, firstJob[0].getYDim() )

        nextJobs = mgr.getJobs(2)
        
        self.assertEqual( 2, len(nextJobs) )
        
        self.assertEqual( 3, nextJobs[0].getXDim() )
        self.assertEqual( 3, nextJobs[0].getYDim() )
        self.assertEqual( 4, nextJobs[1].getXDim() )
        self.assertEqual( 4, nextJobs[1].getYDim() )

        nextJobs.clear()
        self.assertEqual( 0, len(nextJobs) )
        
        nextJobs = mgr.getJobs(20)
        self.assertEqual( 4, len(nextJobs) )
        
        self.assertEqual( 0, mgr.getJobCount() )

if __name__ == '__main__':
    unittest.main()
