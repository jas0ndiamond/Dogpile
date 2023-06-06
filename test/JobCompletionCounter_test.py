from JobCompletionCounter import JobCompletionCounter

import unittest

class JobCompletionCounter_tests(unittest.TestCase):
    def test_completion_zero(self):
        counter = JobCompletionCounter()
        self.assertEqual(counter.getCompletedJobCount(), 0)
        
    def test_failed_zero(self):
        counter = JobCompletionCounter()
        self.assertEqual(counter.getFailedJobCount(), 0)
        
    def test_increment(self):
        counter = JobCompletionCounter()
        
        counter.signalFailedJob()
        counter.signalFailedJob()
        counter.signalFailedJob()
        
        counter.signalCompletedJob()        
        counter.signalCompletedJob()     
        counter.signalCompletedJob()     
        counter.signalCompletedJob()     
        counter.signalCompletedJob()     
        
        self.assertEqual(counter.getFailedJobCount(), 3)
        self.assertEqual(counter.getCompletedJobCount(), 5)
        self.assertEqual(counter.getTotalCompletedJobCount(), 8)
        
        
    
if __name__ == '__main__':
    unittest.main()
