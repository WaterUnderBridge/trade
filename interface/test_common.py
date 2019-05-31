#encoding=utf8
import unittest
from common import *

class TestMathFunc(unittest.TestCase):
    """Test mathfuc.py"""
    def setUp(self):
        print "do something before test.Prepare environment."

    def tearDown(self):
        print "do something after test.Clean up."
    def test_add(self):
        """Test method add(a, b)"""
        self.assertEqual(3, add(1, 2))
        self.assertNotEqual(3, add(2, 2))
        self.assertEqual(4, add(1, 2))
    @classmethod
    def setUpClass(cls):
        print "This setUpClass() method only called once."

    @classmethod
    def tearDownClass(cls):
        print "This tearDownClass() method only called once too."
'''
class TestOther(unittest.TestCase):
	def testMulti(self):
		self.assertEqual(6, multi(2,3))
		self.assertEqual(8, multi(2,3))
'''

if __name__ == "__main__":
    #unittest.main()
    suite = unittest.TestSuite()
    tests = [TestMathFunc("test_add")]
    suite.addTests(tests)
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite)