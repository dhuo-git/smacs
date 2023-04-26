'''
unittest using hubtest.py for hub function test, details see hubtest.py
'''
import unittest
import hubtest, pub, sub

class TestHub(unittest.TestCase):

    #def test_pub(self):
    #    for i in range(3):
    #        with self.subTest(i=i):
    #            self.assertTrue(hubtest.pubsub(i))
    def setUp(self):
        self.ins=hubtest.PubSub(pub.CONF, sub.CONF)
    def tearDown(self):
        self.ins.dispose()

    def case_0(self):
        print('\nut.TestHub.case_0: sub/pub loops')
        self.assertTrue(self.ins.pubsub(),'failed')
        print('\n')
    def case_1(self):
        self.ins.case = 1
        print('\nut.TestHub.case_1: pub reconnects')
        self.assertTrue(self.ins.pubsub(),'failed')
        print('\n')

    def case_2(self):
        self.ins.case = 2
        print('\nut.TestHub.case_2: sub reconnects')
        self.assertTrue(self.ins.pubsub(),'failed')
        print('\n')


if __name__ == '__main__':
    print('ut.TestHub.case_0[1,2]')
    unittest.main()
'''
unittest hub using pub and sub
case_0: sub/pub loops
case_1: sub loop, pub reconnects
case_2: pub loop, sub reconnects
'''
