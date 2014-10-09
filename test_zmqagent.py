import unittest
import time
from zmqagent import ZAgent
import zmq

class TestAgent(ZAgent) :
    
    def handle_msg(self, sender, msg) :

        if(not hasattr(self, 'recvd')):
            self.recvd = []

        self.recvd.append((sender, msg))

class TestAgentWithTimeout(ZAgent) :

    def handle_timeout(self) :
        if( not hasattr(self, 'timed')):
            self.timed = []
        self.timed.append('*')
        self.reset_timeout(100)


class TestZAgent(unittest.TestCase) :

    def setUp(self) :
        self.agent1 = TestAgent(3211, zmq.Context.instance())
        self.agent2 = TestAgentWithTimeout(3212, zmq.Context.instance())


    def tearDown(self) :
        self.agent1.kill()
        self.agent2.kill()



    def testInputQueue(self) :
        self.agent1.start(True)
        self.agent2.push(self.agent1.ref(), "pong")
        time.sleep(0.2)
        self.agent1.kill()

        self.assertEqual(len(self.agent1.recvd), 1)
        self.assertEqual(self.agent1.recvd[0][1], "pong")
        self.assertEqual(self.agent1.recvd[0][0].url(), self.agent2.ref().url())


    def testTimeoutBehaviour(self) :
        self.agent2.reset_timeout(0.1)
        self.agent2.start(True)
        time.sleep(1.)
        self.assertIn(len(self.agent2.timed), range(8,12))
        

if __name__ == "__main__" :
    unittest.main()
