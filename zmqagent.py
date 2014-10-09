import zmq
import pickle
from socket import gethostname
import threading
import time
import uuid

class AgentRef(object) :
    # TODO : add url scheme (tcp, inproc .. at least)
    def __init__(self, host, port) :
        self._host = host
        self._port = port


    def url(self) :
        return "tcp://%s:%d"%(self._host, self._port)

    def __hash__(self) :
        return hash(self.url())

class ZAgent(object) :

    def __init__(self, listen_port, ctxt = None) :

        if ctxt == None :
            ctxt = zmq.Context()
        self._zctxt = ctxt
        self._timeout = None
        self.host = gethostname()
        self.port = listen_port
        self._connections = {}
        self._started = False
        self._kill_socket_address = "inproc://killsocket%d%s"%(self.port, self.host)

    def _connect(self, agentRef) :
        """ connects to another agent"""
        sock = self._zctxt.socket(zmq.PUSH)
        sock.connect(agentRef.url())
        self._connections[agentRef] = sock

    def push(self, agentRef, msg) :
        """ push a message to an agent """
        if not agentRef in self._connections :
            self._connect(agentRef)

        self._connections[agentRef].send(pickle.dumps(((self.host, self.port),msg)))
        
    def broadcast(self, msg) :
        """ broadcast the message to all connected agents """
        for s in self._connections :
            s.send(pickle.dumps(msg))

    def start(self, starts_in_thread=False) :

        if starts_in_thread :
            self._started = True
            self._thread = threading.Thread(target = self.listen_loop).start()
            self._kill_socket = self._zctxt.socket(zmq.PAIR)
            self._kill_socket.connect(self._kill_socket_address)
            print "started", self.ref().url()
        else :
            self.listen_loop()

        
    def kill(self) :

        if not self._started :
            return 
        
        self._started = False
        self._kill_socket.send('k')
        # if hasattr(self, '_thread') :
        #     print waiting for join
        #     self._thread.join()
    
        self._kill_socket.close()
        
    def reset_timeout(self, timeout_ms) :
        """ resets the timeout, will call handle_timeout in timeout_ms (approximately..) 
            unless reset_timeout() has been called before """
        self._timeout = time.time() + timeout_ms * .001

    def listen_loop(self) :

        self._listening = self._zctxt.socket(zmq.PULL)
        url_ = "tcp://*:%d"%(self.port)
        self._listening.bind(url_)
        self._kill_socket_listen = self._zctxt.socket(zmq.PAIR)
        self._kill_socket_listen.bind(self._kill_socket_address)

        poller = zmq.Poller()
        poller.register(self._listening, zmq.POLLIN)
        poller.register(self._kill_socket_listen, zmq.POLLIN)
        
        while True :

            timeout = None 

            if(self._timeout != None ) :
                timeout_val_ms = (self._timeout - time.time()) * 1000
                if(timeout_val_ms < 0) :
                    self._timeout = None
                    self.handle_timeout() 
                    continue

                timeout = timeout_val_ms
            print "pollin'", timeout
            evts =  dict(poller.poll(timeout))
            print evts
            if self._listening in evts :
                sender, msg = pickle.loads(self._listening.recv())
                self.handle_msg(AgentRef(*sender), msg)

            if self._kill_socket_listen in evts :
                print self._kill_socket_listen.recv()
                break
                
            else :
                self._timeout = None
                self.handle_timeout()

        
        self._listening.close()
        self._connections = {}  
        self._kill_socket_listen.close()


    def handle_msg(self, sender, msg) :
        pass


    def handle_timeout(self) :
        pass

    def ref(self) :
        return AgentRef(self.host, self.port)

class PongAgent(ZAgent) :

    # def __init__(self, port, ctxt):
    #     super(PongAgent, self).__init__(port, ctxt)
    
    def handle_msg(self, sender, msg) :
        print msg
        time.sleep(0.5)
        self.push(sender, {"pong":msg["pong"] + 1})
            

class BullyAgent(ZAgent) :
    """ leader election agls."""

    def __init__(self, port, ctxt=None, id = None) :
        super(PongAgent, self).__init__(port, ctxt)
        if id == None :
            id = uuid.uuid4()
        self._id = id
        self.coordinator = None
        self.best_seen_id = None

     
    def handle_msg(self, sender, msg):
        if msg[0] == "ELECT" :
            self.best_seen_id = None
            self.push(sender, ("ANSWER", self._id))
            
        if msg[0] == "ANSWER" :
            sender_id = msg[1]
            if(self._id > sender_id ) :
                self.best_seen_id = (sender, sender_id)
                self.reset_timeout(200) # call me later please

        if msg[0] == "COORD" :
            sender_id = msg[1]
            if(self._id > sender_id) :
                # someone is trying to usurpate the shit 
                self.broadcast(("ELECT"))
                self.best_seen_id = None

            else :
                self.coordinator = (sender, sender_id)


    def handle_timeout(self) :
        if self.best_seen_id == None :
            self.broadcast(("COORD", self._id))
        

if __name__ == "__main__" :


    ctxt = zmq.Context() # whe are going to use the same ctxt for all agents 

    agent1 = PongAgent(4321, ctxt)
    agent1.start(True)
    
    agent2 = PongAgent(5432, ctxt)

    #boot strap    
    agent2.push(agent1.ref(), {"pong":0})
    agent2.start()
    


    
        
