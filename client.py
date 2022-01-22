import sys
import json
import io
import types
import struct
import socket
import hashlib
import selectors
import traceback
import logging
import time
import re
from queue import PriorityQueue
from threading import Thread, Lock, Event

class PeerServer:

    def __init__(self, client_info, client_id, event, lamport_clock, lock):
        self.event = event
        self.client_info = client_info
        self.client_id = client_id
        self.lamport_clock = lamport_clock
        self.sel = selectors.DefaultSelector()
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((client_info[client_id]["host"], client_info[client_id]["port"]))
        self.sock.listen()
        self.sock.setblocking(False)
        self.sel.register(self.sock, selectors.EVENT_READ, data=None)
        self.reply_count = 0
        self.lock = lock

        while True:
            events = self.sel.select(timeout=None)
            for key, mask in events:
                if key.data is None:
                    self.accept_wrapper(key.fileobj)
                else:
                    self.service_connection(key, mask, client_info, client_id)            
                    if self.reply_count == len(client_info) - 1:
                        if request_queue.queue[0][1] == client_id:
                            event.set()
                            self.reply_count = 0

    def accept_wrapper(self, sock):
        conn, addr = sock.accept()
        logging.debug('accepted connection from', addr)
        conn.setblocking(False)
        data = types.SimpleNamespace(addr=addr, inb=b'', outb=b'')
        events = selectors.EVENT_READ | selectors.EVENT_WRITE
        self.sel.register(conn, events, data=data)
    
    def service_connection(self, key, mask, client_info, client_id):
        sock = key.fileobj
        data = key.data
        host, port = sock.getpeername()
        if mask & selectors.EVENT_READ:
            recv_data = sock.recv(1024).decode()
            if recv_data:
                req_list = self.get_request_list(recv_data)
                for req in req_list:
                    self.process_message(req, client_id, client_info)

            else:
                print('\nCLOSING CON: ', data.addr)
                self.sel.unregister(sock)
                sock.close()
    
    def get_request_list(self, data):
        start_ids = [m.start() for m in re.finditer('{"type"', data)]
        req_list = list()
        for i in range(1, len(start_ids)):
            req_list.append(data[start_ids[i-1]:start_ids[i]])
        req_list.append(data[start_ids[len(start_ids)-1]:])
        return req_list


    def process_message(self, message, client_id, client_info):

        req = json.loads(message)
        if req["type"] == "REQ":

            print("\nRECV REQUEST from CLIENT ", req["client_id"],  str(req))
            update_request_queue((req["clock_val"], req["client_id"]))
            update_lamport_clock(req["clock_val"], "Recv REQUEST", self.lock)

            update_lamport_clock(0, "Send REPLY", self.lock)
            resp = {"type": "REP", "client_id": client_id, "clock_val": lamport_clock[0]}
            
            peer_conn = client_info[req["client_id"]]["conn"]
            time.sleep(2)
            peer_conn.sendall(json.dumps(resp).encode())
            print("\nSEND REPLY to CLIENT ", req["client_id"],  str(resp))
        elif req["type"] == "REP":
            print("\nRECV REPLY from CLIENT ", req["client_id"],  str(req))
            update_lamport_clock(req["clock_val"], "Recv REPLY", self.lock)
            self.reply_count += 1
        elif req["type"] == "REL":
            print("\nRECV RELEASE from CLIENT ", req["client_id"],  str(req))
            remove_client_from_queue(req["client_id"])
            update_lamport_clock(req["clock_val"], "Recv RELEASE", self.lock)

def remove_client_from_queue(id):
    global request_queue
    temp = PriorityQueue()
    while not request_queue.empty():
        curr_clock_val, curr_id = request_queue.get()
        if curr_id == id:
            continue
        temp.put((curr_clock_val, curr_id))
    request_queue = temp
    print("\nREQ QUEUE UPDATE : ", request_queue.queue)

def update_request_queue(new_req):  
    request_queue.put(new_req)
    print("\nREQ QUEUE UPDATE : ", request_queue.queue)

def update_lamport_clock(new_clock, message, lock):
    lock.acquire()
    lamport_clock[0] = max(lamport_clock[0], new_clock) + 1
    lock.release()
    print("\nCLOCK UPDATE ", message, " value ->", lamport_clock)

def get_client_id():
    if len(sys.argv) != 2:
        print("Script Usage: python client.py <client_id>")
        exit() 
    else:
        return sys.argv[1]

def get_config_data():
    config_fd = open('config.json')
    return json.load(config_fd)

def display_wait_message():
    print("\n\nInitalizing Menu Please Wait", end =" ")
    for i in range(5):
        print('.', end =" ")
        time.sleep(1)
    print("Menu Ready!")

def start_server(client_info, client_id, event, lamport_clock, lock):
    peerserver = PeerServer(client_info, client_id, event, lamport_clock, lock)

def connect_to_peer_clients():

    for peer_client_id, peer_client_info in client_info.items():
        if peer_client_id == client_id:
            continue
        peer_server_addr = (peer_client_info["host"], peer_client_info["port"])
        peer_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        print("\nCONNECTED TO: ", peer_client_id)
        peer_sock.connect_ex(peer_server_addr)
        peer_client_info["conn"] = peer_sock

def send_request_to_server(req):

    update_lamport_clock(0, "SEND MSG", lock)

    conn = client_info[client_id]["conn"]
    message = json.dumps(req).encode()

    print("\n******* SEND MSG TO BLOCKCHAIN SERVER ", str(req), "*******")
    time.sleep(2)
    conn.sendall(message)

    resp = conn.recv(1024).decode()
    print("\n******* RECV MSG FROM BLOCKCHAIN SERVER ", str(resp), "*******")

    update_lamport_clock(0, "RECV MSG", lock)

    return json.loads(resp)

def lock_resource():

    update_lamport_clock(0, "Send REQUEST", lock)
    update_request_queue((lamport_clock[0], client_id))
    req = {"type": "REQ", "client_id": client_id, "clock_val": lamport_clock[0]}

    for peer_client_id, peer_client_info in client_info.items():
        if peer_client_id == client_id:
            continue
        conn = peer_client_info["conn"]
        message = json.dumps(req).encode()
        time.sleep(2)
        conn.sendall(message)
        print("\nSEND REQUEST TO CLIENT", str(peer_client_id), str(req))

    event.wait()

def release_resource():

    update_lamport_clock(0, "Send RELEASE", lock)
    req = {"type": "REL", "client_id": client_id, "clock_val": lamport_clock[0]}

    for peer_client_id, peer_client_info in client_info.items():
        if peer_client_id == client_id:
            continue
        conn = peer_client_info["conn"]
        message = json.dumps(req).encode()
        time.sleep(2)
        conn.sendall(message)
        print("\nSEND RELEASE to ", str(peer_client_id), str(req))
    remove_client_from_queue(client_id)
    event.clear()

def get_balance(display=False):

    req = {"method": "get_balance", "client_id": client_id}
    resp = send_request_to_server(req)
    balance = resp["balance"]
    if display:
        print("\n\n****** YOUR BALANCE IS $ ", balance, "******\n\n")
    else:
        return balance

def get_transaction_details():

    reciever = int(input("Enter the ID of reciever: "))
    if reciever == client_id or reciever not in client_info:
        print("Invalid reciever")
        return None
    
    amount = int(input("Enter the amount: "))
    if amount < 0:
        print("Invalid amount")
        return None

    return reciever, amount
    
def check_transaction(transaction_details):
    reciever, amount = transaction_details

    req = {"method": "check_transaction", "sender": client_id, "reciever": reciever, "amount": amount}
    resp = send_request_to_server(req)
    if "error" in resp:
        print(resp["error"])
        return 0
    
    add_transaction(reciever, amount, resp["prev_block"])
    

def add_transaction(reciever, amount, prev_block):

    hash_input = ":" if prev_block is None else ":".join([str(val) for key, val in prev_block.items()])
    hash_output = hashlib.sha256(hash_input.encode()).hexdigest()
    block = {"hash": hash_output, "sender": client_id, "reciever": reciever, "amount": amount}
    
    req = {"method": "add_transaction", "block": block}
    resp = send_request_to_server(req)
    if resp["status"] is not None:
        print(resp["status"])


#global state
lock = Lock()
event = Event()
client_info = dict()
request_queue = PriorityQueue()

config_data = get_config_data()
client_id = int(get_client_id())
lamport_clock = [0, client_id]

server_host = config_data["server_host"]
server_port = config_data["server_port"]

for data in config_data["client_data"]:
    i, host, port = data["id"], data["host"], data["port"]
    client_info[i] = {"host": host, "port": port}

if client_id not in client_info:
    print("Invalid Client ID!")
    exit(1)
#start peer
server_thread = Thread(target=start_server, args=(client_info, client_id, event, lamport_clock, lock))
server_thread.setDaemon(True)
server_thread.start()

#connect to the block chain server
client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client_sock.connect((server_host, server_port))
client_info[client_id]["conn"] = client_sock

#wait for peers to be ready
display_wait_message()

#connect to peers
connect_to_peer_clients()

while True:

    user_input = input("\n\n****** WELCOME TO BLOCKCHAIN BANK MENU! ******\n\nPRESS 1 TO CHECK YOUR BALANCE!\nPRESS 2 TO SEND MONEY\nPRESS 0 TO EXIT!\n\nEnter your choice: ")

    if user_input == "1":
        lock_resource()
        get_balance()
        release_resource()
    elif user_input == "2":
        transaction_details = get_transaction_details()
        if transaction_details is None:
            continue
        lock_resource() 
        check_transaction(transaction_details)
        release_resource()
    elif user_input == "0":
        logging.debug("TERMINATING THE CLIENT!")
        break
    else:
        logging.debug("INVALID INPUT!")