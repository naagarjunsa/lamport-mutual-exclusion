import sys
import json
import io
import types
import struct
import socket
import selectors
import traceback
import logging
import time
from threading import Thread, Lock

class BlockChain:
    
    INIT_BALANCE = 10

    def __init__(self):
        self.blocks = list()  

    def get_balance(self, client_id):
        
        balance = BlockChain.INIT_BALANCE

        for block in self.blocks:
            sender = block["sender"]
            reciever = block["reciever"]
            amount = block["amount"]

            if sender == client_id:
                balance -= amount
            elif reciever == client_id:
                balance += amount
        return balance

    def add_transaction(self, block):
        self.blocks.append(block)
    
    def check_transaction(self, sender, reciever, amount):
        curr_balance = self.get_balance(sender)
        if amount > curr_balance:
            return -1
        else:
            return self.blocks[-1] if len(self.blocks) else None

class BlockchainServer:

    def __init__(self, host, port, lock):
        self.sel = selectors.DefaultSelector()
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((host, port))
        print("\nBLOCKCHAIN SERVER STARTING -> " + str(host) + ":" + str(port))
        self.sock.listen()
        self.sock.setblocking(False)
        self.sel.register(self.sock, selectors.EVENT_READ, data=None)

        while True:
            events = self.sel.select(timeout=None)
            for key, mask in events:
                if key.data is None:
                    self.accept_wrapper(key.fileobj)
                else:
                    lock.acquire()
                    self.service_connection(key, mask)
                    lock.release()

    def accept_wrapper(self, sock):
        conn, addr = sock.accept()
        print('\nCONN FROM: ', addr)
        conn.setblocking(False)
        data = types.SimpleNamespace(addr=addr, inb=b'', outb=b'')
        events = selectors.EVENT_READ | selectors.EVENT_WRITE
        self.sel.register(conn, events, data=data)

    def service_connection(self, key, mask):
        sock, data = key.fileobj, key.data
        host, port = sock.getpeername()

        if mask & selectors.EVENT_READ:
            recv_data = sock.recv(1024).decode()
            if recv_data:
                print("\nRECV MSG FROM " + str(host) + ":" + str(port) + " -> " + str(recv_data))
                resp = self.process_message(recv_data)
                sock.sendall(json.dumps(resp).encode())
                print("\nSEND MSG TO " + str(host) + ":" + str(port) + " -> " + str(resp))
            else:
                print("\nCONN TERMINATED: " + str(host) + ":" + str(port))
                self.sel.unregister(sock)
                sock.close()
    
    def process_message(self, message):
        req = json.loads(message)
        if req["method"] == "get_balance":
            return self.process_get_balance(req)
        elif req["method"] == "check_transaction":
            return self.process_check_transaction(req)
        elif req["method"] == "add_transaction":
            return self.process_add_transaction(req)

    def process_check_transaction(self, req):
        global blockchain
        sender, reciever, amount = req["sender"], req["reciever"], req["amount"]
        prev_block = blockchain.check_transaction(sender, reciever, amount)
        if prev_block == -1:
            return {"error" : "INCORRECT"}
        else:
            return {"prev_block" : prev_block}

    def process_get_balance(self, req):
        global blockchain
        client_id = req["client_id"]
        balance = blockchain.get_balance(client_id)
        return {"balance" : balance}
    
    def process_add_transaction(self, req):
        global blockchain
        block = req["block"]
        blockchain.add_transaction(block)
        return {"status" : "SUCCESS"}
    

def start_server(lock, server_host, server_port):
    blockchainserver = BlockchainServer(server_host, server_port, lock)


#global state
lock = Lock()
blockchain = BlockChain()

config_fd = open('config.json')
config_data = json.load(config_fd)
server_host = config_data["server_host"]
server_port = config_data["server_port"]

server_thread = Thread(target=start_server, args=(lock, server_host, server_port,))
server_thread.setDaemon(True)
server_thread.start()

while True:

    user_input = input("\n\n***WELCOME TO CENTRAL BLOCKCHAIN TOY SERVER***\n\nPRESS 1 TO SEE THE BLOCKCHAIN!\nPRESS 0 to exit\nEnter your choice:")

    if user_input == "1":
        lock.acquire()

        idx = 0
        print("{:<3} {:<8} {:<10} {:<9} {:<64}".format('Id','Sender','Reciever','Amount', 'Hash'))
        for block in blockchain.blocks:
            idx, sender, reciever, amount, hash_val = idx+1, block["sender"], block["reciever"], block["amount"], block["hash"]
            print("{:<3} {:<8} {:<10} {:<9} {:<64}".format(idx, sender, reciever, amount, hash_val))

        lock.release()
    elif user_input == "0":
        print("\nTERMINATING THE BLOCKCHAIN SERVER! GOODBYE!")
        break
    else:
        print("\nINVALID INPUT!")

