from is_wire.core import Channel, Message, Subscription, Logger, StatusCode, Status
from RequisicaoRobo_pb2 import RequisicaoRobo
from is_msgs.common_pb2 import Position
from google.protobuf.empty_pb2 import Empty
import socket
import json
import sys
import time
import random

def request_get(channel,ID):
    log.info("Creating GET POSITION request...")
    request = RequisicaoRobo(id=ID,function='get_position')
    message = Message(content=request, reply_to=subscription)

    log.info("Sending GET POSITION request...")
    channel.publish(message, topic=config["topic_request"])
    try:
        log.info("Waiting GET POSITION reply...")
        reply = channel.consume(timeout=1.0)
        req = reply.unpack(RequisicaoRobo)
        log.info("GET POSITION reply:")
        log.info(f'ROBOT ID: {req.id} - FUNCTION: {req.function} - X: {req.positions.x} - Y: {req.positions.y}')
    except socket.timeout:
        print('No reply :(')
    return

def request_set(channel,ID,X,Y):
    log.info(f"Creating SET POSITION request with X: {X} - Y:{Y}")
    position = Position(x=X,y=Y)
    request = RequisicaoRobo(id=ID,function='set_position',positions=position)
    message = Message(content=request, reply_to=subscription)

    log.info("Sending SET POSITION request...")
    channel.publish(message, topic=config["topic_request"])
    try:
        log.info("Waiting SET POSITION reply...")
        reply = channel.consume(timeout=1.0)
        log.info(f'SET POSITION Reply: {reply.status.code}')
    except socket.timeout:
        print('No reply :(')
    return

config_file = sys.argv[1] if len(sys.argv) > 1 else '../etc/conf/config.json'
config = json.load(open(config_file, 'r'))

log = Logger(name = "OPERATOR")

# Connect to the broker
log.info("Creating channels...")
channel = Channel(config["broker"])
subscription = Subscription(channel)
subscription.subscribe(topic=config['topic_start'])

# Create message
log.info("Creating TURN ON message...")
message = Message()

#######################
''' TURN SYSTEM ON '''
#######################

reply = None
while reply != config['message_ON']:
    # Send message
    log.info("Sending TURN ON message...")
    message.body = config['message_start'].encode('latin1')
    channel.publish(message, topic = config["topic_start"])
    log.info("Waiting reply...")

    # Wait reply
    while True:
        message = channel.consume()
        reply = message.body.decode('latin1')
        if reply == config['message_ON']:
            log.info("SYSTEM ONLINE")
            break
        elif reply == config['message_FAIL']:
            log.info("System offline. Trying again...")
            break
    time.sleep(1)

#######################
''' REQUESTS '''
#######################

log.info("Getting a randomized ID")
ID = random.randint(1,5)
log.info(f"Robot ID: {ID}")

request_get(channel,ID)

x = random.randint(0,9)
y = random.randint(0,9)
request_set(channel,ID,x,y)

request_get(channel,ID)
