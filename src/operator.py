from is_wire.core import Channel, Message, Subscription, Logger, StatusCode, Status
from RequisicaoRobo_pb2 import RequisicaoRobo
from is_msgs.common_pb2 import Position
from google.protobuf.empty_pb2 import Empty
import socket
import json
import sys
import time
import random

def request_false(channel):
    function_name = 'FALSE FUNCTION'
    log.info(f"Creating {function_name} request...")
    request = RequisicaoRobo(function='false_funtion')
    message = Message(content=request, reply_to=subscription)

    log.info(f"Sending {function_name} request...")
    channel.publish(message, topic=config["topic_request"])
    try:
        log.info(f"Waiting {function_name} reply...")
        reply = channel.consume(timeout=1.0)
        log.info(f'{function_name} Reply: {reply.status.code} - why: {reply.status.why}')
    except socket.timeout:
        print('No reply :(')
    return

def request_get(channel,ID):
    function_name = 'GET POSITION'
    log.info(f"Creating {function_name} request...")
    request = RequisicaoRobo(id=ID,function='get_position')
    message = Message(content=request, reply_to=subscription)

    log.info(f"Sending {function_name} request...")
    channel.publish(message, topic=config["topic_request"])
    try:
        log.info(f"Waiting {function_name} reply...")
        reply = channel.consume(timeout=1.0)
        req = reply.unpack(RequisicaoRobo)
        log.info(f"{function_name} reply:")
        log.info(f'ROBOT ID: {req.id} - FUNCTION: {req.function} - X: {req.positions.x} - Y: {req.positions.y}')
    except socket.timeout:
        print('No reply :(')
    return

def request_set(channel,ID,X,Y):
    function_name = 'SET POSITION'
    log.info(f"Creating {function_name} request with X: {X} - Y:{Y}")
    position = Position(x=X,y=Y)
    request = RequisicaoRobo(id=ID,function='set_position',positions=position)
    message = Message(content=request, reply_to=subscription)

    log.info(f"Sending {function_name} request...")
    channel.publish(message, topic=config["topic_request"])
    try:
        log.info(f"Waiting {function_name} reply...")
        reply = channel.consume(timeout=1.0)
        log.info(f"{function_name} Reply: {reply.status.code} - why: {reply.status.why}")
    except socket.timeout:
        print('No reply :(')
    return

config_file = sys.argv[1] if len(sys.argv) > 1 else '../etc/conf/config.json'
config = json.load(open(config_file, 'r'))

log = Logger(name = config['operator'])

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

request_get(channel,ID) # get position

x = random.randint(0,9)
y = random.randint(0,9)
request_set(channel,ID,x,y) # set position

request_get(channel,ID) # get new position

request_false(channel) # test invalid request
