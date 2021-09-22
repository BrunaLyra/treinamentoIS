from is_wire.core import Channel, Subscription, Message, Logger, StatusCode, Status
from is_wire.rpc import ServiceProvider
from is_msgs.robot_pb2 import RobotTaskReply, RobotTaskRequest
from RequisicaoRobo_pb2 import RequisicaoRobo
from is_msgs.common_pb2 import Position
import socket
import random
import time
import json
import sys

def requests(RequisicaoRobo, ctx):

    if RequisicaoRobo.function == 'get_position':
        log.info("GET POSITION request received from OPERATOR...")
        request = RobotTaskReply()
        request.id = RequisicaoRobo.id
        message = Message(content=request, reply_to=subscription)
        log.info("Sending GET POSITION request to ROBOT CONTROLLER...")
        channel.publish(message, topic=config["topic_get"])
        try:
            log.info("Waiting GET POSITION reply from ROBOT CONTROLLER...")
            reply = channel.consume(timeout=1.0)
            position = reply.unpack(Position)
            RequisicaoRobo.positions.x = position.x
            RequisicaoRobo.positions.y = position.y
            log.info("GET POSITION from ROBOT CONTROLLER received:")
            log.info(f'Robot ID: {RequisicaoRobo.id} - X: {RequisicaoRobo.positions.x} - Y: {RequisicaoRobo.positions.y}')
            log.info("Sending GET POSITION reply to OPERATOR...")
        except socket.timeout:  print('No reply :(')
    
    elif RequisicaoRobo.function == 'set_position':
        log.info("SET POSITION request received from OPERATOR...")
        request = RobotTaskRequest()
        request.id = RequisicaoRobo.id
        request.basic_move_task.positions.append(RequisicaoRobo.positions)
        message = Message(content=request, reply_to=subscription)
        log.info("Sending SET POSITION request to ROBOT CONTROLLER...")
        log.info(f'Robot ID: {request.id} - X: {request.basic_move_task.positions[0].x} - Y: {request.basic_move_task.positions[0].y}')
        channel.publish(message, topic=config["topic_set"])
        try:
            log.info("Waiting SET POSITION reply from ROBOT CONTROLLER...")
            reply = channel.consume(timeout=1.0)
            log.info(f'SET POSITION Reply: {reply.status.code}')
        except socket.timeout:  print('No reply :(')

    return RequisicaoRobo

config_file = sys.argv[1] if len(sys.argv) > 1 else '../etc/conf/config.json'
config = json.load(open(config_file, 'r'))

log = Logger(name = "CONSOLE")

# Connect to the broker
log.info("Creating channels...")
channel = Channel(config["broker"])
subscription = Subscription(channel)
subscription.subscribe(topic=config['topic_start'])

#######################
''' TURN SYSTEM ON '''
#######################

# Wait message
log.info("Waiting TURN ON message...")
while True:
    message = channel.consume()
    command = message.body.decode('latin1')
    if command == config['message_start']:
        log.info("Message received. Checking content and trying to bring system online...")
        if random.random() < config['probability']:     break
        
        log.warn("Failed to bring system online.")
        time.sleep(1)
        log.info("Sending notification to OPERATOR")
        message.body = config["message_FAIL"].encode('latin1')
        channel.publish(message, topic = config["topic_start"])

log.info("SYSTEM ONLINE")
# Send reply
time.sleep(1)
log.info("Sending notification to OPERATOR")
message.body = config['message_ON'].encode('latin1')
channel.publish(message, topic = config["topic_start"])

#######################
''' REQUESTS '''
#######################

log.info("Creating the RPC Server and waiting requests...")
provider = ServiceProvider(channel)

provider.delegate(
    topic=config["topic_request"],
    function=requests,
    request_type=RequisicaoRobo,
    reply_type=RequisicaoRobo)

provider.run()