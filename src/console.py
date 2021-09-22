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
        function_name = 'GET POSITION'

        log.info(f"{function_name} request received from " + config["operator"] + "...")
        request = RobotTaskReply()
        request.id = RequisicaoRobo.id
        message = Message(content=request, reply_to=subscription)
        log.info(f"Sending {function_name} request to " + config["controller"] + "...")
        channel.publish(message, topic=config["topic_get"])
        try:
            log.info(f"Waiting {function_name} reply from " + config["controller"] + "...")
            reply = channel.consume(timeout=1.0)
            position = reply.unpack(Position)
            RequisicaoRobo.positions.x = position.x
            RequisicaoRobo.positions.y = position.y
            log.info(f"{function_name} from " + config["controller"] + " received:")
            log.info(f'Robot ID: {RequisicaoRobo.id} - X: {RequisicaoRobo.positions.x} - Y: {RequisicaoRobo.positions.y}')
            log.info(f"Sending {function_name} reply to " + config["operator"] + "...")
        except socket.timeout:  print('No reply :(')
    
    elif RequisicaoRobo.function == 'set_position':
        function_name = 'SET POSITION'

        log.info(f"{function_name} request received from " + config["operator"] + "...")
        request = RobotTaskRequest()
        request.id = RequisicaoRobo.id
        request.basic_move_task.positions.append(RequisicaoRobo.positions)
        message = Message(content=request, reply_to=subscription)
        log.info(f"Sending {function_name} request to " + config["controller"] + "...")
        log.info(f'ROBOT ID: {request.id} - X: {request.basic_move_task.positions[0].x} - Y: {request.basic_move_task.positions[0].y}')
        channel.publish(message, topic=config["topic_set"])
        try:
            log.info(f"Waiting {function_name} reply from " + config["controller"] + "...")
            reply = channel.consume(timeout=1.0)
            log.info(f'{function_name} Reply: {reply.status.code} - why: {reply.status.why}')
        except socket.timeout:  print('No reply :(')

    else:
        log.info("INVALID ARGUMENT request received from "+config["operator"]+"...")
        return Status(StatusCode.INVALID_ARGUMENT, "Function must be get_position or set_position")

    return RequisicaoRobo

config_file = sys.argv[1] if len(sys.argv) > 1 else '../etc/conf/config.json'
config = json.load(open(config_file, 'r'))

log = Logger(name = config["console"])

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
        log.info("Sending notification to ", config["operator"])
        message.body = config["message_FAIL"].encode('latin1')
        channel.publish(message, topic = config["topic_start"])

log.info("SYSTEM ONLINE")
# Send reply
time.sleep(1)
log.info("Sending notification to " + config["operator"])
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