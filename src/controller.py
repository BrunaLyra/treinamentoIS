from is_wire.rpc import ServiceProvider, LogInterceptor
from is_wire.core import Channel, StatusCode, Status, Logger
from is_msgs.robot_pb2 import RobotTaskReply, RobotTaskRequest
from is_msgs.common_pb2 import Position
from google.protobuf.empty_pb2 import Empty
import time
import json
import sys
import random

class Robot():
    def __init__(self, id, x, y):
        self.id = id
        self.pos_x = x
        self.pos_y = y

    def get_id(self):
        return self.id

    def set_position(self, x, y):
        self.pos_x = x
        self.pos_y = y

    def get_position(self):
        return self.pos_x, self.pos_y

def get_position(RobotTaskReply, ctx):
    function_name = "GET POSITION"
    log.info(f"{function_name} request received...")
    log.info("Validating arguments...")
    if RobotTaskReply.id < 0 or RobotTaskReply.id > len(list_of_robot):
        return Status(StatusCode.INVALID_ARGUMENT, "Invalid ID")
    robot = get_robot_object(list_of_robot,RobotTaskReply.id)
    log.info(f'ROBOT ID: {robot.id} - X: {robot.pos_x} - Y: {robot.pos_y}')
    position = Position()
    position.x, position.y = robot.get_position()
    log.info(f"Sending {function_name} reply...")
    return position

def set_position(RobotTaskRequest, ctx):
    function_name = "SET POSITION"
    log.info(f"{function_name} request received...")
    log.info("Validating arguments...")
    if RobotTaskRequest.id < 0 or RobotTaskRequest.id > len(list_of_robot):
        return Status(StatusCode.INVALID_ARGUMENT, "Invalid ID")
    if RobotTaskRequest.basic_move_task.positions[0].x < 0 or RobotTaskRequest.basic_move_task.positions[0].y < 0:
        return Status(StatusCode.OUT_OF_RANGE, "The number must be positive")

    robot = get_robot_object(list_of_robot,RobotTaskRequest.id)
    robot.set_position(x=RobotTaskRequest.basic_move_task.positions[0].x, y=RobotTaskRequest.basic_move_task.positions[0].y)
    log.info(f'Moving ROBOT ID: {robot.id} to X: {robot.pos_x} - Y: {robot.pos_y}')
    time.sleep(0.5)
    log.info(f"Sending {function_name} reply...")
    return Empty()

def get_robot_object(list_of_robot,id):
    for robot in list_of_robot:
        if robot.get_id() == id:
            log.info("Robot found...")
            return robot


config_file = sys.argv[1] if len(sys.argv) > 1 else '../etc/conf/config.json'
config = json.load(open(config_file, 'r'))

log = Logger(name = config["controller"])

log.info("Initializing robots...")
list_of_robot = []
for i in range(5):
    list_of_robot.append(Robot(id=i+1, x=random.randint(0,9), y=random.randint(0,9)))
    log.info(f"Robot: {list_of_robot[i].id} - X: {list_of_robot[i].pos_x} - Y: {list_of_robot[i].pos_y}")


log.info("Creating channel...")
channel = Channel(config["broker"])

log.info("Creating the RPC Server and waiting requests...")
provider = ServiceProvider(channel)

# Os tipos das mensagens devem ser passados, tanto no request como no reply
provider.delegate(
    topic=config["topic_get"],
    function=get_position,
    request_type=RobotTaskReply,
    reply_type=Position)

provider.delegate(
    topic=config["topic_set"],
    function=set_position,
    request_type=RobotTaskRequest,
    reply_type=Empty)

provider.run()