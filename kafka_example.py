import Queue
import logging
import pint
import string

from liota.dccs.dcc import RegistrationFailure
from liota.device_comms.mqtt_device_comms import MqttDeviceComms
from liota.entities.edge_systems.dk300_edge_system import Dk300EdgeSystem
from liota.entities.metrics.metric import Metric
from liota.lib.utilities.utility import read_user_config
# from liota.dccs.graphite import Graphite
from liota.dcc_comms.socket_comms import SocketDccComms
from kafka import KafkaConsumer
from kafka_comms import KafkaDccComms
from _kafka import Kafka

log = logging.getLogger(__name__)
# getting values from conf file
config = read_user_config('samplePropMqtt.conf')

# Create unit registry
ureg = pint.UnitRegistry()

# Store temperature values in Queue
kafka_data = Queue.Queue()

# Callback functions
# To put corresponding values in queue
def callback_kitchen_temp(client, userdata, message):
    kitchen_temperature_data.put(float(message.payload))


def callback_living_room_temp(client, userdata, message):
    living_room_temperature_data.put(float(message.payload))


def callback_presence(client, data, message):
    presence_data.put(float(message.payload))
    
def callback_kafka(client, data, message):
    try:
        kafka_data.put({str(string.replace(str(message.topic),"/",".")) : str(message.payload)})	#Excluding part before '/' in topic
    except:
        pass

# Extract data from Queue
def get_value(queue):
    data = kafka_data.get(block=True) 
    print "Got data "
    print data
    return data

if __name__ == "__main__":

    #  Creating EdgeSystem
    edge_system = Dk300EdgeSystem(config['EdgeSystemName'])

    # Connect with MQTT broker using DeviceComms and subscribe to topics
    # Get kitchen and living room temperature values using MQTT channel
    
    kafka = Kafka(KafkaDccComms(ip = config['KafkaIP'], port = str(config['KafkaPort'])))
    # graphite = Graphite(SocketDccComms(ip=config['GraphiteIP'],
                               # port=int(config['GraphitePort'])))

    kafka_reg_edge_system = kafka.register(edge_system)
    # graphite_reg_edge_system = graphite.register(edge_system)
    mqtt_conn = MqttDeviceComms(url = config['BrokerIP'], port = config['BrokerPort'], identity=None,
                            tls_conf=None,
                            qos_details=None,
                            clean_session=True,
                            keep_alive=config['keep_alive'], enable_authentication=False)

    mqtt_conn.subscribe(config['MqttChannel1'],0, callback_kafka)


    try:

        metric_name = config['MetricName']
        content_metric = Metric(
                                name=metric_name,
                                unit=None,
                                interval=1,
                                aggregation_size=1,
                                sampling_function=get_value  #this is coming from the xmpp device/server
                                #sampling_function = read_cpu_utilization
                                #sampling_function = random_fun
        )
        reg_content_metric = kafka.register(content_metric)
        kafka.create_relationship(kafka_reg_edge_system, reg_content_metric)
        reg_content_metric.start_collecting()
    
    except RegistrationFailure:
        print "Registration to IOTCC failed"
