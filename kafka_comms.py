import logging
import time

from kafka import KafkaProducer
from liota.dcc_comms.dcc_comms import DCCComms

log = logging.getLogger(__name__)

class KafkaDccComms(DCCComms):
	def __init__(self, ip, port):
		self.ip = ip
		self.port = port
		self._connect()

	def _connect(self):
		try:
			self.client = KafkaProducer(bootstrap_servers=str(self.ip)+':'+str(self.port))
			log.info("Establishing Kafka Connection")
		except Exception as ex:
			log.exception("Unable to establish Kafka connection.")
			raise ex


	def send(self,message,msg_attr=None):
		if self.client is not None:
			for publishing_data in message:
				log.debug("Publishing message to kafka on topic "+ str(publishing_data.keys()[0]) + " : " + str(publishing_data.values()[0]))
				self.client.send(str(publishing_data.keys()[0]),str(publishing_data.values()[0]))

	def _disconnect(self):
		raise NotImplementedError

	def receive(self):
		raise NotImplementedError

	
