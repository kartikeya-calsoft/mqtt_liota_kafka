import logging
from liota.dccs.dcc import DataCenterComponent
from liota.entities.metrics.registered_metric import RegisteredMetric
from liota.entities.metrics.metric import Metric
from liota.entities.registered_entity import RegisteredEntity

log = logging.getLogger(__name__)

class Kafka(DataCenterComponent):
	def __init__(self, comms):
		super(Kafka, self).__init__(
			comms=comms
		)

	def register(self,entity_obj):
		log.info("Registering resource with Kafka DCC {0}".format(entity_obj.name))
		if isinstance(entity_obj, Metric):
			return RegisteredMetric(entity_obj, self, None)
		else:
			return RegisteredEntity(entity_obj, self, None)

	def create_relationship(self, reg_entity_parent, reg_entity_child):
		reg_entity_child.parent = reg_entity_parent

	def _format_data(self, reg_metric):
		met_cnt = reg_metric.values.qsize()
		message = []
		if met_cnt == 0:
			return
		for _ in range(met_cnt):
			v = reg_metric.values.get(block=True)[1]	#tuple is received where 1st index contains data
			if v is not None:
				message.append(v)
		if message == '':
			return
		log.info ("Publishing values to Kafka DCC")
		log.debug("Formatted message: {0}".format(message))
		return message

	def set_properties(self, reg_entity, properties):
		raise NotImplementedError

	def unregister(self, entity_obj):
		raise NotImplementedError
