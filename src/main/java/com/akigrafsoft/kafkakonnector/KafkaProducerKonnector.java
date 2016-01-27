package com.akigrafsoft.kafkakonnector;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.akigrafsoft.knetthreads.ExceptionDuplicate;
import com.akigrafsoft.knetthreads.konnector.Konnector;
import com.akigrafsoft.knetthreads.konnector.KonnectorConfiguration;
import com.akigrafsoft.knetthreads.konnector.KonnectorDataobject;

public class KafkaProducerKonnector extends Konnector {

	private KafkaProducer<String, String> m_producer;

	private String m_serversList;

	protected KafkaProducerKonnector(String name) throws ExceptionDuplicate {
		super(name);
	}

	@Override
	protected void doLoadConfig(KonnectorConfiguration config) {

		if (!(config instanceof KafkaProducerConfiguration))
			throw new IllegalArgumentException(
					"config must be KafkaProducerConfiguration");

		KafkaProducerConfiguration l_config = (KafkaProducerConfiguration) config;
		m_serversList = l_config.getServersList();
	}

	@Override
	protected CommandResult doStart() {

		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, m_serversList);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringSerializer");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringSerializer");
		props.put(ProducerConfig.CLIENT_ID_CONFIG, "my-producer");
		// props.put("request.required.acks", "1");
		m_producer = new KafkaProducer<String, String>(props);

		this.setStarted();

		return CommandResult.Success;
	}

	@Override
	public void doHandle(KonnectorDataobject dataobject) {

		KafkaDataobject l_dataobject = (KafkaDataobject) dataobject;

		final ProducerRecord<String, String> data;

		if (l_dataobject.key != null) {
			data = new ProducerRecord<String, String>(l_dataobject.topic,
					l_dataobject.key, l_dataobject.outboundBuffer);
		} else {
			data = new ProducerRecord<String, String>(l_dataobject.topic,
					l_dataobject.outboundBuffer);
		}

		m_producer.send(data);

		this.resumeWithExecutionComplete(dataobject);
	}

	@Override
	protected CommandResult doStop() {

		if (m_producer != null)
			m_producer.close();

		m_producer = null;

		this.setStopped();
		return CommandResult.Success;
	}

}
