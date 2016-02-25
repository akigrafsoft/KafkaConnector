/**
 * Open-source, by AkiGrafSoft.
 *
 * $Id:  $
 *
 **/
package com.akigrafsoft.kafkakonnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import com.akigrafsoft.knetthreads.ExceptionDuplicate;
import com.akigrafsoft.knetthreads.Message;
import com.akigrafsoft.knetthreads.konnector.Konnector;
import com.akigrafsoft.knetthreads.konnector.KonnectorConfiguration;
import com.akigrafsoft.knetthreads.konnector.KonnectorDataobject;

public class KafkaConsumerKonnector extends Konnector {

	public String m_zookeeper;
	public String m_groupId;
	private String m_topic;
	private int m_numberOfThreads = 5;

	private ConsumerConnector m_consumer;
	private ExecutorService m_executor;

	protected KafkaConsumerKonnector(String name) throws ExceptionDuplicate {
		super(name);
	}

	class ConsumerTest implements Runnable {
		private KafkaStream<byte[], byte[]> m_stream;
		private int m_threadNumber;

		public ConsumerTest(KafkaStream<byte[], byte[]> a_stream, int a_threadNumber) {
			m_threadNumber = a_threadNumber;
			m_stream = a_stream;
		}

		public void run() {
			ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
			while (it.hasNext()) {
				// System.out.println("Thread " + m_threadNumber + ": "
				// + new String(it.next().message()));
				//
				Message message = new Message();
				KonnectorDataobject l_dataobject = new KonnectorDataobject(message);
				l_dataobject.inboundBuffer = new String(it.next().message());
				injectMessageInApplication(message, l_dataobject);
			}

			System.out.println("Shutting down Thread: " + m_threadNumber);
		}
	}

	@Override
	public Class<? extends KonnectorConfiguration> getConfigurationClass() {
		return KafkaConsumerConfiguration.class;
	}

	@Override
	protected void doLoadConfig(KonnectorConfiguration config) {
		KafkaConsumerConfiguration cfg = (KafkaConsumerConfiguration) config;
		this.m_zookeeper = cfg.getZookeeper();
		this.m_groupId = cfg.getGroupId();
		this.m_topic = cfg.getTopic();
		this.m_numberOfThreads = cfg.getNumberOfThreads();
	}

	@Override
	protected CommandResult doStart() {

		Properties props = new Properties();
		props.put("zookeeper.connect", m_zookeeper);
		props.put("group.id", m_groupId);
		props.put("zookeeper.session.timeout.ms", "400");
		props.put("zookeeper.sync.time.ms", "200");
		props.put("auto.commit.interval.ms", "1000");

		m_consumer = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(props));

		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(m_topic, new Integer(m_numberOfThreads));
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = m_consumer.createMessageStreams(topicCountMap);
		List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(m_topic);

		// now launch all the threads
		//
		m_executor = Executors.newFixedThreadPool(m_numberOfThreads);

		// now create an object to consume the messages
		//
		int threadNumber = 0;
		for (final KafkaStream<byte[], byte[]> stream : streams) {
			m_executor.submit(new ConsumerTest(stream, threadNumber));
			threadNumber++;
		}

		this.setStarted();
		this.setAvailable();
		return CommandResult.Success;
	}

	@Override
	public void doHandle(KonnectorDataobject dataobject) {
		// TODO Auto-generated method stub

	}

	@Override
	protected CommandResult doStop() {

		if (m_consumer != null)
			m_consumer.shutdown();
		if (m_executor != null)
			m_executor.shutdown();
		try {
			if (!m_executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
				System.out.println("Timed out waiting for consumer threads to shut down, exiting uncleanly");
			}
		} catch (InterruptedException e) {
			System.out.println("Interrupted during shutdown, exiting uncleanly");
		} finally {
			m_executor = null;
			this.setUnavailable();
			this.setStopped();
		}

		return CommandResult.Success;
	}

}
