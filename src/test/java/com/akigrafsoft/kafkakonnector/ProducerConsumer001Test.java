package com.akigrafsoft.kafkakonnector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.akigrafsoft.knetthreads.Dispatcher;
import com.akigrafsoft.knetthreads.Endpoint;
import com.akigrafsoft.knetthreads.ExceptionAuditFailed;
import com.akigrafsoft.knetthreads.ExceptionDuplicate;
import com.akigrafsoft.knetthreads.FlowProcessContext;
import com.akigrafsoft.knetthreads.Message;
import com.akigrafsoft.knetthreads.RequestEnum;
import com.akigrafsoft.knetthreads.konnector.Konnector;
import com.akigrafsoft.knetthreads.konnector.KonnectorDataobject;
import com.akigrafsoft.knetthreads.routing.KonnectorRouter;

/**
 * IN order to tun this test, a Kafka instance MUST be running.<br>
 * 1) Install Apache Kafka:<br>
 * <code>
tar -xzf kafka_2.10-0.8.2.0.tgz
cd kafka_2.10-0.8.2.0
</code>
 * 
 * 
 * 2) Setting up a multi-broker cluster<br>
 * <code>
cp config/server.properties config/server-1.properties
cp config/server.properties config/server-2.properties
</code> Update : config/server.properties: <code>
delete.topic.enable=true
</code> config/server-1.properties: <code>
broker.id=1
port=9093
log.dir=/tmp/kafka-logs-1
delete.topic.enable=true
</code>
 * 
 * config/server-2.properties: <code>
broker.id=2
port=9094
log.dir=/tmp/kafka-logs-2
delete.topic.enable=true
</code>
 * 
 * The broker.id property is the unique and permanent name of each node in the
 * cluster. We have to override the port and log directory only because we are
 * running these all on the same machine and we want to keep the brokers from
 * all trying to register on the same port or overwrite each others data.
 * 
 * 3) Start zookeeper and Kafka server nodes :<br>
 * <code>
./bin/kafka-server-stop.sh
./bin/zookeeper-server-stop.sh

./bin/zookeeper-server-start.sh config/zookeeper.properties &
./bin/kafka-server-start.sh config/server.properties &
./bin/kafka-server-start.sh config/server-1.properties &
./bin/kafka-server-start.sh config/server-2.properties &
</code>
 * 
 * 4) Create topic<br>
 * <code>
./bin/kafka-topics.sh --describe --zookeeper localhost:2181 --topic my-replicated-topic
./bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 3 --partitions 1 --topic my-replicated-topic
./bin/kafka-topics.sh --describe --zookeeper localhost:2181 --topic my-replicated-topic
Topic:my-replicated-topic	PartitionCount:1	ReplicationFactor:3	Configs:
	Topic: my-replicated-topic	Partition: 0	Leader: 0	Replicas: 0,1,2	Isr: 0,1,2

</code>
 * 
 * @author kmoyse
 * 
 */
@Ignore
public class ProducerConsumer001Test {
	static String PRODUCER_NAME = "Produce";
	static Konnector m_producer;

	static String CONSUMER_NAME = "Consume";
	static Konnector m_consumer;

	static String REPLICATED_TOPIC = "my-replicated-topic";

	// static int port = Utils.findFreePort();

	class Received {
		Message message;
		KonnectorDataobject dataobject;

		public Received(Message message, KonnectorDataobject dataobject) {
			super();
			this.message = message;
			this.dataobject = dataobject;
		}
	}

	static Received received;

	@BeforeClass
	public static void setUpClass() {
		try {
			m_producer = new KafkaProducerKonnector(PRODUCER_NAME);
		} catch (ExceptionDuplicate e) {
			e.printStackTrace();
			fail(e.getMessage());
			return;
		}

		try {
			m_consumer = new KafkaConsumerKonnector(CONSUMER_NAME);
		} catch (ExceptionDuplicate e) {
			e.printStackTrace();
			fail(e.getMessage());
			return;
		}
	}

	@AfterClass
	public static void tearDownClass() throws Exception {
		m_consumer.destroy();
	}

	private void setupProducer() {
		KafkaProducerConfiguration cfg = new KafkaProducerConfiguration();
		cfg.setServersList("localhost:9092,localhost:9093,localhost:9094");

		try {
			assertEquals(Konnector.CommandResult.Success, m_producer.configure(cfg));
		} catch (ExceptionAuditFailed e) {
			e.printStackTrace();
			fail(e.getMessage());
			return;
		}

		try {
			Endpoint nap = new Endpoint(PRODUCER_NAME) {
				@Override
				public KonnectorRouter getKonnectorRouter(Message message, KonnectorDataobject dataobject) {
					return new KonnectorRouter() {
						public Konnector resolveKonnector(Message message, KonnectorDataobject dataobject) {
							return m_producer;
						}
					};
				}

				@Override
				public RequestEnum classifyInboundMessage(Message message, KonnectorDataobject dataobject) {
					received = new Received(message, dataobject);
					System.out.println(PRODUCER_NAME + "|classifyInboundMessage|" + dataobject.inboundBuffer);
					// leave null as this is fake anyway
					return null;
				}
			};
			nap.setDispatcher(new Dispatcher<RequestEnum>("foo") {
				public FlowProcessContext getContext(Message message, KonnectorDataobject dataobject,
						RequestEnum request) {
					return null;
				}
			});
			m_consumer.setEndpoint(nap);
		} catch (ExceptionDuplicate e) {
			e.printStackTrace();
			fail(e.getMessage());
			return;
		}
	}

	private void setupConsumer() {
		KafkaConsumerConfiguration cfg = new KafkaConsumerConfiguration();
		cfg.setZookeeper("localhost:2181");
		cfg.setGroupId("group3");
		cfg.setNumberOfThreads(1);
		cfg.setTopic("my-replicated-topic");

		try {
			assertEquals(Konnector.CommandResult.Success, m_consumer.configure(cfg));
		} catch (ExceptionAuditFailed e) {
			e.printStackTrace();
			fail(e.getMessage());
			return;
		}

		try {
			Endpoint nap = new Endpoint(CONSUMER_NAME) {
				@Override
				public KonnectorRouter getKonnectorRouter(Message message, KonnectorDataobject dataobject) {
					return new KonnectorRouter() {
						public Konnector resolveKonnector(Message message, KonnectorDataobject dataobject) {
							return m_consumer;
						}
					};
				}

				@Override
				public RequestEnum classifyInboundMessage(Message message, KonnectorDataobject dataobject) {
					received = new Received(message, dataobject);
					System.out.println(CONSUMER_NAME + "|classifyInboundMessage|" + dataobject.inboundBuffer);
					// leave null as this is fake anyway
					return null;
				}
			};
			nap.setDispatcher(new Dispatcher<RequestEnum>("foo") {
				public FlowProcessContext getContext(Message message, KonnectorDataobject dataobject,
						RequestEnum request) {
					return null;
				}
			});
			m_consumer.setEndpoint(nap);
		} catch (ExceptionDuplicate e) {
			e.printStackTrace();
			fail(e.getMessage());
			return;
		}
	}

	@Test
	public void test() {

		setupProducer();
		setupConsumer();

		assertEquals(Konnector.CommandResult.Success, m_consumer.start());
		assertEquals(Konnector.CommandResult.Success, m_producer.start());

		// Inject a message with producer
		Message l_msg = new Message();
		KafkaDataobject l_do = new KafkaDataobject(l_msg);
		l_do.topic = REPLICATED_TOPIC;
		l_do.outboundBuffer = "my message test";

		// {"unique_id":"glop","uri":"testuri/uri","host":"terbium","headers":{},"geoip":{},"qs":{},"cookies":{},"estat":{},"metrics":{}}

		for (int i = 0; i < 5; i++) {
			l_do.outboundBuffer = "my message test" + i;
			m_producer.handle(l_do);
			assertEquals("check DO status", l_do.executionStatus, KonnectorDataobject.ExecutionStatus.PASS);
		}

		// Utils.sleep(1000);
		try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		assertEquals(Konnector.CommandResult.Success, m_consumer.stop());
	}
}
