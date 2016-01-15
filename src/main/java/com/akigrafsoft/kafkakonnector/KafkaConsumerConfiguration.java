package com.akigrafsoft.kafkakonnector;

import com.akigrafsoft.knetthreads.ExceptionAuditFailed;
import com.akigrafsoft.knetthreads.konnector.KonnectorConfiguration;

public class KafkaConsumerConfiguration extends KonnectorConfiguration {

	public String zookeeper;
	public String groupId;
	public int numberOfThreads = 5;

	public String topic;

	@Override
	public void audit() throws ExceptionAuditFailed {
		// TODO Auto-generated method stub

		if (zookeeper == null || zookeeper.isEmpty()) {
			throw new ExceptionAuditFailed("zookeeper must be filled");
		}
		if (groupId == null || groupId.isEmpty()) {
			throw new ExceptionAuditFailed("groupId must be filled");
		}
		if (topic == null || topic.isEmpty()) {
			throw new ExceptionAuditFailed("topic must be filled");
		}
	}

}
