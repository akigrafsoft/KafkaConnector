/**
 * Open-source, by AkiGrafSoft.
 *
 * $Id:  $
 *
 **/
package com.akigrafsoft.kafkakonnector;

import com.akigrafsoft.knetthreads.ExceptionAuditFailed;
import com.akigrafsoft.knetthreads.konnector.KonnectorConfiguration;

/**
 * Configuration class for {@link KafkaConsumerKonnector}
 * <p>
 * <b>This MUST be a Java bean</b>
 * </p>
 * 
 * @author kmoyse
 * 
 */
public class KafkaConsumerConfiguration extends KonnectorConfiguration {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2293791232462670686L;

	private String zookeeper;
	private String groupId;
	private int numberOfThreads = 5;
	private String topic;

	// ------------------------------------------------------------------------
	// Java Bean

	public String getZookeeper() {
		return zookeeper;
	}

	public void setZookeeper(String zookeeper) {
		this.zookeeper = zookeeper;
	}

	public String getGroupId() {
		return groupId;
	}

	public void setGroupId(String groupId) {
		this.groupId = groupId;
	}

	public int getNumberOfThreads() {
		return numberOfThreads;
	}

	public void setNumberOfThreads(int numberOfThreads) {
		this.numberOfThreads = numberOfThreads;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	// ------------------------------------------------------------------------
	// Configuration

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
