package com.akigrafsoft.kafkakonnector;

import com.akigrafsoft.knetthreads.Message;
import com.akigrafsoft.knetthreads.konnector.KonnectorDataobject;

public class KafkaDataobject extends KonnectorDataobject {

	/**
	 * 
	 */
	private static final long serialVersionUID = -2657604807506805050L;

	public String topic;

	/**
	 * Optional key associated with the message
	 */
	public String key = null;

	public KafkaDataobject(Message message) {
		super(message);
		// TODO Auto-generated constructor stub
	}

}
