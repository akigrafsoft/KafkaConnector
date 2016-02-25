/**
 * Open-source, by AkiGrafSoft.
 *
 * $Id:  $
 *
 **/
package com.akigrafsoft.kafkakonnector;

import com.akigrafsoft.knetthreads.konnector.KonnectorConfiguration;

/**
 * Configuration class for {@link KafkaProducerKonnector}
 * <p>
 * <b>This MUST be a Java bean</b>
 * </p>
 * 
 * @author kmoyse
 * 
 */
public class KafkaProducerConfiguration extends KonnectorConfiguration {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3519105875733777710L;

	private String serversList;

	// ------------------------------------------------------------------------
	// Java Bean

	public String getServersList() {
		return serversList;
	}

	public void setServersList(String serversList) {
		this.serversList = serversList;
	}

}
