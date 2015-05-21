/**
 * 
 */
package mil.nga.giat.geowave.datastore.accumulo.wrappers;

import mil.nga.giat.geowave.core.iface.store.client.IIteratorSetting;
import mil.nga.giat.geowave.core.store.data.IteratorScopeEntity;
import mil.nga.giat.geowave.core.store.mapreduce.client.CoreIteratorConfig;
import mil.nga.giat.geowave.datastore.accumulo.IteratorConfig;

/**
 * @author viggy
 *
 */
public class AccumuloIteratorConfig extends CoreIteratorConfig {

	private IteratorConfig m_config;

	public AccumuloIteratorConfig(IIteratorSetting iteratorSettings,
			IteratorScopeEntity iteratorScopeEntity) {
		super(iteratorSettings, iteratorScopeEntity);
	}

	public AccumuloIteratorConfig(IteratorConfig config) {
		//TODO #238 Need to fix this
		this(null, null);
		m_config = config;
	}

}
