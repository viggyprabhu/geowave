/**
 * 
 */
package mil.nga.giat.geowave.datastore.accumulo.wrappers;

import mil.nga.giat.geowave.core.iface.store.client.IIteratorSetting;

import org.apache.accumulo.core.client.IteratorSetting;

/**
 * @author viggy
 *
 */
public class AccumuloIteratorSetting implements IIteratorSetting {

	
	private IteratorSetting m_iteratorSetting;

	public AccumuloIteratorSetting(IteratorSetting iteratorSetting) {
		m_iteratorSetting = iteratorSetting; 
	}

	@Override
	public void addOption(String option, String value) {
		m_iteratorSetting.addOption(option, value);
	}

	@Override
	public String getName() {
		return m_iteratorSetting.getName();
	}

}
