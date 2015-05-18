/**
 * 
 */
package mil.nga.giat.geowave.core.iface.store.scan;

import mil.nga.giat.geowave.core.iface.store.client.IIteratorSetting;

/**
 * @author viggy
 *
 */
public interface IScannerBase {

	void addScanIterator(IIteratorSetting iteratorSettings);

}
