/**
 * 
 */
package mil.nga.giat.geowave.core.iface.store.scan;

import java.util.Iterator;
import java.util.Map.Entry;

import mil.nga.giat.geowave.core.iface.field.IKey;
import mil.nga.giat.geowave.core.iface.field.IValue;
import mil.nga.giat.geowave.core.iface.store.client.IIteratorSetting;

import org.apache.hadoop.io.Text;

/**
 * @author viggy
 *
 */
public interface IScannerBase {

	void addScanIterator(IIteratorSetting iteratorSettings);

	void fetchColumnFamily(Text text);

	void close();

	Iterator<Entry<IKey, IValue>> iterator();


}
