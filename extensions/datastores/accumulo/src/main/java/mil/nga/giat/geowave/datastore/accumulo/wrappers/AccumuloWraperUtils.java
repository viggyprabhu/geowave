/**
 * 
 */
package mil.nga.giat.geowave.datastore.accumulo.wrappers;

import java.util.Iterator;
import java.util.Map.Entry;

import mil.nga.giat.geowave.core.iface.field.IKey;
import mil.nga.giat.geowave.core.iface.field.IValue;
import mil.nga.giat.geowave.core.iface.store.CoreScanner;
import mil.nga.giat.geowave.core.iface.store.CoreWriter;
import mil.nga.giat.geowave.core.iface.store.client.IIteratorSetting;
import mil.nga.giat.geowave.core.iface.store.scan.IScannerBase;
import mil.nga.giat.geowave.core.store.mapreduce.client.CoreIteratorConfig;
import mil.nga.giat.geowave.datastore.accumulo.IteratorConfig;
import mil.nga.giat.geowave.datastore.accumulo.Writer;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

/**
 * @author viggy
 *
 */
public class AccumuloWraperUtils {

	public static ScannerBase getScannerBase(IScannerBase scanner) {
		// TODO #238 Auto-generated method stub
		return null;
	}

	public static IteratorSetting getIteratorSetting(
			IIteratorSetting iteratorSettings) {
		// TODO #238 Auto-generated method stub
		return null;
	}

	public static Writer getWriter(CoreWriter createWriter) {
		// TODO #238 Auto-generated method stub
		return null;
	}

	public static Scanner getScanner(CoreScanner createScanner) {
		// TODO #238 Auto-generated method stub
		return null;
	}

	public static Iterator<Entry<IKey, IValue>> convert(
			Iterator<Entry<Key, Value>> iterator) {
		// TODO #238 Need to convert
		return null;
	}

	public static Iterator<Entry<Key, Value>> reconvert(
			Iterator<Entry<IKey, IValue>> iterator) {
		// TODO #238 NEed to reconvert
		return null;
	}

	public static IteratorConfig getIteratorConfig(
			CoreIteratorConfig coreIteratorConfig) {
		// TODO #238 Auto-generated method stub
		return null;
	}

}
