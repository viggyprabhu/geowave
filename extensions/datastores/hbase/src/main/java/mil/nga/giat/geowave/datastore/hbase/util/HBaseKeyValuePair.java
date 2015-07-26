/**
 * 
 */
package mil.nga.giat.geowave.datastore.hbase.util;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

/**
 * @author viggy
 * 
 */
public class HBaseKeyValuePair
{

	private final ImmutableBytesWritable key;
	private final KeyValue value;

	public HBaseKeyValuePair(
			final ImmutableBytesWritable key,
			final KeyValue value ) {
		super();
		this.key = key;
		this.value = value;
	}

	public ImmutableBytesWritable getKey() {
		return key;
	}

	public KeyValue getValue() {
		return value;
	}

}
