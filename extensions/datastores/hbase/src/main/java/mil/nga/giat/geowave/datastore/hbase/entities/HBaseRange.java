/**
 * 
 */
package mil.nga.giat.geowave.datastore.hbase.entities;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * @author viggy
 * 
 */

public class HBaseRange implements
		WritableComparable<HBaseRange>
{

	private boolean infiniteStartKey = false;
	private boolean infiniteStopKey = false;
	private boolean startKeyInclusive;
	private boolean stopKeyInclusive;
	private byte[] start;
	private byte[] stop;

	@Override
	public void readFields(
			DataInput in )
			throws IOException {
		// TODO #406 Need to fix
	}

	@Override
	public void write(
			DataOutput arg0 )
			throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public int compareTo(
			HBaseRange o ) {
		// TODO #406 Need to fix
		return 0;
	}

	public boolean isInfiniteStartKey() {
		return infiniteStartKey;
	}

	public byte[] getStartKey() {
		if (infiniteStartKey) {
			return null;
		}
		return start;
	}

	public boolean isInfiniteStopKey() {
		return infiniteStopKey;
	}

	public byte[] getEndKey() {
		if (infiniteStopKey) {
			return null;
		}
		return stop;
	}

}
