/**
 * 
 */
package mil.nga.giat.geowave.datastore.hbase.entities;

import java.nio.ByteBuffer;

/**
 * @author viggy
 *
 */
public class HBaseRowId {


	private byte[] insertionId;
	private byte[] dataId;
	private byte[] adapterId;
	private int numberOfDuplicates;

	public HBaseRowId(
			final byte[] indexId,
			final byte[] dataId,
			final byte[] adapterId,
			final int numberOfDuplicates ) {
		this.insertionId = indexId;
		this.dataId = dataId;
		this.adapterId = adapterId;
		this.numberOfDuplicates = numberOfDuplicates;
	}
	
	public byte[] getRowId() {
		final ByteBuffer buf = ByteBuffer.allocate(12 + dataId.length + adapterId.length + insertionId.length);
		buf.put(insertionId);
		buf.put(adapterId);
		buf.put(dataId);
		buf.putInt(adapterId.length);
		buf.putInt(dataId.length);
		buf.putInt(numberOfDuplicates);
		return buf.array();
	}
}
