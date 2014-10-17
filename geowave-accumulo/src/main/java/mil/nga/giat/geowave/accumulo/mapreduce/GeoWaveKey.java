package mil.nga.giat.geowave.accumulo.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import mil.nga.giat.geowave.index.ByteArrayId;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public abstract class GeoWaveKey implements
		WritableComparable<GeoWaveKey>
{
	protected ByteArrayId adapterId;
	protected WritableComparable comparableDelegate;

	protected GeoWaveKey() {}

	public GeoWaveKey(
			final ByteArrayId adapterId,
			final WritableComparable comparableDelegate ) {
		this.adapterId = adapterId;
		this.comparableDelegate = comparableDelegate;
	}

	public ByteArrayId getAdapterId() {
		return adapterId;
	}

	@Override
	public int compareTo(
			final GeoWaveKey o ) {
		if (equals(o)) {
			return 0;
		}
		if (o == null) {
			return 1;
		}
		if (comparableDelegate == null) {
			if (o.comparableDelegate == null) {
				return 0;
			}
			return -1;
		}
		return comparableDelegate.compareTo(o.comparableDelegate);
	}

	@Override
	public void readFields(
			final DataInput input )
			throws IOException {
		try {
			final Class<? extends WritableComparable> cls = Class.forName(
					Text.readString(input)).asSubclass(
					WritableComparable.class);
			comparableDelegate = cls.newInstance();
			comparableDelegate.readFields(input);
			final int adapterIdLength = input.readInt();
			final byte[] adapterIdBinary = new byte[adapterIdLength];
			input.readFully(adapterIdBinary);
			adapterId = new ByteArrayId(
					adapterIdBinary);
		}
		catch (final InstantiationException | IllegalAccessException | ClassNotFoundException e) {
			throw (IOException) new IOException(
					"Failed geowave key init").initCause(e);
		}
	}

	@Override
	public void write(
			final DataOutput output )
			throws IOException {
		Text.writeString(
				output,
				comparableDelegate.getClass().getName());
		comparableDelegate.write(output);
		final byte[] adapterIdBinary = adapterId.getBytes();
		output.writeInt(adapterIdBinary.length);
		output.write(adapterIdBinary);
	}

}
