package mil.nga.giat.geowave.accumulo.mapreduce.input;

import mil.nga.giat.geowave.accumulo.mapreduce.GeoWaveKey;
import mil.nga.giat.geowave.index.ByteArrayId;

import org.apache.hadoop.io.WritableComparator;

public class GeoWaveInputKey extends
		GeoWaveKey
{
	private final ByteArrayId dataId;

	public GeoWaveInputKey(
			final ByteArrayId adapterId,
			final ByteArrayId dataId ) {
		super(
				adapterId);
		this.dataId = dataId;
	}

	public ByteArrayId getDataId() {
		return dataId;
	}

	@Override
	public int compareTo(
			final GeoWaveKey o ) {
		final int baseCompare = super.compareTo(o);
		if (baseCompare != 0) {
			return baseCompare;
		}
		if (o instanceof GeoWaveInputKey) {
			final GeoWaveInputKey other = (GeoWaveInputKey) o;
			return WritableComparator.compareBytes(
					dataId.getBytes(),
					0,
					dataId.getBytes().length,
					other.dataId.getBytes(),
					0,
					other.dataId.getBytes().length);
		}
		return 1;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = (prime * result) + ((adapterId == null) ? 0 : adapterId.hashCode());
		result = (prime * result) + ((dataId == null) ? 0 : dataId.hashCode());
		return result;
	}

	@Override
	public boolean equals(
			final Object obj ) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		final GeoWaveInputKey other = (GeoWaveInputKey) obj;
		if (adapterId == null) {
			if (other.adapterId != null) {
				return false;
			}
		}
		else if (!adapterId.equals(other.adapterId)) {
			return false;
		}
		if (dataId == null) {
			if (other.dataId != null) {
				return false;
			}
		}
		else if (!dataId.equals(other.dataId)) {
			return false;
		}
		return true;
	}

}
