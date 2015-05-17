package mil.nga.giat.geowave.adapter.raster.adapter.merge;

import java.io.IOException;
import java.util.Map;

import mil.nga.giat.geowave.adapter.raster.adapter.RasterTile;
import mil.nga.giat.geowave.core.iface.combiner.ICombiner;
import mil.nga.giat.geowave.core.iface.combiner.IIteratorEnvironment;
import mil.nga.giat.geowave.core.iface.combiner.ISortedKeyValueIterator;
import mil.nga.giat.geowave.core.iface.field.IKey;
import mil.nga.giat.geowave.core.iface.field.IValue;
import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.core.index.Persistable;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.store.DataStoreFactory;

public class RasterTileCombiner implements ICombiner 
{
	//TODO #238 This is hardcoded as against getting it from MergingCombiner.COLUMN_OPTIONS 
	//     to solve direct dependency to Accumulo-core
	public static final String COLUMNS_KEY = "columns";
	
	private final RasterTileCombinerHelper<Persistable> helper = new RasterTileCombinerHelper<Persistable>();

	protected Mergeable getMergeable(
			final IKey key,
			final byte[] binary ) {
		final RasterTile mergeable = PersistenceUtils.classFactory(
				RasterTile.class.getName(),
				RasterTile.class);

		if (mergeable != null) {
			mergeable.fromBinary(binary);
		}
		return helper.transform(
				key,
				mergeable);
	}

	protected byte[] getBinary(
			final Mergeable mergeable ) {
		return mergeable.toBinary();
	}

	public void init(
			final ISortedKeyValueIterator<IKey, IValue> source,
			final Map<String, String> options,
			final IIteratorEnvironment env )
			throws IOException {
		DataStoreFactory.getFactory().getMergedCombiner().init(
				source,
				options,
				env);
		helper.init(options);
	}
}
