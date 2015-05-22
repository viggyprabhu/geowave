package mil.nga.giat.geowave.adapter.raster.adapter.merge;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import mil.nga.giat.geowave.adapter.raster.RasterHelper;
import mil.nga.giat.geowave.adapter.raster.adapter.RasterTile;
import mil.nga.giat.geowave.core.iface.combiner.ICombiner;
import mil.nga.giat.geowave.core.iface.combiner.IIteratorEnvironment;
import mil.nga.giat.geowave.core.iface.combiner.ISortedKeyValueIterator;
import mil.nga.giat.geowave.core.iface.field.IColumnSet;
import mil.nga.giat.geowave.core.iface.field.IKVBuffer;
import mil.nga.giat.geowave.core.iface.field.IKey;
import mil.nga.giat.geowave.core.iface.field.IValue;
import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.core.index.Persistable;
import mil.nga.giat.geowave.core.index.PersistenceUtils;

public class RasterTileVisibilityCombiner implements
		ICombiner
{
	private final RasterTileCombinerHelper<Persistable> helper = new RasterTileCombinerHelper<Persistable>();
	private IColumnSet columns;

	protected void transformRange(
			final ISortedKeyValueIterator<IKey, IValue> input,
			final IKVBuffer output )
			throws IOException {
		if (input.hasTop() && columns.contains(input.getTopKey())) {
			RasterHelper.getMergingVisibilityCombiner().transformRange(
					input,
					output);
		}
		else {
			while (input.hasTop()) {
				output.append(
						input.getTopKey(),
						input.getTopValue());
				input.next();
			}
		}
	}

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
		RasterHelper.getMergingVisibilityCombiner().init(
				source,
				options,
				env);
		if (!options.containsKey(RasterTileCombiner.COLUMNS_KEY)) {
			throw new IllegalArgumentException(
					"Must specify " + RasterTileCombiner.COLUMNS_KEY + " option");
		}

		final String encodedColumns = options.get(RasterTileCombiner.COLUMNS_KEY);
		if (encodedColumns.length() == 0) {
			throw new IllegalArgumentException(
					"The " + RasterTileCombiner.COLUMNS_KEY + " must not be empty");
		}

		columns = RasterHelper.getNewColumnSet(Arrays.asList(encodedColumns.split(",")));
		helper.init(options);
	}

}
