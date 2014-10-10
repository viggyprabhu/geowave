package mil.nga.giat.geowave.vector.query;

import java.util.Collection;

import mil.nga.giat.geowave.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.accumulo.TransformerWriter;
import mil.nga.giat.geowave.accumulo.VisibilityTransformationIterator;
import mil.nga.giat.geowave.accumulo.query.AccumuloRowIdsQuery;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.StringUtils;
import mil.nga.giat.geowave.store.CloseableIterator;
import mil.nga.giat.geowave.store.adapter.AdapterStore;
import mil.nga.giat.geowave.store.index.Index;

import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;

/**
 * Used to remove the transaction id from the visibility of data fields for
 * specific row IDS
 * 
 */
public class TransformingVisibilityQuery extends
		AccumuloRowIdsQuery
{

	private final VisibilityTransformationIterator transformIterator;

	public TransformingVisibilityQuery(
			final VisibilityTransformationIterator transformIterator,
			final Index index,
			final Collection<ByteArrayId> rows,
			final String[] authorizations) {
		super(
				index,
				rows,
				authorizations);
		this.transformIterator = transformIterator;
	}

	@Override
	protected void addScanIteratorSettings(
			ScannerBase scanner ) {
		super.addScanIteratorSettings(scanner);
		transformIterator.addScanIteratorSettings(
				scanner,
				this.getAdditionalAuthorizations(),
				true);
	}

	public void run(
			final AccumuloOperations accumuloOperations,
			final AdapterStore adapterStore,
			final Integer limit )
			throws TableNotFoundException {

	}

	public CloseableIterator<Boolean> query(
			final AccumuloOperations accumuloOperations,
			final AdapterStore adapterStore,
			final Integer limit ) {
		final ScannerBase scanner = getScanner(
				accumuloOperations,
				limit);
		addScanIteratorSettings(scanner);
		final String tableName = StringUtils.stringFromBinary(index.getId().getBytes());
		TransformerWriter writer = new TransformerWriter(
				scanner,
				tableName,
				accumuloOperations);
		writer.transform();
		scanner.close();

		return new CloseableIterator.Empty<Boolean>();
	}
}
