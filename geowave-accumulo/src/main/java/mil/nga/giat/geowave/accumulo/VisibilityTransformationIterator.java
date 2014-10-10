package mil.nga.giat.geowave.accumulo;

import java.io.IOException;
import java.util.Map;

import mil.nga.giat.geowave.index.ByteArrayUtils;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.TransformingIterator;
import org.apache.hadoop.io.Text;

public class VisibilityTransformationIterator extends
		TransformingIterator
{

	private String transformingRegex;
	private String replacement;
	private boolean outputOriginal = false;
	protected static final String REGEX_ID = "transformingRegex";
	protected static final String REPLACEMENT_ID = "replacement";
	protected static final String OUTPUT_ORIG_ID = "outputOrig";

	public VisibilityTransformationIterator() {
		super();
	}
	
	public VisibilityTransformationIterator(
			final String transformingRegex,
			final String replacement ) {
		super();
		this.transformingRegex = transformingRegex;
		this.replacement = replacement;
	}

	public VisibilityTransformationIterator(
			final String transformingRegex,
			final String replacement,
			final boolean outputOriginal ) {
		super();
		this.transformingRegex = transformingRegex;
		this.replacement = replacement;
		this.outputOriginal = outputOriginal;
	}

	public void addScanIteratorSettings(
			final ScannerBase base,
			final String[] additionalAuthorizations ) {
		addScanIteratorSettings(
				base,
				additionalAuthorizations,
				transformingRegex,
				replacement,
				this.outputOriginal,
				this.getClass());
	}

	public void addScanIteratorSettings(
			final ScannerBase base,
			final String[] additionalAuthorizations,
			final boolean outputOriginal ) {
		addScanIteratorSettings(
				base,
				additionalAuthorizations,
				transformingRegex,
				replacement,
				outputOriginal,
				this.getClass());
	}

	public static void addScanIteratorSettings(
			final ScannerBase scanner,
			final String[] additionalAuthorizations,
			final String transformingRegex,
			final String replacement,
			final boolean outputOriginal,
			final Class<? extends SortedKeyValueIterator<Key, Value>> transformIteratorClass ) {
		final IteratorSetting iteratorSettings = new IteratorSetting(
				100,
				"GEOWAVE_TRANSFORM",
				transformIteratorClass);
		iteratorSettings.addOption(
				REGEX_ID,
				ByteArrayUtils.byteArrayToString(transformingRegex.getBytes()));
		iteratorSettings.addOption(
				REPLACEMENT_ID,
				ByteArrayUtils.byteArrayToString(replacement.getBytes()));
		iteratorSettings.addOption(
				OUTPUT_ORIG_ID,
				outputOriginal ? "1" : "0");
		if (additionalAuthorizations.length > 0) iteratorSettings.addOption(
				AUTH_OPT,
				authString(additionalAuthorizations));
		scanner.addScanIterator(iteratorSettings);
	}

	private static String authString(
			String[] authorizations ) {
		StringBuffer buffer = new StringBuffer();
		for (String authorization : authorizations) {
			if (buffer.length() > 0) buffer.append(',');
			buffer.append(authorization);
		}
		return buffer.toString();
	}

	@Override
	public void init(
			SortedKeyValueIterator<Key, Value> source,
			Map<String, String> options,
			IteratorEnvironment env )
			throws IOException {
		super.init(
				source,
				options,
				env);
		transformingRegex = new String(
				ByteArrayUtils.byteArrayFromString(options.get(REGEX_ID)));
		replacement = new String(
				ByteArrayUtils.byteArrayFromString(options.get(REPLACEMENT_ID)));
		outputOriginal = "1".equals(options.get(OUTPUT_ORIG_ID));
	}

	@Override
	protected PartialKey getKeyPrefix() {
		return PartialKey.ROW_COLFAM_COLQUAL;
	}

	@Override
	protected void transformRange(
			SortedKeyValueIterator<Key, Value> input,
			KVBuffer output )
			throws IOException {
		while (input.hasTop()) {
			Key originalKey = input.getTopKey();
			Value value = input.getTopValue();
			Text visibiltity = originalKey.getColumnVisibility();
			String newVisibility = visibiltity.toString().replaceFirst(
					transformingRegex,
					replacement);
			if (newVisibility.length() > 0) {
				char one = newVisibility.charAt(0);
				// strip off any ending options
				if (one == '&' || one == '|') newVisibility = newVisibility.substring(1);
			}
			byte[] row = originalKey.getRowData().toArray();
			byte[] cf = originalKey.getColumnFamilyData().toArray();
			byte[] cq = originalKey.getColumnQualifierData().toArray();
			long timestamp = originalKey.getTimestamp();
			byte[] cv = newVisibility.getBytes();
			Key newKey = new Key(
					row,
					0,
					row.length,
					cf,
					0,
					cf.length,
					cq,
					0,
					cq.length,
					cv,
					0,
					cv.length,
					timestamp + 1);
			newKey.setDeleted(originalKey.isDeleted());
			if (outputOriginal) {
				output.append(
						originalKey,
						value);
			}
			output.append(
					newKey,
					value);

			input.next();
		}
	}

}
