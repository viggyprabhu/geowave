/**
 * 
 */
package mil.nga.giat.geowave.core.ingest.local;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.log4j.Logger;

/**
 * @author viggy
 * Functionality similar to <code> LocalPluginFileVisitor </code> 
 */
public class LocalPluginHBaseFileVisitor<P extends LocalPluginBase, R> implements
		FileVisitor<Path>
{

	private final static Logger LOGGER = Logger.getLogger(LocalPluginHBaseFileVisitor.class);

	private class PluginVisitor
	{
		private final Pattern pattern;
		private final String typeName;
		private final P localPluginBase;

		public PluginVisitor(
				final P localPluginBase,
				final String typeName,
				final String[] userExtensions ) {
			final String[] combinedExtensions = ArrayUtils.addAll(
					localPluginBase.getFileExtensionFilters(),
					userExtensions);
			if ((combinedExtensions != null) && (combinedExtensions.length > 0)) {
				final String[] lowerCaseExtensions = new String[combinedExtensions.length];
				for (int i = 0; i < combinedExtensions.length; i++) {
					lowerCaseExtensions[i] = combinedExtensions[i].toLowerCase();
				}
				final String extStr = String.format(
						"([^\\s]+(\\.(?i)(%s))$)",
						StringUtils.join(
								lowerCaseExtensions,
								"|"));
				pattern = Pattern.compile(extStr);
			}
			else {
				pattern = null;
			}
			this.localPluginBase = localPluginBase;
			this.typeName = typeName;
		}

		public boolean supportsFile(
				final File file ) {
			if ((pattern != null) && !pattern.matcher(
					file.getName().toLowerCase()).matches()) {
				return false;
			}
			else if (!localPluginBase.supportsFile(file)) {
				return false;
			}
			return true;
		}
	}

	private final AbstractLocalHBaseFileDriver<P, R> driver;
	private final List<PluginVisitor> pluginVisitors;
	private final R runData;

	public LocalPluginHBaseFileVisitor(
			final Map<String, P> localPlugins,
			final AbstractLocalHBaseFileDriver<P, R> driver,
			final R runData,
			final String[] userExtensions ) {
		pluginVisitors = new ArrayList<PluginVisitor>(
				localPlugins.size());
		for (final Entry<String, P> localPluginBase : localPlugins.entrySet()) {
			pluginVisitors.add(new PluginVisitor(
					localPluginBase.getValue(),
					localPluginBase.getKey(),
					userExtensions));
		}
		this.driver = driver;
		this.runData = runData;
	}

	@Override
	public FileVisitResult postVisitDirectory(
			final Path path,
			final IOException e )
			throws IOException {
		return FileVisitResult.CONTINUE;
	}

	@Override
	public FileVisitResult preVisitDirectory(
			final Path path,
			final BasicFileAttributes bfa )
			throws IOException {
		return FileVisitResult.CONTINUE;
	}

	@Override
	public FileVisitResult visitFile(
			final Path path,
			final BasicFileAttributes bfa )
			throws IOException {
		final File file = path.toFile();
		for (final PluginVisitor visitor : pluginVisitors) {
			if (visitor.supportsFile(file)) {
				driver.processFile(
						file,
						visitor.typeName,
						visitor.localPluginBase,
						runData);
			}
		}
		return FileVisitResult.CONTINUE;
	}

	@Override
	public FileVisitResult visitFileFailed(
			final Path path,
			final IOException bfa )
			throws IOException {
		LOGGER.error("Cannot visit path: " + path);
		return FileVisitResult.CONTINUE;
	}

}
