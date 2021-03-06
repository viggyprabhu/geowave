package mil.nga.giat.geowave.analytics.kmeans.serial;

import java.util.ArrayList;
import java.util.List;

import mil.nga.giat.geowave.analytics.clustering.CentroidPairing;
import mil.nga.giat.geowave.analytics.kmeans.AssociationNotification;
import mil.nga.giat.geowave.analytics.kmeans.CentroidAssociationFn;
import mil.nga.giat.geowave.analytics.sample.SampleNotification;
import mil.nga.giat.geowave.analytics.sample.Sampler;
import mil.nga.giat.geowave.analytics.tools.AnalyticItemWrapper;
import mil.nga.giat.geowave.analytics.tools.AnalyticItemWrapperFactory;

import org.apache.commons.lang3.tuple.Pair;

public class KMeansParallelInitialize<T>
{
	private CentroidAssociationFn<T> centroidAssociationFn = new CentroidAssociationFn<T>();
	private double psi = 5.0;
	private final Sampler<T> sampler = new Sampler<T>();
	private AnalyticItemWrapperFactory<T> centroidFactory;
	private final AnalyticStats stats = new StatsMap();

	public CentroidAssociationFn<T> getCentroidAssociationFn() {
		return centroidAssociationFn;
	}

	public void setCentroidAssociationFn(
			final CentroidAssociationFn<T> centroidAssociationFn ) {
		this.centroidAssociationFn = centroidAssociationFn;
	}

	public double getPsi() {
		return psi;
	}

	public void setPsi(
			final double psi ) {
		this.psi = psi;
	}

	public Sampler<T> getSampler() {
		return sampler;
	}

	public AnalyticItemWrapperFactory<T> getCentroidFactory() {
		return centroidFactory;
	}

	public void setCentroidFactory(
			final AnalyticItemWrapperFactory<T> centroidFactory ) {
		this.centroidFactory = centroidFactory;
	}

	public AnalyticStats getStats() {
		return stats;
	}

	public Pair<List<CentroidPairing<T>>, List<AnalyticItemWrapper<T>>> runLocal(
			final Iterable<AnalyticItemWrapper<T>> pointSet ) {

		stats.reset();

		final List<AnalyticItemWrapper<T>> sampleSet = new ArrayList<AnalyticItemWrapper<T>>();
		sampleSet.add(pointSet.iterator().next());

		final List<CentroidPairing<T>> pairingSet = new ArrayList<CentroidPairing<T>>();

		final AssociationNotification<T> assocFn = new AssociationNotification<T>() {
			@Override
			public void notify(
					final CentroidPairing<T> pairing ) {
				pairingSet.add(pairing);
				pairing.getCentroid().incrementAssociationCount(
						1);
			}
		};
		// combine to get pairing?
		double normalizingConstant = centroidAssociationFn.compute(
				pointSet,
				sampleSet,
				assocFn);
		stats.notify(
				AnalyticStats.StatValue.COST,
				normalizingConstant);

		final int logPsi = Math.max(
				1,
				(int) (Math.log(psi) / Math.log(2)));
		for (int i = 0; i < logPsi; i++) {
			sampler.sample(
					pairingSet,
					new SampleNotification<T>() {
						@Override
						public void notify(
								final T item,
								final boolean partial ) {
							sampleSet.add(centroidFactory.create(item));
						}
					},
					normalizingConstant);
			pairingSet.clear();
			for (final AnalyticItemWrapper<T> centroid : sampleSet) {
				centroid.resetAssociatonCount();
			}
			normalizingConstant = centroidAssociationFn.compute(
					pointSet,
					sampleSet,
					assocFn);
			stats.notify(
					AnalyticStats.StatValue.COST,
					normalizingConstant);
		}
		return Pair.of(
				pairingSet,
				sampleSet);
	}

}
