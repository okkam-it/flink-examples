package it.okkam.flink.ckan;

import java.io.Serializable;

/**
 * This interface is used by the {@link CkanRestInputFormat} to compute the list of parallel query to run (i.e. splits).
 * Each query will be parameterized using a row of the matrix provided by each {@link CkanParameterValuesProvider}
 * implementation.
 */
public interface CkanParameterValuesProvider {

	/** Returns the necessary parameters array to use for query in parallel a table. */
	Serializable[][] getParameterValues();
}
