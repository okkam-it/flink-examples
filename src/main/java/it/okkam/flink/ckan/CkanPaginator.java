package it.okkam.flink.ckan;

import java.io.Serializable;

public class CkanPaginator implements CkanParameterValuesProvider {
  private final int fetchSize;
  private final int rowCount;

  public CkanPaginator(int rowCount, int fetchSize) {
    this.rowCount = rowCount;
    this.fetchSize = fetchSize;
  }

  @Override
  public Serializable[][] getParameterValues() {
    final int numSplit = (int) (Math.ceil((double) rowCount / fetchSize));
    final Serializable[][] parameters = new Serializable[numSplit][1];
    for (int i = 0, from = 0; i < numSplit; i++, from += fetchSize) {
      parameters[i] = new Serializable[] { from };
    }
    return parameters;
  }

}
