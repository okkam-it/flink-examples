package it.okkam.flink.ckan.model;

public class CkanDatastoreSearchResponse extends CkanBaseResponse {
  private CkanDatastoreSearch result;

  public CkanDatastoreSearch getResult() {
    return result;
  }

  public void setResult(CkanDatastoreSearch result) {
    this.result = result;
  }
}
