package it.okkam.flink.ckan.model;

public class CkanBaseResponse {

  private String help;
  private boolean success;
  private CkanError error;

  public CkanBaseResponse() {
  }

  /**
   * CkanBaseResponse constructor.
   * 
   * @param help
   *          the help
   * @param success
   *          true or false
   * @param error
   *          the error (if any)
   */
  public CkanBaseResponse(String help, boolean success, CkanError error) {
    this.help = help;
    this.success = success;
    this.error = error;
  }

  public String getHelp() {
    return help;
  }

  public void setHelp(String help) {
    this.help = help;
  }

  public boolean isSuccess() {
    return success;
  }

  public void setSuccess(boolean success) {
    this.success = success;
  }

  public CkanError getError() {
    return error;
  }

  public void setError(CkanError error) {
    this.error = error;
  }

  @Override
  public String toString() {
    return "CkanBaseResponse{error=" + error + ", success=" + success + ", help=" + help + '}';
  }
}
