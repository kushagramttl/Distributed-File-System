package client;

enum Operation {
  UPLOAD("UPLOAD"),
  GET("GET"),
  DELETE("DELETE"),
  UPDATE("UPDATE");

  private final String name;

  Operation(String name) {
    this.name = name;
  }

  public boolean equalsName(String otherName) {
    return name.equals(otherName);
  }

  public String toString() {
    return this.name;
  }
}
