package org.pentaho.di.streaming.www.cache;

public class TimedNumberedRow {
  private long id;
  private long time;
  private Object[] row;

  public TimedNumberedRow() {
    time = System.currentTimeMillis();
  }

  /**
   * @param id
   * @param time
   * @param row
   */
  public TimedNumberedRow( long id, long time, Object[] row ) {
    this.id = id;
    this.time = time;
    this.row = row;
  }

  /**
   * @param id
   * @param row
   */
  public TimedNumberedRow( long id, Object[] row ) {
    this();
    this.id = id;
    this.row = row;
  }

  /**
   * @return the id
   */
  public long getId() {
    return id;
  }

  /**
   * @param id the id to set
   */
  public void setId( long id ) {
    this.id = id;
  }

  /**
   * @return the time
   */
  public long getTime() {
    return time;
  }

  /**
   * @param time the time to set
   */
  public void setTime( long time ) {
    this.time = time;
  }

  /**
   * @return the row
   */
  public Object[] getRow() {
    return row;
  }

  /**
   * @param row the row to set
   */
  public void setRow( Object[] row ) {
    this.row = row;
  }
}
