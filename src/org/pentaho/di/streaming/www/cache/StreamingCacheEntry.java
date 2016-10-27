package org.pentaho.di.streaming.www.cache;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.pentaho.di.core.row.RowMetaInterface;

public class StreamingCacheEntry {
  private RowMetaInterface rowMeta;
  private List<TimedNumberedRow> rowData;

  public StreamingCacheEntry() {
  }

  /**
   * @param rowMeta
   * @param rowData
   */
  public StreamingCacheEntry( RowMetaInterface rowMeta, List<TimedNumberedRow> rowData ) {
    this();
    this.rowMeta = rowMeta;
    this.rowData = rowData;
  }

  /**
   * @return the rowMeta
   */
  public RowMetaInterface getRowMeta() {
    return rowMeta;
  }

  /**
   * @param rowMeta the rowMeta to set
   */
  public void setRowMeta( RowMetaInterface rowMeta ) {
    this.rowMeta = rowMeta;
  }

  /**
   * @return the rowData
   */
  public List<TimedNumberedRow> getRowData() {
    return rowData;
  }

  /**
   * @param rowData the rowData to set
   */
  public void setRowData( List<TimedNumberedRow> rowData ) {
    this.rowData = rowData;
  }

  /**
   * Find rows in the cache
   * 
   * @param lastSize
   * @param lastPeriod
   * @param fromId
   * @param toId
   * @param newSize
   * @param maxWait
   * @param now 
   * @return
   */
  public List<TimedNumberedRow> findRows( int lastSize, int lastPeriod, long fromId, long toId, int newSize, int maxWait, long now ) {
    synchronized ( rowData ) {
      Iterator<TimedNumberedRow> iterator = rowData.iterator();

      List<TimedNumberedRow> rows = new ArrayList<TimedNumberedRow>();

      if ( fromId > 0 && toId > 0 ) {
        while ( iterator.hasNext() ) {
          TimedNumberedRow row = iterator.next();
          if ( row.getId() >= fromId && row.getId() <= toId ) {
            rows.add( row );
          }
        }
      } else if ( lastSize > 0 || lastPeriod > 0 ) {
        int size = rowData.size();
        TimedNumberedRow lastRow = rowData.get( size - 1 );
        long lastId = lastRow.getId();
        long lastTime = lastRow.getTime();
        if ( fromId > 0 && ( newSize > 0 || maxWait > 0 ) ) {
          long cutOffId = fromId + newSize;
          long startId = lastSize <= 0 ? -1 : lastId - lastSize;
          long startTime = lastPeriod <= 0 ? -1 : lastTime - lastPeriod * 1000;

          if ( newSize > 0 && lastId - fromId < newSize ) {
            return null;
          }

          while ( iterator.hasNext() ) {
            TimedNumberedRow row = iterator.next();
            if ( ( ( startId > 0 && row.getId() > startId )
              || ( ( startTime > 0 ) && row.getTime() > startTime ) ) && ( row.getId() < cutOffId ) ) {
              rows.add( row );
            }
          }

        } else {
          while ( iterator.hasNext() ) {
            TimedNumberedRow row = iterator.next();
            if ( ( lastSize > 0 && row.getId() > lastId - lastSize )
              || ( ( lastPeriod > 0 ) && row.getTime() > lastTime - lastPeriod * 1000 ) ) {
              rows.add( row );
            }
          }
        }
      } else {
        // Simply return all rows
        rows.addAll( rowData );
      }

      return rows;
    }
  }
}
