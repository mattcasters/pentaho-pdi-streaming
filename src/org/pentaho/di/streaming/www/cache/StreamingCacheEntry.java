package org.pentaho.di.streaming.www.cache;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;

public class StreamingCacheEntry {
  private RowMetaInterface rowMeta;
  private List<StreamingTimedNumberedRow> rowData;

  public StreamingCacheEntry() {
    rowMeta = new RowMeta();
    rowData = Collections.synchronizedList( new LinkedList<StreamingTimedNumberedRow>() );
  }

  /**
   * @param rowMeta
   * @param rowData
   */
  public StreamingCacheEntry( RowMetaInterface rowMeta, List<StreamingTimedNumberedRow> rowData ) {
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
  public List<StreamingTimedNumberedRow> findRows(LogChannelInterface log, int lastSize, int lastPeriod, long fromId, long toId, int newSize, int maxWait, long now ) {
    synchronized ( rowData ) {
      
      log.logBasic("Finding rows, args:  lastSize="+lastSize+" lastPeriod="+lastPeriod+" fromId="+fromId+" toId="+toId+" newSize="+newSize+" maxWait="+maxWait+" now="+now);
      
      List<StreamingTimedNumberedRow> rows = new ArrayList<StreamingTimedNumberedRow>();

      if ( fromId > 0 && toId > 0 ) {
        
        log.logBasic("Finding row range by ID fromId="+fromId+" toId="+toId+" buffer size : "+rowData.size());
        
        Iterator<StreamingTimedNumberedRow> iterator = rowData.iterator();
        while ( iterator.hasNext() ) {
          StreamingTimedNumberedRow row = iterator.next();
          if ( row.getId() >= fromId && row.getId() <= toId ) {
            rows.add( row );
          }
        }
      } else if ( lastSize > 0 || lastPeriod > 0 ) {
        
        log.logBasic("Finding row range by last size or period  lastSize="+lastSize+" lastPeriod="+lastPeriod+" buffer size : "+rowData.size());
        
        Iterator<StreamingTimedNumberedRow> iterator = rowData.iterator();
        int size = rowData.size();
        StreamingTimedNumberedRow lastRow = rowData.get( size - 1 );
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
            StreamingTimedNumberedRow row = iterator.next();
            if ( ( ( startId > 0 && row.getId() > startId )
              || ( ( startTime > 0 ) && row.getTime() > startTime ) ) && ( row.getId() < cutOffId ) ) {
              rows.add( row );
            }
          }

        } else {
          while ( iterator.hasNext() ) {
            StreamingTimedNumberedRow row = iterator.next();
            if ( ( lastSize > 0 && row.getId() > lastId - lastSize )
              || ( ( lastPeriod > 0 ) && row.getTime() > lastTime - lastPeriod * 1000 ) ) {
              rows.add( row );
            }
          }
        }
      } else {
        log.logBasic("Finding all rows, buffer size : "+rowData.size());
        
        // Simply return all rows
        rows.addAll( rowData );
      }

      return rows;
    }
  }

  public synchronized void addRow(StreamingTimedNumberedRow row) {
    rowData.add( row );
  }

  public synchronized int size() {
    return rowData.size();
  }

  public synchronized void removeFirst() {
    rowData.remove(0);
  }

  public Iterator<StreamingTimedNumberedRow> getIterator() {
    return rowData.iterator();
  }

  public StreamingTimedNumberedRow getRow(int rowIndex) {
    return rowData.get(rowIndex);
  }
  
}
