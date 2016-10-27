package org.pentaho.di.streaming.www.cache;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class StreamingCache {
  private static StreamingCache streamingCache;

  protected Map<String, StreamingCacheEntry> cache;
  protected Map<String, AtomicLong> sequences;

  private StreamingCache() {
    cache = new HashMap<String, StreamingCacheEntry>();
    sequences = new HashMap<String, AtomicLong>();
  }

  public static StreamingCache getInstance() {
    if ( streamingCache == null ) {
      streamingCache = new StreamingCache();
    }
    return streamingCache;
  }

  public StreamingCacheEntry get( String serviceName ) {
    return cache.get( serviceName );
  }

  public void put( String serviceName, StreamingCacheEntry entry ) {
    cache.put( serviceName, entry );
  }

  public Map<String, StreamingCacheEntry> getCache() {
    return cache;
  }

  public long nextValue( String serviceName ) {
    AtomicLong atomicLong = sequences.get( serviceName );
    if ( atomicLong == null ) {
      atomicLong = new AtomicLong( 0L );
      sequences.put( serviceName, atomicLong );
    }
    return atomicLong.incrementAndGet();
  }
}
