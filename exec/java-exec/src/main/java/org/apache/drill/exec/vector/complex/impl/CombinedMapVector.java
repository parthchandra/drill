package org.apache.drill.exec.vector.complex.impl;

import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.MapVector;
import org.apache.drill.exec.vector.complex.reader.FieldReader;

/**
 * Decorator class to allow creating SingleMapReaderImpl 
 * from RecordBatchLoader
 */
public class CombinedMapVector extends MapVector {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CombinedMapVector.class);
  
  private RecordBatchLoader loader;
  
  public CombinedMapVector(RecordBatchLoader loader) {
    super("", null);
    this.loader = loader;
  }
  
  /**
   * Helper method to add ValueVectors from loader
   * to MapVector and filling up SingleMapReaderImpl
   * 
   */
  public void load() {
    for (VectorWrapper<?> vectorWrapper : loader) {
      ValueVector vv = vectorWrapper.getValueVector();
      String name = vv.getField().getLastName();
      try {
        super.put(name, vv);
      } catch (IllegalStateException e) {
        logger.warn("Most likely schema changed with additional columns: {}", name);
      }
    }
    // just to get readers into SingleMapReader
    FieldReader fr = this.getAccessor().getReader();
    for ( String name : fr) {
      fr.reader(name);
    }
  }
}
