/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package parquet.FileReader;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.net.InetAddress;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.RootAllocatorFactory;
import org.apache.drill.exec.store.dfs.DrillPathFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;


public class ParquetTableReader {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ParquetTableReader.class);

  private final Configuration dfsConfig = new Configuration();
  private final FileSystem fs;
  private final List<ParquetMetadata> parquetMetadata = Lists.newArrayList();
  private final List<FileStatus> fileStatuses = Lists.newArrayList();
  private final List<RowGroupInfo> rowGroupInfos = Lists.newArrayList();

  private String pathName;

  ParquetTableReader(String pathName) throws IOException {
    fs = FileSystem.get(dfsConfig);
    this.pathName = pathName;
  }

  /*
    Initialize thread pool.
    Read FileStatuses -- Get Metadata
    Read Footers --
    Read RowGroups --
    Read ColumnHeaders --
    Setup ColumnReaderRunnables and queue them up
    Run
   */

  public void init(String whichOne){
    return;
  }

  private List<FileStatus> getFileStatuses(FileStatus fileStatus) throws IOException {
    if (fileStatus.isDirectory()) {
      for (FileStatus child : fs.listStatus(fileStatus.getPath(), new DrillPathFilter())) {
        //fileStatuses.addAll(getFileStatuses(child));
        getFileStatuses(child);
      }
    } else {
      fileStatuses.add(fileStatus);
    }
    return fileStatuses;
  }

  private void getMetadata() throws IOException {
    // get All file Statuses
    // read Footers
    // compute affinities
    final List<RowGroupInfo> rowGroupsInfo = Lists.newArrayList();
    getFileStatuses(fs.getFileStatus(new Path(this.pathName)));
    for(FileStatus fileStatus : fileStatuses){
      ParquetMetadata parquetMetadata = ParquetFileReader.readFooter(dfsConfig, fileStatus);
      this.parquetMetadata.add(parquetMetadata);
      List<BlockMetaData> blocks = parquetMetadata.getBlocks();
      int rowGroupIndex = 0;
      for (BlockMetaData block : blocks) {
        RowGroupInfo rowGroupInfo = new RowGroupInfo();
        rowGroupInfo.index = ++rowGroupIndex;
        rowGroupInfo.filePath = fileStatus.getPath().toString();
        rowGroupInfo.fileStatus = fileStatus;
        rowGroupInfo.rowGroup = block;
        List<ColumnInfo> columns = getColumns(block);
        rowGroupInfo.columns = columns;
        long length = 0;
        for(ColumnInfo col : columns){
          length += col.totalSize;
        }
        Map<String, Float> hostAffinity =
            getHostAffinity(fileStatus, block.getStartingPos(), length);
        rowGroupInfo.hostAffinity = hostAffinity;
        String hostname = InetAddress.getLocalHost().getHostName();
        Float localAffinity = hostAffinity.get(hostname);
        if(localAffinity == null){
          // useful only for testing. Otherwise bogus.
          localAffinity = hostAffinity.get("localhost");
        }
        rowGroupInfo.localAffinity = localAffinity != null ? localAffinity.floatValue() : 0;
        this.rowGroupInfos.add(rowGroupInfo);
        try {
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
    return;
  }

  private List<ColumnInfo> getColumns(BlockMetaData block) {
    final List<ColumnChunkMetaData> parquetColumns = block.getColumns();
    final List<ColumnInfo> columns = Lists.newArrayList();
    for (ColumnChunkMetaData columnMetadata : parquetColumns) {
      ColumnInfo colInfo = new ColumnInfo();
      colInfo.columnName = columnMetadata.getPath().toDotString();
      colInfo.startPos = columnMetadata.getStartingPos();
      colInfo.totalSize = columnMetadata.getTotalSize();
      colInfo.columnMetadata = columnMetadata;
      columns.add(colInfo);
    }
    return columns;
  }

  private Map<String, Float> getHostAffinity(FileStatus fileStatus, long start, long length)
      throws IOException {
    BlockLocation[] blockLocations = fs.getFileBlockLocations(fileStatus, start, length);
    Map<String, Float> hostAffinityMap = Maps.newHashMap();
    for (BlockLocation blockLocation : blockLocations) {
      for (String host : blockLocation.getHosts()) {
        Float currentAffinity = hostAffinityMap.get(host);
        float blockStart = blockLocation.getOffset();
        float blockEnd = blockStart + blockLocation.getLength();
        float rowGroupEnd = start + length;
        Float newAffinity = (blockLocation.getLength() - (blockStart < start ? start - blockStart : 0) -
            (blockEnd > rowGroupEnd ? blockEnd - rowGroupEnd : 0)) / length;
        if (currentAffinity != null) {
          hostAffinityMap.put(host, currentAffinity + newAffinity);
        } else {
          hostAffinityMap.put(host, newAffinity);
        }
      }
    }
    return hostAffinityMap;
  }

  private class RowGroupInfo {
    int index;
    String filePath;
    FileStatus fileStatus;
    float localAffinity;
    BlockMetaData rowGroup;
    List<ColumnInfo> columns;
    Map<String, Float> hostAffinity;
  }


  public  class ColumnInfo {
    String columnName;
    long startPos;
    long totalSize;
    ColumnChunkMetaData columnMetadata;
  }

  //TODO: Move this to an Executor class
  private static <T> List<T> runAllInParallel(int parallelism, List<Callable<T>> toRun) throws
      ExecutionException {
    ExecutorService threadPool = Executors.newFixedThreadPool(parallelism);

    try {
      ArrayList futures = new ArrayList();
      Iterator result = toRun.iterator();

      while(result.hasNext()) {
        Callable i$ = (Callable)result.next();
        futures.add(threadPool.submit(i$));
      }

      ArrayList result1 = new ArrayList(toRun.size());
      Iterator i$1 = futures.iterator();

      while(i$1.hasNext()) {
        Future future = (Future)i$1.next();

        try {
          result1.add(future.get());
        } catch (InterruptedException var11) {
          throw new RuntimeException("The thread was interrupted", var11);
        }
      }

      ArrayList i$2 = result1;
      return i$2;
    } finally {
      threadPool.shutdownNow();
    }
  }

  public static void main(String[] args) {
    if (args.length != 4 && args.length != 5 && args.length != 6 && args.length != 7) {
      System.out.println("Usage: ParquetTableReader block|page filepath parallelism maxData [shuffle [buffer_size [enable_hints]]]");
      return;
    }
    String whichOne = args[0];
    String fileName = args[1];
    int parallelism = new  Integer(args[2]).intValue();
    int bufsize = 8 * 1024 * 1024;
    boolean enableHints = true;
    boolean shuffle = true;
    long maxData = new  Long(args[3]).longValue();

    if(args.length == 5){
      shuffle = args[4].equalsIgnoreCase("true")?true:false;
    }
    if(args.length == 6){
      bufsize = new  Integer(args[4]).intValue();
    }

    if(args.length == 7){
      enableHints = args[5].equalsIgnoreCase("true")?true:false;
    }

    ParquetTableReader reader = null;
    List<Callable<Object>> runnables = Lists.newArrayList();
    final DrillConfig config = DrillConfig.create();
    final BufferAllocator allocator = RootAllocatorFactory.newRoot(config);
    final Configuration dfsConfig = new Configuration();
    logger.info("Parquet Table Reader - Beginning new run to read {}.", fileName);
    try {
      //Ready
      reader = new ParquetTableReader(fileName);
      reader.getMetadata();
      long totalDataQueued = 0;
      //Set
      int numRowGroups = 0; // num of row groups read
      int numColumnsRead = 0; // total num of columns read;
      long totalColumnData = 0; // same as totalDataQueued?? (bytes)
      long avgSplitSize = 0; // avg size of a column being read (bytes)
      long elapsedTime = 0;
      double averageTimePerColumn = 0;
      double averageReadSpeed = 0; // (totalDataQueued * 1000000 )/ (elapsedTime * 1024 * 1024) -  MiB per second

      for (RowGroupInfo rg : reader.rowGroupInfos) {
        // Create a new Runnable for every column for every row group, if the row group
        // has local affinity of 1.0. Otherwise log the info that the row group was skipped.
        if (rg.localAffinity >= 0.99) {
          numRowGroups++;
          for (ColumnInfo columnInfo : rg.columns) {
            //if maxData specified  and is non-negative read only as much data as can be cached
            if(maxData > 0 && totalDataQueued + columnInfo.totalSize > maxData){
             break;
            }
            numColumnsRead++;
            RunnableReader runnable;
            if (whichOne.equalsIgnoreCase("page")) {
              runnable = new RunnableBlockReader(allocator, dfsConfig, rg.fileStatus, columnInfo, bufsize, enableHints);
            } else {
              runnable = new RunnablePageReader(allocator, dfsConfig, rg.fileStatus, columnInfo, bufsize, enableHints);
            }
            logger.info("[READING]\t{}\t{}\t{}\t{}\t{}", rg.filePath, "RowGroup-" + rg.index,
                columnInfo.columnName, columnInfo.startPos, columnInfo.totalSize);
            runnables.add(Executors.callable(runnable));
            totalDataQueued += columnInfo.totalSize;
            totalColumnData += columnInfo.totalSize;
          }
        } else {
          logger.info("[SKIPPING]\t{}\t{}", rg.filePath, "RowGroup-" + rg.index);
        }
      }

      if(shuffle){
        Collections.shuffle(runnables);
      }

      // Go
      Stopwatch stopwatch = Stopwatch.createStarted();
      reader.runAllInParallel(parallelism, runnables);
      elapsedTime = stopwatch.elapsed(TimeUnit.MICROSECONDS);

      avgSplitSize = totalColumnData/numColumnsRead;
      averageTimePerColumn = (elapsedTime)/(1.0 * numColumnsRead * 1000000 ) ;
      averageReadSpeed = (1.0 * totalColumnData * 1000000) / (elapsedTime * 1024 * 1024);
      StringBuilder SUMMARY;
      SUMMARY = new StringBuilder("SUMMARY:\n");
      SUMMARY.append("\t PATH             : ").append(fileName).append("\n");
      SUMMARY.append("\t THREADS          : ").append(parallelism).append("\n");
      SUMMARY.append("\t MAXDATA          : ").append(maxData).append(" (bytes)\n");
      SUMMARY.append("\t BUFFER_SIZE      : ").append(bufsize).append(" (bytes)\n");
      SUMMARY.append("\t FADVISE          : ").append(enableHints?"Enabled":"Disabled").append("\n");
      SUMMARY.append("\t TOTAL ROW_GROUPS : ").append(reader.rowGroupInfos.size()).append("\n");
      SUMMARY.append("\t ROW_GROUPS READ  : ").append(numRowGroups).append("\n");
      SUMMARY.append("\t COLUMNS READ     : ").append(numColumnsRead).append("\n");
      SUMMARY.append("\t AVG SPLIT SIZE   : ").append(avgSplitSize).append(" (bytes)\n");
      SUMMARY.append("\t AVG SPLIT SIZE   : ").append(avgSplitSize).append(" (bytes)\n");
      SUMMARY.append("\t AVG SPLIT TIME   : ").append(averageTimePerColumn).append(" (seconds)\n");
      SUMMARY.append("\t TOTAL DATA READ  : ").append(totalDataQueued).append(" (bytes)\n");
      SUMMARY.append("\t TOTAL TIME       : ").append(elapsedTime/1000000.0).append(" ( seconds)\n");
      SUMMARY.append("\t AVG READ SPEED   : ").append(averageReadSpeed).append(" (MiB per second)\n");

      logger.info(SUMMARY.toString());
      System.out.println(SUMMARY.toString());

    } catch (IOException e) {
      e.printStackTrace();
      return;
    } catch (ExecutionException e) {
      e.printStackTrace();
    }
    return;
  }
}
