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
package org.apache.drill.exec.store.parquet;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator.Feature;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.SchemaPath.De;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.store.TimedRunnable;
import org.apache.drill.exec.store.dfs.DrillPathFilter;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.metadata.BlockMetaData;
import parquet.hadoop.metadata.ColumnChunkMetaData;
import parquet.hadoop.metadata.ParquetMetadata;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class Metadata {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Metadata.class);
  public static final String FILENAME = ".drill.parquet_metadata";

  public static void createMeta(FileSystem fs, String path) throws IOException {
    FileStatus fileStatus = fs.getFileStatus(new Path(path));
    Preconditions.checkArgument(fileStatus.isDirectory(), "Can only create metadata file for directories");
    createMetaFilesRecursively(fs, path);
  }

  private static List<FileMetaColumnValuePair> createMetaFilesRecursively(final FileSystem fs, final String path) throws IOException {
    List<FileMetaColumnValuePair> metaPairs = Lists.newArrayList();
    Path p = new Path(path);
    FileStatus fileStatus = fs.getFileStatus(p);
    assert fileStatus.isDirectory() : "Expected directory";

    final List<FileStatus> childFiles = Lists.newArrayList();

    for (final FileStatus file : fs.listStatus(p, new DrillPathFilter())) {
      if (file.isDirectory()) {
        metaPairs.addAll(createMetaFilesRecursively(fs, file.getPath().toString()));
      } else {
        childFiles.add(file);
      }
    }
    if (childFiles.size() > 0) {
      metaPairs.addAll(getMetaPairs(fs, childFiles));
    }
    ParquetTableMetadata parquetTableMetadata = fetchParquetTableMetadata(metaPairs);
    writeFile(parquetTableMetadata, fs, new Path(p, FILENAME));
    return metaPairs;
  }

  public static List<String> getFilesFromMetadata(final DrillConfig config, final FileSystem fs, String path) throws IOException {
    ParquetTableMetadata tableMetadata = readParquetTableMetadataFromFile(config, fs, path);
    List<String> files = Lists.newArrayList();
    for (ParquetFileMetadata fileMetadata : tableMetadata.files) {
      files.add(fileMetadata.path);
    }
    return files;
  }

  public static ParquetTableMetadata fetchParquetTableMetadata(FileSystem fs, List<FileStatus> fileStatuses) throws IOException {
    List<FileMetaColumnValuePair> metaPairs = getMetaPairs(fs, fileStatuses);
    return fetchParquetTableMetadata(metaPairs);
  }

  private static List<FileMetaColumnValuePair> getMetaPairs(FileSystem fs, List<FileStatus> fileStatuses) throws IOException {
    List<TimedRunnable<FileMetaColumnValuePair>> gatherers = Lists.newArrayList();
    for (FileStatus file : fileStatuses) {
      gatherers.add(new MetadataGatherer(fs, file));
    }

    List<FileMetaColumnValuePair> metaPairs = Lists.newArrayList();
    metaPairs.addAll(TimedRunnable.run("Fetch parquet metadata", logger, gatherers, 16));
    return metaPairs;
  }

  private static ParquetTableMetadata fetchParquetTableMetadata(List<FileMetaColumnValuePair> metaPairs) {
    Map<SchemaPath,Long> columnValueCounts = Maps.newHashMap();
    List<ParquetFileMetadata> fileMetadataList = Lists.newArrayList();
    long rowCount = 0;
    for (FileMetaColumnValuePair f : metaPairs) {
      for (ColumnWithValueCount column : f.columnValueCounts) {
        SchemaPath schemaPath = column.column;
        Long count = columnValueCounts.get(schemaPath);
        if (count == null) {
          columnValueCounts.put(schemaPath, column.valueCount);
        } else {
          long newCount = (count < 0 || column.valueCount < 0) ? GroupScan.NO_COLUMN_STATS : count + column.valueCount;
          columnValueCounts.put(schemaPath, newCount);
        }
      }
      fileMetadataList.add(f.parquetFileMetadata);
      rowCount += f.parquetFileMetadata.rowCount;
    }
    List<ColumnWithValueCount> columnsWithValueCounts = Lists.newArrayList();
    for (SchemaPath schemaPath : columnValueCounts.keySet()) {
      ColumnWithValueCount cvc = new ColumnWithValueCount(schemaPath, columnValueCounts.get(schemaPath));
      columnsWithValueCounts.add(cvc);
    }

    ParquetTableMetadata parquetTableMetadata = new ParquetTableMetadata(rowCount, columnsWithValueCounts, fileMetadataList);
    return parquetTableMetadata;
  }

  private static class MetadataGatherer extends TimedRunnable<FileMetaColumnValuePair> {

    private FileSystem fs;
    private FileStatus fileStatus;

    public MetadataGatherer(FileSystem fs, FileStatus fileStatus) {
      this.fs = fs;
      this.fileStatus = fileStatus;
    }

    @Override
    protected FileMetaColumnValuePair runInner() throws Exception {
      return fetchParquetFileMetadata(fs, fileStatus);
    }

    @Override
    protected IOException convertToIOException(Exception e) {
      return null;
    }
  }

  private static FileMetaColumnValuePair fetchParquetFileMetadata(FileSystem fs, FileStatus file) throws IOException {
    ParquetMetadata metadata = ParquetFileReader.readFooter(fs.getConf(), file);
    long rowCount = 0;
    Map<SchemaPath,Long> columnValueCounts = Maps.newHashMap();

    for (BlockMetaData rowGroup : metadata.getBlocks()) {
      for (ColumnChunkMetaData col : rowGroup.getColumns()) {
        SchemaPath path = SchemaPath.getSimplePath(col.getPath().toString().replace("[", "").replace("]", "").toLowerCase());

        long previousCount = 0;
        long currentCount = 0;

        if (! columnValueCounts.containsKey(path)) {
          // create an entry for this column
          columnValueCounts.put(path, previousCount /* initialize to 0 */);
        } else {
          previousCount = columnValueCounts.get(path);
        }

        boolean statsAvail = (col.getStatistics() != null && !col.getStatistics().isEmpty());

        if (statsAvail && previousCount != GroupScan.NO_COLUMN_STATS) {
          currentCount = col.getValueCount() - col.getStatistics().getNumNulls(); // only count non-nulls
          columnValueCounts.put(path, previousCount + currentCount);
        } else {
          // even if 1 chunk does not have stats, we cannot rely on the value count for this column
          columnValueCounts.put(path, GroupScan.NO_COLUMN_STATS);
        }

      }

      rowCount += rowGroup.getRowCount();
    }
    List<BlockLocation> blockLocations = Arrays.asList(fs.getFileBlockLocations(file, 0, file.getLen()));
    String path = Path.getPathWithoutSchemeAndAuthority(file.getPath()).toString();
    ByteArrayDataOutput out = ByteStreams.newDataOutput();
    file.write(out);
    byte[] fileStatus = out.toByteArray();
    List<ColumnWithValueCount> columnsWithValueCounts = Lists.newArrayList();
    for (SchemaPath schemaPath : columnValueCounts.keySet()) {
      ColumnWithValueCount cvc = new ColumnWithValueCount(schemaPath, columnValueCounts.get(schemaPath));
      columnsWithValueCounts.add(cvc);
    }
    ParquetFileMetadata parquetFileMetadata = new ParquetFileMetadata(path, fileStatus, blockLocations, rowCount);
    FileMetaColumnValuePair fileMetaColumnValuePair = new FileMetaColumnValuePair(parquetFileMetadata, columnsWithValueCounts);
    return fileMetaColumnValuePair;
  }

  private static void writeFile(ParquetTableMetadata parquetTableMetadata, FileSystem fs, Path p) throws IOException {
    JsonFactory jsonFactory = new JsonFactory();
    jsonFactory.configure(Feature.AUTO_CLOSE_TARGET, false);
    jsonFactory.configure(JsonParser.Feature.AUTO_CLOSE_SOURCE, false);
    ObjectMapper mapper = new ObjectMapper(jsonFactory);
    FSDataOutputStream os = fs.create(p);
    mapper.writerWithDefaultPrettyPrinter().writeValue(os, parquetTableMetadata);
    os.flush();
    os.close();
  }

  public static ParquetTableMetadata readParquetTableMetadataFromFile(DrillConfig config, FileSystem fs, String path) throws IOException {
    logger.debug("Read table metadat from file " + path);
    Path p = new Path(path);
    ObjectMapper mapper = new ObjectMapper();
    SimpleModule module = new SimpleModule();
    module.addDeserializer(SchemaPath.class, new De(config));
    mapper.registerModule(module);
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    FSDataInputStream is = fs.open(p);
    ParquetTableMetadata parquetTableMetadata = mapper.readValue(is, ParquetTableMetadata.class);
    return parquetTableMetadata;
  }

  public static class ParquetTableMetadata {
    @JsonProperty
    Long rowCount;
    @JsonProperty
    public List<ColumnWithValueCount> columnValueCounts;
    @JsonProperty
    List<ParquetFileMetadata> files;

    public ParquetTableMetadata() {
      super();
    }

    public ParquetTableMetadata(Long rowCount, List<ColumnWithValueCount> columnValueCounts, List<ParquetFileMetadata> files) {
      this.rowCount = rowCount;
      this.columnValueCounts = columnValueCounts;
      this.files = files;
    }
  }

  public static class FileMetaColumnValuePair {
    ParquetFileMetadata parquetFileMetadata;
    List<ColumnWithValueCount> columnValueCounts;

    public FileMetaColumnValuePair(ParquetFileMetadata parquetFileMetadata, List<ColumnWithValueCount> columnValueCounts) {
      this.parquetFileMetadata = parquetFileMetadata;
      this.columnValueCounts = columnValueCounts;
    }
  }

  public static class ParquetFileMetadata {
    @JsonProperty
    public String path;
    @JsonProperty
    public byte[] fileStatus;
    @JsonProperty
    public Long rowCount;
    @JsonProperty
    public List<BlockLocation> blockLocations;

    public ParquetFileMetadata() {
     super();
    }

    public ParquetFileMetadata(String path, byte[] fileStatus, List<BlockLocation> blockLocations, Long rowCount) {
      this.path = path;
      this.fileStatus = fileStatus;
      this.blockLocations = blockLocations;
      this.rowCount = rowCount;
    }

    @Override
    public String toString() {
      return String.format("path: %s blocks: %s", path, blockLocations);
    }
  }

  public static class ColumnWithValueCount {
    @JsonProperty
    public SchemaPath column;
    @JsonProperty
    public Long valueCount;

    public ColumnWithValueCount() {
      super();
    }

    public ColumnWithValueCount(SchemaPath column, Long valueCount) {
      this.column = column;
      this.valueCount = valueCount;
    }
  }
}
