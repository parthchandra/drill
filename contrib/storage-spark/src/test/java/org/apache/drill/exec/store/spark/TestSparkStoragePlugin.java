package org.apache.drill.exec.store.spark;

import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.inputformat.DrillRecordReader;
import org.apache.drill.exec.inputformat.DrillQueryInputFormat.DrillQueryInputSplit;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.TopLevelAllocator;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.ExecProtos.PlanFragment;
import org.apache.drill.exec.proto.UserBitShared.QueryType;
import org.apache.drill.exec.proto.UserProtos.QueryFragmentQuery;
import org.apache.drill.exec.proto.UserProtos.QueryPlanFragments;
import org.apache.drill.exec.record.ExtendedFragmentWritableBatch;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.WritableBatch;
import org.apache.drill.exec.rpc.DrillRpcFuture;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.RemoteServiceSet;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.IntVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VarCharVector;
import org.apache.drill.exec.vector.complex.fn.JsonWriter;
import org.apache.drill.exec.vector.complex.reader.FieldReader;
import org.apache.drill.exec.work.DataPushConnectionManager;
import org.apache.hadoop.mapreduce.InputSplit;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;

/**
 * End to End test to execute query from Drill and use Spark as StoragePlugin to produce the data 
 *
 */
public class TestSparkStoragePlugin {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestSparkStoragePlugin.class);
  private static BufferAllocator ALLOCATOR = new TopLevelAllocator();


  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
  }

  @Before
  public void setUp() throws Exception {
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testPushingSparkData() throws Exception {    
    String query = "select key, sum(`value`) from spark.`{ \"name\": \"sparkTbl\", \"numPartitions\" : 5}` group by key";
    logger.info("SparkQuery: " + query);
    DrillConfig drillConfig = DrillConfig.create();
    RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();

    try (Drillbit bit1 = new Drillbit(drillConfig, serviceSet);
        Drillbit bit2 = new Drillbit(drillConfig, serviceSet);
        final DrillClient client = new DrillClient(drillConfig, serviceSet.getCoordinator());) {
      bit1.run();
      bit2.run();
      client.connect();
      
      List<QueryResultBatch> results = client.runQuery(QueryType.SQL, "alter session set `planner.slice_target`=1");
      for(QueryResultBatch batch : results) {
        batch.release();
      }


      DrillRpcFuture<QueryPlanFragments> queryFragmentsFutures = client.planQuery(query);
      
      final QueryPlanFragments planFragments = queryFragmentsFutures.get();
      
      final List<InputSplit> splits = Lists.newArrayList();
      for (int i = 0; i < planFragments.getFragmentsCount(); i++) {
        PlanFragment fragment = planFragments.getFragments(i);
        if (fragment.getFragmentJson().toLowerCase().contains("-writer")) {
          splits.add(new DrillQueryInputSplit(planFragments, fragment));
        }
      }
      logger.info("Splits: " + splits.size());
     
      Runnable receivingThread = new Runnable() {

        @Override
        public void run() {
          List<DrillRecordReader> readers = Lists.newArrayList();
          try {
          for (InputSplit split : splits) {
            DrillRecordReader reader = new DrillRecordReader();
            reader.initialize(split, null);
            readers.add(reader);
          }

          for(DrillRecordReader reader : readers) {
            int i = 0;
            while ( reader.nextKeyValue() ) {
              FieldReader fr = reader.getCurrentValue();
              if ( i % 100 == 0 ) {
                System.out.println();
                for ( String name : fr ) {
                  System.out.print(name + ", ");
                }            
                System.out.println();
              }
              System.out.print("Row#: " + ++i + ", ");
              for ( String name : fr ) {
                FieldReader frChild = fr.reader(name);
                ByteArrayOutputStream stream = new ByteArrayOutputStream();
                JsonWriter jsonWriter = new JsonWriter(stream, true);
                jsonWriter.write(frChild);
                System.out.print(new String(stream.toByteArray(), Charsets.UTF_8) + ", ");
                stream.close();
               }
              System.out.println();
            }
            reader.close();
          }
          } catch(Exception t) {
            t.printStackTrace();
            fail(t.fillInStackTrace().getMessage());
          }
        }};

      Thread receivingT = new Thread(receivingThread);
      receivingT.start();
      
      List<Thread> threads = new ArrayList<Thread>();
      int k = 0;
      for ( int i = 0; i < planFragments.getFragmentsCount(); i++) {
        final PlanFragment fragment = planFragments.getFragments(i);
        System.out.println(fragment.getFragmentJson());
        if (!fragment.getFragmentJson().toLowerCase().contains("spark-sub-scan")) {
          continue;
        }
        final int threadNumber = k;
        k++;
        Runnable myThread = new Runnable() {
  
          PlanFragment planFragment = fragment;
          int number = threadNumber;
          @Override
          public void run() {
            WritableBatch writableBatch = getWritableBatch(number);

            DrillbitEndpoint assignedNode = planFragment.getAssignment();
            logger.info("DrillbitEndPoint: " + assignedNode);
            DrillClient threadClient = new DrillClient();
            try {
              threadClient.connect(assignedNode);
            } catch (RpcException e) {
              e.printStackTrace();
              fail(e.fillInStackTrace().getMessage());
            }

            QueryFragmentQuery queryFragment = QueryFragmentQuery.newBuilder().addAllFragments(planFragments.getFragmentsList()).
                setFragmentHandle(planFragment.getHandle()).build();
            // create fragmentrecordbatch
            ExtendedFragmentWritableBatch extFRB = new ExtendedFragmentWritableBatch
                (queryFragment, true, planFragments.getQueryId(), planFragment.getHandle().getMajorFragmentId(), planFragment.getHandle().getMinorFragmentId(), 
                    planFragment.getHandle().getMajorFragmentId(), planFragment.getHandle().getMinorFragmentId(), writableBatch);
            threadClient.submitDataPushRequest(extFRB);
            threadClient.close();
          }
        };
        threads.add(new Thread(myThread));
      }
      for ( int i = 0; i < k; i++ ) {
        threads.get(i).start();
      }
      for ( int i = 0; i < k; i++ ) {
        threads.get(i).join();
      }      
      receivingT.join();
      int entriesCount = DataPushConnectionManager.getInstance().getHeadersCount();
      logger.info("EntriesCount: " + entriesCount);
     } catch(Throwable t) {
      t.printStackTrace();
      fail(t.fillInStackTrace().getMessage());
    }
  }

  private WritableBatch getWritableBatch(int number) {
    List<ValueVector> vectorList = Lists.newArrayList();

    MaterializedField stringField = MaterializedField.create(SchemaPath.getSimplePath("key"),
        Types.required(TypeProtos.MinorType.VARCHAR));
    VarCharVector stringVector = (VarCharVector) TypeHelper.getNewVector(stringField, ALLOCATOR);
    MaterializedField intField = MaterializedField.create(SchemaPath.getSimplePath("value"),
        Types.required(TypeProtos.MinorType.INT));
    IntVector intVector = (IntVector) TypeHelper.getNewVector(intField, ALLOCATOR);
    AllocationHelper.allocate(stringVector, 6, 5);
    AllocationHelper.allocate(intVector, 6, 4);
    vectorList.add(stringVector);
    vectorList.add(intVector);

    stringVector.getMutator().setSafe(0, "ZERO".getBytes());
    intVector.getMutator().setSafe(0, 1*number + 1);
    stringVector.getMutator().setSafe(1, "ONE".getBytes());
    intVector.getMutator().setSafe(1, 1*number +1);
    stringVector.getMutator().setSafe(2, "TWO".getBytes());
    intVector.getMutator().setSafe(2, 2*number +2);
    stringVector.getMutator().setSafe(3, "THREE".getBytes());
    intVector.getMutator().setSafe(3, 3*number +3);
    stringVector.getMutator().setSafe(4, "FOUR".getBytes());
    intVector.getMutator().setSafe(4, 4*number +4);
    stringVector.getMutator().setSafe(5, "FIVE".getBytes());
    intVector.getMutator().setSafe(5, 5*number +5);
    stringVector.getMutator().setValueCount(6);
    intVector.getMutator().setValueCount(6);

    VectorContainer container = new VectorContainer();
    container.addCollection(vectorList);
    container.setRecordCount(6);
    WritableBatch batch = WritableBatch.getBatchNoHVWrap(container.getRecordCount(), container, false);
    return batch;
  }
}
