package com.cloudera.nav.sdk.examples.extraction;

import com.cloudera.nav.sdk.client.ClientConfig;
import com.cloudera.nav.sdk.client.ClientConfigFactory;
import com.cloudera.nav.sdk.client.MetadataExtractor;
import com.cloudera.nav.sdk.client.MetadataResultIterator;
import com.cloudera.nav.sdk.client.MetadataResultSet;
import com.cloudera.nav.sdk.client.NavApiCient;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.opencsv.CSVWriter;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.Months;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryExtraction {
  private static final Logger LOG =
      LoggerFactory.getLogger(QueryExtraction.class);

  private static Integer limit = 500000;

  static class OperationExecution {
    String identity;
    long duration;
    Instant startTime;
    String principal;
    private Instant minStartTime;
    private Instant maxEndTime;
    private List<String> mrStartTimes = Lists.newArrayList();
    private List<String> mrEndTimes = Lists.newArrayList();
    int numStages;

    public OperationExecution(Map<String, Object> obj) {
      identity = (String) obj.get("identity");
      Object started = obj.get("started");
      if (started != null) {
        startTime = Instant.parse((String) obj.get("started"));
      }
      principal = (String) obj.get("principal");
    }

    public void addMrJob(Map<String, Object> mrJobExec) {
      minStartTime = getMinTime(minStartTime, mrJobExec.get("started"));
      maxEndTime = getMaxTime(maxEndTime, mrJobExec.get("ended"));
      this.mrStartTimes.add((String) mrJobExec.get("started"));
      this.mrEndTimes.add((String) mrJobExec.get("ended"));
      numStages++;
      if (!"hive".equals((String) mrJobExec.get("principal"))) {
        principal = (String) mrJobExec.get("principal");
      }
    }

    public void computeDuration() {
      if (startTime == null) {
        startTime = minStartTime;
      }
      if (minStartTime != null && maxEndTime != null) {
        duration = maxEndTime.minus(minStartTime.getMillis()).getMillis();
      }

    }

    @Override
    public String toString() {
      return identity;
    }
  }

  static class Operation {
    String queryText;
    String identity;
    Instant lastStarted;
    String lastPrincipal;

    TreeSet<Long> durations = Sets.newTreeSet();
    TreeSet<Instant> startTimes = Sets.newTreeSet();
    TreeSet<String> users = Sets.newTreeSet();

    long minDuration;
    long maxDuration;
    long averageDuration;
    long medianDuration;
    List<OperationExecution> executions = Lists.newArrayList();
    private String minStartTime;
    private String maxStartTime;
    private String userString;
    private int maxStages;

    public Operation(String opId) {
      this.identity = opId;

    }

    public void addOperationInstances(List<String> execIds, Map<String,
        OperationExecution> allOpExecs) {
      for(String execId : execIds) {
        OperationExecution operationExecution = allOpExecs.get(execId);
        executions.add(operationExecution);
      }
    }

    public void computeDurations() {
      for(OperationExecution operationExecution : executions) {
        durations.add(operationExecution.duration);
        averageDuration += operationExecution.duration;

        if (operationExecution.startTime != null) {
          startTimes.add(operationExecution.startTime);
        }
        if (operationExecution.principal != null) {
          users.add(operationExecution.principal);
        }
        maxStages =
            maxStages > operationExecution.numStages ? maxStages : operationExecution.numStages;
      }

      minDuration = durations.first();
      maxDuration = durations.last();
      medianDuration = Iterables.get(durations, (durations.size() - 1) / 2);
      averageDuration = averageDuration/executions.size();

      minStartTime = startTimes.isEmpty() ? "" : startTimes.first().toString();
      maxStartTime = startTimes.isEmpty() ? "" : startTimes.last().toString();
      userString = Joiner.on(",").skipNulls().join(users);
    }

    public static void writeHeaders(CSVWriter sigmaCSVWriter,
                                    CSVWriter optimizerCsvWriter,
                                    CSVWriter googleFusionCsvWriter) {
      sigmaCSVWriter.writeNext(new String[] {
          "QueryExecIdentity", "mrStartTime", "mrEndTime", "query"});
      googleFusionCsvWriter.writeNext(new String[] {
          "index", "QueryIdentity", "queryExecutionIdentity", "duration", "startTime", "endTime",
          "numStages",
          "user", "queryType"});
      optimizerCsvWriter.writeNext(new String[] {
          "index", "QueryIdentity", "totalExecutions", "averageDuration",
          "minStartTime", "maxStartTime", "users", "query"});
    }

    public static void writeHeadersOperations(CSVWriter sigmaCSVWriter,
                                    CSVWriter optimizerCsvWriter,
                                    CSVWriter googleFusionCsvWriter) {
      sigmaCSVWriter.writeNext(new String[] {
          "QueryExecIdentity", "mrStartTime", "mrEndTime", "query"});
      googleFusionCsvWriter.writeNext(new String[] {
          "index", "QueryIdentity", "totalExecutions", "minDuration", "maxDuration",
          "averageDuration", "medianDuration",
          "minStartTime", "maxStartTime", "maxStages",
          "users", "queryType"});
      optimizerCsvWriter.writeNext(new String[] {
          "index", "QueryIdentity", "totalExecutions", "averageDuration",
          "minStartTime", "maxStartTime", "users", "query"});
    }

    public void writeGoogleFusionFormat(int index, CSVWriter csvWriter) {
      String queryType = getQueryType(escapeQueryText(queryText));

      for(int i = 0; i < executions.size(); i++) {
        OperationExecution opExec = executions.get(i);

        csvWriter.writeNext(new String[]{
            Integer.toString(index),
            identity,
            opExec.identity,
            Long.toString(opExec.duration),
            opExec.startTime != null ? opExec.startTime.toString() : "",
            opExec.maxEndTime != null ? opExec.maxEndTime.toString() : "",
            Integer.toString(opExec.numStages),
            opExec.principal,
            queryType});
      }
    }

    public void writeOptimizerFormat(int index, CSVWriter csvWriter) {
      String escapedQuery = escapeQueryText(queryText);

      csvWriter.writeNext(new String[] {
          Integer.toString(index++),
          identity,
          Integer.toString(executions.size()),
          Long.toString(averageDuration),
          minStartTime, maxStartTime,
          userString, escapedQuery});
    }

    public void writeSigmaFormat(int index, CSVWriter csvWriter) {
      for (OperationExecution oe : executions) {
        for (int i = 0; i < oe.mrEndTimes.size(); i++) {
          if (oe.mrStartTimes.get(i) == null ||
              oe.mrEndTimes.get(i) == null) {
            continue;
          }

          csvWriter.writeNext(new String[] {
              oe.identity,
              Long.toString(Instant.parse(oe.mrStartTimes.get(i)).getMillis()),
              Long.toString(Instant.parse(oe.mrEndTimes.get(i)).getMillis()),
              escapeQueryText(queryText)
          });
        }
      }
    }

    public void write(int i, CSVWriter sigmaCSVWriter, CSVWriter
        optimizerCsvWriter, CSVWriter googleFusionWriter) {
      if (queryText == null) {
        Joiner joiner = Joiner.on( "," ).skipNulls();
        LOG.warn("Skipping {} as query text is null. Operation Execution Ids: {}",
            identity, joiner.join(executions));
        return;
      }
      //writeSigmaFormat(i, sigmaCSVWriter);
      writeOptimizerFormat(i, optimizerCsvWriter);
      writeGoogleFusionFormat(i, googleFusionWriter);
    }
  }

  @SuppressWarnings("unchecked")
  public static  void main(String[] args) throws IOException {
    LogManager.getRootLogger().setLevel(Level.INFO);

    String configFilePath = args[0];
    ClientConfig config = (new ClientConfigFactory())
        .readConfigurations(configFilePath);
    extractQueries(config, args[1], "hive");
    extractQueries(config, args[1], "impala");

    extractDDL(config, args[1]);
  }

  private static void extractDDL(ClientConfig config, String dir) throws
      IOException {
    // Initialize the API.
    NavApiCient client = new NavApiCient(config);
    MetadataExtractor extractor = new MetadataExtractor(client, limit);

    Set<String> allPartitionColumns = Sets.newHashSet();

    Map<String, String> tablePathToIdentity = Maps.newHashMap();

    // Collect all the partition columns
    String entityQuery =
      "+(-deleted:true) +sourceType:hive +type:table";
    MetadataResultSet resultSet = extractor.extractMetadata(null, null,
        entityQuery, null);
    MetadataResultIterator iterator = resultSet.getEntities().iterator();

    int index = 0;
    while(iterator.hasNext()) {
      Map<String, Object> obj = iterator.next();

      index++;
      if (index%1000 == 0) {
        LOG.info("Processed {} tables", index);
      }

      String parentPath = (String) obj.get("parentPath");
      String tableName = (String) obj.get("originalName");

      String tablePath = parentPath + "/" + tableName;
      tablePathToIdentity.put(tablePath, (String) obj.get("identity"));

      Object partColNames = obj.get("partColNames");
      if (partColNames != null) {
        List<String> partitionColumns = (List<String>) partColNames;
        for(String column : partitionColumns) {
          allPartitionColumns.add(tablePath + "/" + column);
        }
      }
    }
    LOG.info("Total processed {} tables", index);

    CSVWriter ddlWriter = createCsvWriter(dir + "/ddl.csv", "\n");
    ddlWriter.writeNext(new String[] {"ColumnId", "databaseName", "tableId", "tableName", "columnName", "type", "isPartitionColumn"});

    // Collect all the operation executions.
    entityQuery =
        "+sourceType:hive +type:field";
    // "+(-deleted:true) +sourceType:hive +type:field +parentPath:\\/l1_ammnprn2_mantas\\/kdd_pttrn_tag";

    index = 0;
    resultSet = extractor.extractMetadata(null, null, entityQuery, null);
    iterator = resultSet.getEntities().iterator();
    while(iterator.hasNext()) {
      if (index%1000 == 0) {
        LOG.info("Processed {} columns", index);
      }

      Map<String, Object> obj = iterator.next();
      String identity = (String) obj.get("identity");
      String type = (String) obj.get("dataType");
      String name = (String) obj.get("originalName");
      if (name == null) {
        continue;
      }

      String parentPath = (String) obj.get("parentPath");
      String[] parentPaths = parentPath.split("/");

      String databaseName = parentPaths[1];
      String tableName = parentPaths[2];
      boolean isPartitionColumn = allPartitionColumns.contains(parentPath + "/" + name);

      ddlWriter.writeNext(new String[]{
          identity, databaseName,
          tablePathToIdentity.get(parentPath), tableName,
          name, type, Boolean.toString(isPartitionColumn)});
      index++;
    }
    LOG.info("Total processed {} columns", index);

    ddlWriter.close();
  }

  private static void   extractQueries(ClientConfig config, String dir, String sourceType) throws IOException {
    Instant startDate = Instant.parse("2015-07-01T00:00:00.000Z");

    for (int i = 0; i < 12; i++) {
      Duration duration = Months.TWELVE.toPeriod().toDurationFrom(startDate);

      Instant endDate = startDate.plus(duration.getMillis());

      String filter =
          String.format(" +(started:[%s TO %s])",
              startDate.toString(), endDate.toString());

//      filter = filter + " +(principal:sagbcsed@jnj.com)";

      String sigmaFile = dir + "/" + sourceType + "/sigma/";
      String optimizerFile = dir + "/" + sourceType + "/optimizer/";
      String googleFile = dir + "/" + sourceType + "/goog/";

      CSVWriter sigmaCSVWriter = createCsvWriter(sigmaFile + startDate.toString() + ".csv", "\n");
      CSVWriter optimizerCsvWriter = createCsvWriter(optimizerFile + startDate.toString() + ".csv", "@@@@");
      CSVWriter googleCsvWriter = createCsvWriter(googleFile + startDate.toString() + ".csv", "\n");

      extractOperations(config, filter, sigmaCSVWriter, optimizerCsvWriter,
          googleCsvWriter, sourceType);

      sigmaCSVWriter.close();
      optimizerCsvWriter.close();
      googleCsvWriter.close();

      startDate = endDate;
    }
  }

  private static void extractOperations(ClientConfig config, String filter,
                                        CSVWriter sigmaCSVWriter, CSVWriter optimizerCsvWriter,
                                        CSVWriter googleCsvWriter, String sourceType) throws IOException {
    // Initialize the API.
    NavApiCient client = new NavApiCient(config);
    MetadataExtractor extractor = new MetadataExtractor(client, limit);


    // Collect all the operation executions.
    String entityQuery =
        //"sourceType:HIVE AND (type:operation_execution)";
      "+(+sourceType:" + sourceType + " +type:operation_execution)" + filter;
    LOG.info("Processing query: {}", entityQuery);

    // GEt all the op execs and their time, user
    MetadataResultSet resultSet = null;
    MetadataResultIterator iterator = null;

    Map<String, OperationExecution> opExecs = Maps.newHashMap();
    resultSet = extractor.extractMetadata(null, null, entityQuery, null);
    iterator = resultSet.getEntities().iterator();
    while(iterator.hasNext()) {
      Map<String, Object> obj = iterator.next();
      OperationExecution operationExecution =
          new OperationExecution(obj);
      opExecs.put(operationExecution.identity, operationExecution);

      if (opExecs.size()%1000 == 0) {
        LOG.info("Processed {} operation executions", opExecs.size());
      }
    }

    LOG.info("Obtained {} operation executions", opExecs.size());
    if (opExecs.size() == 0) {
      return;
    }

    // Collect MR operations so that we can collect the elapsed time.
    if (sourceType.equals("hive")) {
      collectElapsedTimes(extractor, opExecs);
    }

    // Collect operation Ids.
    Map<String, Operation> operations = Maps.newHashMap();

    int index = 0;
    Iterable<List<String>> partitions = Iterables.partition
        (opExecs.keySet(), 10000);
    for(List<String> partition : partitions) {
      LOG.info("Processed {} operation executions", index);

      String entityIds = Joiner.on(",").join(partition);
      String relationsQuery = "type:INSTANCE_OF AND {!terms f=endpoint2Ids}" + entityIds + ",dummy";
      resultSet = extractor.extractMetadata(null, null, null, relationsQuery);
      iterator = resultSet.getRelations().iterator();
      while(iterator.hasNext()) {
        Map<String, Object> obj = iterator.next();
        Map<String, Object> template = (Map<String, Object>) obj.get("template");
        String opId = (String) template.get("entityId");

        Map<String, Object> instances = (Map<String, Object>) obj.get("instances");
        List<String> execIds = (List<String>) instances.get("entityIds");

        Operation operation = operations.get(opId);
        if (operation == null) {
          operation = new Operation(opId);
          operations.put(opId, operation);
        }
        operation.addOperationInstances(execIds, opExecs);

        index++;
      }
    }

    for (Operation operation : operations.values()) {
      operation.computeDurations();
    }

    index = 0;
    LOG.info("Obtained {} operations", operations.size());
    partitions = Iterables.partition(operations.keySet(), 10000);
    for(List<String> partition : partitions) {
      LOG.info("Processed {} operations", index);

      // Get the operation Ids now.
      String entityIds = Joiner.on(",").join(partition);
      entityQuery = "{!terms f=identity}" + entityIds + ",dummy";
      resultSet = extractor.extractMetadata(null, null, entityQuery, null);
      iterator = resultSet.getEntities().iterator();
      while(iterator.hasNext()) {
        Map<String, Object> obj = iterator.next();
        String queryText = (String) obj.get("queryText");
        String identity = (String) obj.get("identity");

        Operation operation = operations.get(identity);
        operation.queryText = queryText;
        index++;
      }
    }
    LOG.info("Total Processed {} operations", index);

    Operation.writeHeaders(sigmaCSVWriter, optimizerCsvWriter, googleCsvWriter);

    index = 0;
    for(Operation operation : operations.values()) {
      operation.write(index++, sigmaCSVWriter, optimizerCsvWriter, googleCsvWriter);
    }
  }

  private static CSVWriter createCsvWriter(String file, String seperator)
      throws FileNotFoundException, UnsupportedEncodingException {
    PrintWriter writer = new PrintWriter(file, "UTF-8");
    return new CSVWriter(writer, CSVWriter.DEFAULT_SEPARATOR, CSVWriter.DEFAULT_QUOTE_CHARACTER, seperator);

  }

  @SuppressWarnings({ "unused", "unchecked" })
  private static void collectElapsedTimes(MetadataExtractor extractor,
                                          Map<String, OperationExecution> opExecs) {
    LOG.info("Processing elapsed times");

    // First collect all MRIds.
    Map<String, OperationExecution> mrIdToOpExecs = Maps.newHashMap();

    int index = 0;
    Iterable<List<String>> partitions = Iterables.partition
        (opExecs.keySet(), 10000);
    for(List<String> partition : partitions) {
      LOG.info("Processed {} operation executions", index);

      String entityIds = Joiner.on(",").join(partition);
      String relationsQuery = "type:LOGICAL_PHYSICAL AND {!terms f=endpoint1Ids}" + entityIds + ",dummy";

      MetadataResultSet resultSet = extractor.extractMetadata(null, null,
          null, relationsQuery);
      MetadataResultIterator iterator = resultSet.getRelations().iterator();
      while(iterator.hasNext()) {
        Map<String, Object> relation = iterator.next();
        Map<String, Object> ep1 = (Map<String, Object>) relation.get("logical");
        String opExecId = (String) ep1.get("entityId");

        Map<String, Object> ep2 = (Map<String, Object>) relation.get("physical");
        List<String> mrExecIds = (List<String>) ep2.get("entityIds");

        for(String mrId : mrExecIds) {
          mrIdToOpExecs.put(mrId, opExecs.get(opExecId));
        }
        index++;
      }
    }

    LOG.info("Obtained {} MR job executions", mrIdToOpExecs.size());

    index = 0;
    // Now collect all the entities for these MRIds.
    partitions = Iterables.partition(mrIdToOpExecs.keySet(), 10000);
    for(List<String> partition : partitions) {
      LOG.info("Processed {} Mr job executions", index);

      String entityIds = Joiner.on(",").join(partition);
      String entitisQuery = "{!terms f=identity}" + entityIds + ",dummy";
      MetadataResultSet resultSet = extractor.extractMetadata(null, null,
          entitisQuery, null);
      MetadataResultIterator iterator = resultSet.getEntities().iterator();
      while(iterator.hasNext()) {
        Map<String, Object> mrJobExec = iterator.next();
        String mrId = (String) mrJobExec.get("identity");

        OperationExecution opExecution = mrIdToOpExecs.get(mrId);
        opExecution.addMrJob(mrJobExec);
        index++;
      }
    }

    for(OperationExecution operationExecution : opExecs.values()) {
      operationExecution.computeDuration();
    }

    LOG.info("Finished computing elapsed times");
  }

  private static Instant getMaxTime(Instant maxEndTime, Object ended) {
    if (ended == null) {
      return maxEndTime;
    }

    Instant endTime = Instant.parse((String) ended);
    if (maxEndTime == null) {
      return endTime;
    }
    return endTime.isAfter(maxEndTime) ? endTime : maxEndTime;
  }

  private static Instant getMinTime(Instant minStartTime, Object started) {
    if (started == null) {
      return minStartTime;
    }

    Instant startTime = Instant.parse((String) started);
    if (minStartTime == null) {
      return startTime;
    }
    return startTime.isBefore(minStartTime) ? startTime : minStartTime;
  }

  private static String escapeQueryText(String queryText) {
    return queryText.replaceAll("\"", "'");
  }

  private static String getQueryType(String queryText) {
    queryText = queryText.trim().toLowerCase();
    if (queryText.startsWith("insert")) {
      return "insert";
    } else if (queryText.startsWith("create")) {
      return "create";
    } else if (queryText.startsWith("delete")) {
      return "delete";
    } else if (queryText.startsWith("select")) {
      return "select";
    } else {
      return "unknown";
    }
  }
}