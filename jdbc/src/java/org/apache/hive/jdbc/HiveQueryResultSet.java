/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.jdbc;

import static org.apache.hive.service.cli.thrift.TCLIServiceConstants.TYPE_NAMES;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.service.HiveServerException;
import org.apache.hive.service.cli.TableSchema;
import org.apache.hive.service.cli.thrift.TCLIService;
import org.apache.hive.service.cli.thrift.TColumnDesc;
import org.apache.hive.service.cli.thrift.TFetchOrientation;
import org.apache.hive.service.cli.thrift.TFetchResultsReq;
import org.apache.hive.service.cli.thrift.TFetchResultsResp;
import org.apache.hive.service.cli.thrift.TGetResultSetMetadataReq;
import org.apache.hive.service.cli.thrift.TGetResultSetMetadataResp;
import org.apache.hive.service.cli.thrift.TOperationHandle;
import org.apache.hive.service.cli.thrift.TRow;
import org.apache.hive.service.cli.thrift.TSessionHandle;
import org.apache.hive.service.cli.thrift.TTableSchema;

/**
 * HiveQueryResultSet.
 *
 */
public class HiveQueryResultSet extends HiveBaseResultSet {

  public static final Log LOG = LogFactory.getLog(HiveQueryResultSet.class);

  private TCLIService.Iface client;
  private TOperationHandle stmtHandle;
  private TSessionHandle sessHandle;
  
  private int maxRows = 0;
  private int rowsFetched = 0;
  private int fetchSize = 50;

  private List<TRow> fetchedRows;
  private Iterator<TRow> fetchedRowsItr;
  private boolean isClosed = false;
  private boolean emptyResultSet = false;

  public static class Builder {

    private TCLIService.Iface client = null;
    private TOperationHandle stmtHandle = null;
    private TSessionHandle sessHandle  = null;

    /**
     * Sets the limit for the maximum number of rows that any ResultSet object produced by this
     * Statement can contain to the given number. If the limit is exceeded, the excess rows
     * are silently dropped. The value must be >= 0, and 0 means there is not limit.
     */
    private int maxRows = 0;
    private boolean retrieveSchema = true;
    private List<String> colNames;
    private List<String> colTypes;
    private int fetchSize = 50;
    private boolean emptyResultSet = false;

    public Builder setClient(TCLIService.Iface client) {
      this.client = client;
      return this;
    }

    public Builder setStmtHandle(TOperationHandle stmtHandle) {
      this.stmtHandle = stmtHandle;
      return this;
    }

    public Builder setSessionHandle(TSessionHandle sessHandle) {
      this.sessHandle = sessHandle;
      return this;
    }

    public Builder setMaxRows(int maxRows) {
      this.maxRows = maxRows;
      return this;
    }

    public Builder setSchema(List<String> colNames, List<String> colTypes) {
      this.colNames = new ArrayList<String>();
      this.colNames.addAll(colNames);
      this.colTypes = new ArrayList<String>();
      this.colTypes.addAll(colTypes);
      this.retrieveSchema = false;
      return this;
    }

    public Builder setFetchSize(int fetchSize) {
      this.fetchSize = fetchSize;
      return this;
    }

    public Builder setEmptyResultSet(boolean emptyResultSet) {
      this.emptyResultSet = emptyResultSet;
      return this;
    }

    public HiveQueryResultSet build() throws SQLException {
      return new HiveQueryResultSet(this);
    }
  }

  protected HiveQueryResultSet(Builder builder) throws SQLException {
    this.client = builder.client;
    this.stmtHandle = builder.stmtHandle;
    this.sessHandle = builder.sessHandle;
    this.fetchSize = builder.fetchSize;
    columnNames = new ArrayList<String>();
    columnTypes = new ArrayList<String>();
    if (builder.retrieveSchema) {
      retrieveSchema();
    } else {
      this.columnNames.addAll(builder.colNames);
      this.columnTypes.addAll(builder.colTypes);
    }
    this.emptyResultSet = builder.emptyResultSet;
    if (builder.emptyResultSet) {
      this.maxRows = 0;
    } else {
      this.maxRows = builder.maxRows;
    }
  }

  /**
   * Retrieve schema from the server
   */
  private void retrieveSchema() throws SQLException {
    try {
      TGetResultSetMetadataReq metadataReq = new TGetResultSetMetadataReq(stmtHandle);
      // TODO need session handle
      TGetResultSetMetadataResp  metadataResp = client.GetResultSetMetadata(metadataReq);
      Utils.verifySuccess(metadataResp.getStatus());

      StringBuilder namesSb = new StringBuilder();
      StringBuilder typesSb = new StringBuilder();

      TTableSchema schema = metadataResp.getSchema();
      if (schema == null || !schema.isSetColumns()) {
        // TODO: should probably throw an exception here.
        return;
      }
      setSchema(new TableSchema(schema));

      List<TColumnDesc> columns = schema.getColumns();
      for (int pos = 0; pos < schema.getColumnsSize(); pos++) {
        if (pos != 0) {
          namesSb.append(",");
          typesSb.append(",");
        }
        String columnName = columns.get(pos).getColumnName();
        columnNames.add(columnName);
        String columnTypeName = TYPE_NAMES.get(
            columns.get(pos).getTypeDesc().getTypes().get(0).getPrimitiveEntry().getType());
        columnTypes.add(columnTypeName);
        namesSb.append(columnName);
        typesSb.append(columnTypeName.toLowerCase());
      }
    } catch (SQLException eS) {
      throw eS; // rethrow the SQLException as is
    } catch (Exception ex) {
      ex.printStackTrace();
      throw new SQLException("Could not create ResultSet: " + ex.getMessage(), ex);
    }
  }

  /**
   * Set the specified schema to the resultset
   * @param colNames
   * @param colTypes
   */
  public void setSchema(List<String> colNames, List<String> colTypes) {
    columnNames.addAll(colNames);
    columnTypes.addAll(colTypes);
  }

  @Override
  public void close() throws SQLException {
    // Need reset during re-open when needed
    client = null;
    stmtHandle = null;
    sessHandle = null;
    isClosed = true;
  }

  /**
   * Moves the cursor down one row from its current position.
   *
   * @see java.sql.ResultSet#next()
   * @throws SQLException
   *           if a database access error occurs.
   */
  public boolean next() throws SQLException {
    if (isClosed) {
      throw new SQLException("Resultset is closed");
    }
    if (emptyResultSet || (maxRows > 0 && rowsFetched >= maxRows)) {
      return false;
    }

    try {
      if (fetchedRows == null || !fetchedRowsItr.hasNext()) {
        TFetchResultsReq fetchReq = new TFetchResultsReq(stmtHandle,
            TFetchOrientation.FETCH_NEXT, fetchSize);
        TFetchResultsResp fetchResp = client.FetchResults(fetchReq);
        Utils.verifySuccessWithInfo(fetchResp.getStatus());
        fetchedRows = fetchResp.getResults().getRows();
        fetchedRowsItr = fetchedRows.iterator();
      }

      String rowStr = "";
      if (fetchedRowsItr.hasNext()) {
        row = fetchedRowsItr.next();
        rowStr = row.toString(); 
      } else {
        return false;
      }

      rowsFetched++;
      if (LOG.isDebugEnabled()) {
        LOG.debug("Fetched row string: " + rowStr);
      }
    } catch (HiveServerException e) {
      if (e.getErrorCode() == 0) { // error code == 0 means reached the EOF
        return false;
      } else {
        throw new SQLException("Error retrieving next row", e);
      }
    } catch (Exception ex) {
      ex.printStackTrace();
      throw new SQLException("Error retrieving next row", ex);
    }
    // NOTE: fetchOne dosn't throw new SQLException("Method not supported").
    return true;
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    if (isClosed) {
      throw new SQLException("Resultset is closed");
    }
    return super.getMetaData();
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {
    if (isClosed) {
      throw new SQLException("Resultset is closed");
    }
    fetchSize = rows;
  }

  @Override
  public int getFetchSize() throws SQLException {
    if (isClosed) {
      throw new SQLException("Resultset is closed");
    }
    return fetchSize;
  }
  
  //TODO: REMOVE THIS HACK!!!! Hive's "show tables" command returns a column name of "tab_name" instead of the
  // JDBC-compliant TABLE_NAME
  @Override
  public String getString(String columnName) throws SQLException {
    
    
    String columnVal = null;
    SQLException exception = null;
    try {
      columnVal = super.getString(columnName);
    }
    catch(SQLException se) {
      // Save for returning later
      exception = se;
    }
    if(columnVal != null) return columnVal;
    if(columnName != null && "TABLE_NAME".equals(columnName)) {
      if(columnName != null && "TABLE_NAME".equals(columnName)) {
        try {
          // If we're using the "show tables" hack in getTables(), return the first column
          columnVal = super.getString(1);
        }
        catch(SQLException se) {
          throw (exception == null) ? se : exception;
        }
      }
    }
    return columnVal;
}
}
