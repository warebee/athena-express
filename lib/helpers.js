"use strict";
const { StartQueryExecutionCommand, GetQueryExecutionCommand, GetQueryResultsCommand } = require("@aws-sdk/client-athena");
const { GetObjectCommand } = require('@aws-sdk/client-s3');
//helpers.js

const readline = require("readline"),
  csv = require("csvtojson");

function startQueryExecution(config) {
  const params = {
    QueryString: config.sql,
    WorkGroup: config.workgroup,
    ResultConfiguration: {
      OutputLocation: config.s3Bucket,
    },
    QueryExecutionContext: {
      Database: config.db,
      Catalog: config.catalog,
    },
    ExecutionParameters: config.parameters,
  };
  if (config.encryption)
    params.ResultConfiguration.EncryptionConfiguration = config.encryption;

  return new Promise(function (resolve, reject) {
    const startQueryExecutionRecursively = async function () {
      try {
        let data = await config.athenaClient.send(new StartQueryExecutionCommand(params));
        resolve(data.QueryExecutionId);
      } catch (err) {
        isCommonAthenaError(err.code)
          ? setTimeout(() => {
              startQueryExecutionRecursively();
            }, 2000)
          : reject(err);
      }
    };
    startQueryExecutionRecursively();
  });
}

function checkIfExecutionCompleted(config, context) {
  let retry = config.retry;
  return new Promise(function (resolve, reject) {
    const keepCheckingRecursively = async function () {
      try {
        let data = await config.athenaClient.send(new GetQueryExecutionCommand({
            QueryExecutionId: config.QueryExecutionId,
          }));
        if (data.QueryExecution.Status.State === "SUCCEEDED") {
          retry = config.retry;
          context.s3Metadata = config.athenaClient.send(new GetQueryResultsCommand({
              QueryExecutionId: config.QueryExecutionId,
              MaxResults: 1,
            }));
          resolve(data);
        } else if (data.QueryExecution.Status.State === "FAILED") {
          reject(data.QueryExecution.Status.StateChangeReason);
        } else {
          setTimeout(() => {
            keepCheckingRecursively();
          }, retry);
        }
      } catch (err) {
        if (isCommonAthenaError(err.code)) {
          retry = 2000;
          setTimeout(() => {
            keepCheckingRecursively();
          }, retry);
        } else reject(err);
      }
    };
    keepCheckingRecursively();
  });
}

async function getQueryResultsFromS3(params, context) {
  const s3Params = {
    Bucket: params.s3Output.split("/")[2],
    Key: params.s3Output.split("/").slice(3).join("/"),
  };

  if (params.statementType === "UTILITY" || params.statementType === "DDL") {
    const input = (await params.config.s3Client.send(new GetObjectCommand(s3Params))).Body;
    return { items: await cleanUpNonDML(input, context) };
  } else if (Boolean(params.config.pagination)) {
    //user wants DML response paginated

    const paginationFactor = Boolean(params.config.NextToken) ? 0 : 1;

    let paginationParams = {
      QueryExecutionId: params.config.QueryExecutionId,
      MaxResults: params.config.pagination + paginationFactor,
      NextToken: params.config.NextToken,
    };

    const queryResults = await params.config.athenaClient.send(new GetQueryResultsCommand(paginationParams));
      
    if (params.config.formatJson) {
      return {
        items: await cleanUpPaginatedDML(queryResults, paginationFactor, context),
        nextToken: queryResults.NextToken,
        metadata: queryResults.ResultSet.ResultSetMetadata
      };
    } else {
      return {
        items: queryResults.ResultSet?.Rows,
        nextToken: queryResults.NextToken,
        metadata: queryResults.ResultSet.ResultSetMetadata
      };
    }
  } else {
    //user doesn't want DML response paginated
    const metadataQueryPromise = context.s3Metadata || (params.config.includeMetadata ? params.config.athenaClient.send(new GetQueryResultsCommand({
      QueryExecutionId: params.config.QueryExecutionId,
      MaxResults: 1
    })) : null);

    const input = (await params.config.s3Client.send(new GetObjectCommand(s3Params))).Body;
    if (params.config.formatJson) {
      return {
        items: await cleanUpDML(input, params.config.ignoreEmpty, context),
        metadata: params.config.includeMetadata ? (await metadataQueryPromise).ResultSet.ResultSetMetadata : null
      };
    } else {
      return { 
        items: await getRawResultsFromS3(input),
        metadata: params.config.includeMetadata ? (await metadataQueryPromise).ResultSet.ResultSetMetadata : null
      };
    }
  }
}

async function cleanUpPaginatedDML(queryResults, paginationFactor, context) {
  const dataTypes = await getDataTypes(context);
  const columnNames = Object.keys(dataTypes).reverse();
  let rowObject = {};
  let unformattedS3RowArray = null;
  let formattedArray = [];

  for (let i = paginationFactor; i < queryResults.ResultSet.Rows.length; i++) {
    unformattedS3RowArray = queryResults.ResultSet.Rows[i].Data;

    for (let j = 0; j < unformattedS3RowArray.length; j++) {
      if (unformattedS3RowArray[j].hasOwnProperty("VarCharValue")) {
        [rowObject[columnNames[j]]] = [unformattedS3RowArray[j].VarCharValue];
      }
    }

    formattedArray.push(addDataType(rowObject, dataTypes));
    rowObject = {};
  }
  return formattedArray;
}

function getRawResultsFromS3(input) {
  let rawJson = [];
  return new Promise(function (resolve, reject) {
    readline
      .createInterface({
        input,
      })
      .on("line", (line) => {
        rawJson.push(line.trim());
      })
      .on("close", function () {
        resolve(rawJson);
      });
  });
}

function getDataTypes(context) {
  return new Promise(async function (resolve) {
    const columnInfoArray = (await context.s3Metadata).ResultSet.ResultSetMetadata
      .ColumnInfo;
    let columnInfoArrayLength = columnInfoArray.length;
    let columnInfoObject = {};
    while (columnInfoArrayLength--) {
      [columnInfoObject[columnInfoArray[columnInfoArrayLength].Name]] = [
        columnInfoArray[columnInfoArrayLength].Type,
      ];
    }
    resolve(columnInfoObject);
  });
}

async function cleanUpDML(input, ignoreEmpty, context) {
  let cleanJson = [];
  const dataTypes = await getDataTypes(context);
  return new Promise(function (resolve) {
    input.pipe(
      csv({
        ignoreEmpty,
      })
        .on("data", (data) => {
          cleanJson.push(
            addDataType(JSON.parse(data.toString("utf8")), dataTypes)
          );
        })
        .on("finish", function () {
          resolve(cleanJson);
        })
    );
  });
}

function addDataType(input, dataTypes) {
  let updatedObjectWithDataType = {};

  for (const key in input) {
    if (!input[key]) {
      updatedObjectWithDataType[key] = null;
    } else {
      switch (dataTypes[key]) {
        case "varchar":
          updatedObjectWithDataType[key] = input[key];
          break;
        case "boolean":
          updatedObjectWithDataType[key] = JSON.parse(input[key].toLowerCase());
          break;
        case "bigint":
          updatedObjectWithDataType[key] = BigInt(input[key]);
          break;
        case "integer":
        case "tinyint":
        case "smallint":
        case "int":
        case "float":
        case "double":
          updatedObjectWithDataType[key] = Number(input[key]);
          break;
        default:
          updatedObjectWithDataType[key] = input[key];
      }
    }
  }
  return updatedObjectWithDataType;
}

function cleanUpNonDML(input) {
  let cleanJson = [];
  return new Promise(function (resolve) {
    readline
      .createInterface({
        input,
      })
      .on("line", (line) => {
        switch (true) {
          case line.indexOf("\t") > 0:
            line = line.split("\t");
            cleanJson.push({
              [line[0].trim()]: line[1].trim(),
            });
            break;
          default:
            if (line.trim().length) {
              cleanJson.push({
                row: line.trim(),
              });
            }
        }
      })
      .on("close", function () {
        resolve(cleanJson);
      });
  });
}

function validateConstructor(init) {
  if (!init)
    throw new TypeError("Config object not present in the constructor");

  if (!init.athenaClient) {
    throw new TypeError(
      "AthenaClient object not present or incorrect in the constructor"
    );
  }
  
  if (!init.s3Client) {
    throw new TypeError(
      "S3Client object not present or incorrect in the constructor"
    );
  }
}

function isCommonAthenaError(err) {
  return err === "TooManyRequestsException" ||
    err === "ThrottlingException" ||
    err === "NetworkingError" ||
    err === "UnknownEndpoint"
    ? true
    : false;
}

const lowerCaseKeys = (obj) =>
  Object.keys(obj).reduce((acc, key) => {
    if (obj[key] !== undefined) {
      acc[key.toLowerCase()] = obj[key];
    }
    return acc;
  }, {});

module.exports = {
  validateConstructor,
  startQueryExecution,
  checkIfExecutionCompleted,
  getQueryResultsFromS3,
  lowerCaseKeys,
};
