var json = {
    "jid": "e9f5994bf5d51deda871566ab9dab657",
    "name": "insert-into_default_catalog.default_database.pv_user_1,default_catalog.default_database.pv_user_2",
    "isStoppable": false,
    "state": "RUNNING",
    "start-time": 1657867750494,
    "end-time": -1,
    "duration": 13370,
    "maxParallelism": -1,
    "now": 1657867763864,
    "timestamps": {
      "RESTARTING": 0,
      "RECONCILING": 0,
      "RUNNING": 1657867753669,
      "CANCELED": 0,
      "SUSPENDED": 0,
      "CANCELLING": 0,
      "CREATED": 1657867751080,
      "INITIALIZING": 1657867750494,
      "FAILED": 0,
      "FAILING": 0,
      "FINISHED": 0
    },
    "vertices": [
      {
        "id": "cbc357ccb763df2852fee8c4fc7d55f2",
        "name": "Source: TableSourceScan(table=[[default_catalog, default_database, pv, watermark=[TO_TIMESTAMP(FROM_UNIXTIME(/($2, 1000)))]]], fields=[pageId, userId, startTime]) -> Calc(select=[pageId, userId, Reinterpret(TO_TIMESTAMP(FROM_UNIXTIME((startTime / 1000)))) AS ts])",
        "maxParallelism": 128,
        "parallelism": 2,
        "status": "INITIALIZING",
        "start-time": 1657867753895,
        "end-time": -1,
        "duration": 9969,
        "tasks": {
          "CANCELED": 0,
          "RECONCILING": 0,
          "CANCELING": 0,
          "RUNNING": 0,
          "CREATED": 0,
          "DEPLOYING": 0,
          "INITIALIZING": 2,
          "FINISHED": 0,
          "SCHEDULED": 0,
          "FAILED": 0
        },
        "metrics": {
          "read-bytes": 0,
          "read-bytes-complete": false,
          "write-bytes": 0,
          "write-bytes-complete": false,
          "read-records": 0,
          "read-records-complete": false,
          "write-records": 0,
          "write-records-complete": false
        }
      },
      {
        "id": "6cdc5bb954874d922eaee11a8e7b5dd5",
        "name": "Source: TableSourceScan(table=[[default_catalog, default_database, user_info]], fields=[id, userId, level, update_time]) -> DropUpdateBefore -> WatermarkAssigner(rowtime=[update_time], watermark=[update_time]) -> Calc(select=[userId, level, update_time])",
        "maxParallelism": 128,
        "parallelism": 2,
        "status": "RUNNING",
        "start-time": 1657867753911,
        "end-time": -1,
        "duration": 9953,
        "tasks": {
          "CANCELED": 0,
          "RECONCILING": 0,
          "CANCELING": 0,
          "RUNNING": 2,
          "CREATED": 0,
          "DEPLOYING": 0,
          "INITIALIZING": 0,
          "FINISHED": 0,
          "SCHEDULED": 0,
          "FAILED": 0
        },
        "metrics": {
          "read-bytes": 0,
          "read-bytes-complete": false,
          "write-bytes": 0,
          "write-bytes-complete": false,
          "read-records": 0,
          "read-records-complete": false,
          "write-records": 0,
          "write-records-complete": false
        }
      },
      {
        "id": "9b292e94298dd6ba47492a59dffbd251",
        "name": "TemporalJoin(joinType=[LeftOuterJoin], where=[((userId = userId0) AND (userId = _UTF-16LE'user1':VARCHAR(2147483647) CHARACTER SET \"UTF-16LE\") AND __TEMPORAL_JOIN_CONDITION(ts, update_time, __TEMPORAL_JOIN_CONDITION_PRIMARY_KEY(userId0), __TEMPORAL_JOIN_LEFT_KEY(userId), __TEMPORAL_JOIN_RIGHT_KEY(userId0)))], select=[pageId, userId, ts, userId0, level, update_time]) -> Calc(select=[pageId, userId, level, ts]) -> Sink: Sink(table=[default_catalog.default_database.pv_user_1], fields=[pageId, userId, level, ts])",
        "maxParallelism": 128,
        "parallelism": 2,
        "status": "RUNNING",
        "start-time": 1657867753912,
        "end-time": -1,
        "duration": 9952,
        "tasks": {
          "CANCELED": 0,
          "RECONCILING": 0,
          "CANCELING": 0,
          "RUNNING": 2,
          "CREATED": 0,
          "DEPLOYING": 0,
          "INITIALIZING": 0,
          "FINISHED": 0,
          "SCHEDULED": 0,
          "FAILED": 0
        },
        "metrics": {
          "read-bytes": 0,
          "read-bytes-complete": false,
          "write-bytes": 0,
          "write-bytes-complete": false,
          "read-records": 0,
          "read-records-complete": false,
          "write-records": 0,
          "write-records-complete": false
        }
      },
      {
        "id": "3f9bae5596df80cf7fcdf2c454ab4bdc",
        "name": "TemporalJoin(joinType=[LeftOuterJoin], where=[((userId = userId0) AND (userId = _UTF-16LE'user2':VARCHAR(2147483647) CHARACTER SET \"UTF-16LE\") AND __TEMPORAL_JOIN_CONDITION(ts, update_time, __TEMPORAL_JOIN_CONDITION_PRIMARY_KEY(userId0), __TEMPORAL_JOIN_LEFT_KEY(userId), __TEMPORAL_JOIN_RIGHT_KEY(userId0)))], select=[pageId, userId, ts, userId0, level, update_time]) -> Calc(select=[pageId, userId, level, ts]) -> Sink: Sink(table=[default_catalog.default_database.pv_user_2], fields=[pageId, userId, level, ts])",
        "maxParallelism": 128,
        "parallelism": 2,
        "status": "RUNNING",
        "start-time": 1657867753919,
        "end-time": -1,
        "duration": 9945,
        "tasks": {
          "CANCELED": 0,
          "RECONCILING": 0,
          "CANCELING": 0,
          "RUNNING": 2,
          "CREATED": 0,
          "DEPLOYING": 0,
          "INITIALIZING": 0,
          "FINISHED": 0,
          "SCHEDULED": 0,
          "FAILED": 0
        },
        "metrics": {
          "read-bytes": 0,
          "read-bytes-complete": false,
          "write-bytes": 0,
          "write-bytes-complete": false,
          "read-records": 0,
          "read-records-complete": false,
          "write-records": 0,
          "write-records-complete": false
        }
      }
    ],
    "status-counts": {
      "CANCELED": 0,
      "RECONCILING": 0,
      "CANCELING": 0,
      "RUNNING": 3,
      "CREATED": 0,
      "DEPLOYING": 0,
      "INITIALIZING": 1,
      "FINISHED": 0,
      "SCHEDULED": 0,
      "FAILED": 0
    },
    "plan": {
      "jid": "e9f5994bf5d51deda871566ab9dab657",
      "name": "insert-into_default_catalog.default_database.pv_user_1,default_catalog.default_database.pv_user_2",
      "nodes": [
        {
          "id": "9b292e94298dd6ba47492a59dffbd251",
          "parallelism": 2,
          "operator": "",
          "operator_strategy": "",
          "description": "TemporalJoin(joinType=[LeftOuterJoin], where=[((userId = userId0) AND (userId = _UTF-16LE'user1':VARCHAR(2147483647) CHARACTER SET &quot;UTF-16LE&quot;) AND __TEMPORAL_JOIN_CONDITION(ts, update_time, __TEMPORAL_JOIN_CONDITION_PRIMARY_KEY(userId0), __TEMPORAL_JOIN_LEFT_KEY(userId), __TEMPORAL_JOIN_RIGHT_KEY(userId0)))], select=[pageId, userId, ts, userId0, level, update_time]) -&gt; Calc(select=[pageId, userId, level, ts]) -&gt; Sink: Sink(table=[default_catalog.default_database.pv_user_1], fields=[pageId, userId, level, ts])",
          "inputs": [
            {
              "num": 0,
              "id": "cbc357ccb763df2852fee8c4fc7d55f2",
              "ship_strategy": "HASH",
              "exchange": "pipelined_bounded"
            },
            {
              "num": 1,
              "id": "6cdc5bb954874d922eaee11a8e7b5dd5",
              "ship_strategy": "HASH",
              "exchange": "pipelined_bounded"
            }
          ],
          "optimizer_properties": {}
        },
        {
          "id": "3f9bae5596df80cf7fcdf2c454ab4bdc",
          "parallelism": 2,
          "operator": "",
          "operator_strategy": "",
          "description": "TemporalJoin(joinType=[LeftOuterJoin], where=[((userId = userId0) AND (userId = _UTF-16LE'user2':VARCHAR(2147483647) CHARACTER SET &quot;UTF-16LE&quot;) AND __TEMPORAL_JOIN_CONDITION(ts, update_time, __TEMPORAL_JOIN_CONDITION_PRIMARY_KEY(userId0), __TEMPORAL_JOIN_LEFT_KEY(userId), __TEMPORAL_JOIN_RIGHT_KEY(userId0)))], select=[pageId, userId, ts, userId0, level, update_time]) -&gt; Calc(select=[pageId, userId, level, ts]) -&gt; Sink: Sink(table=[default_catalog.default_database.pv_user_2], fields=[pageId, userId, level, ts])",
          "inputs": [
            {
              "num": 0,
              "id": "cbc357ccb763df2852fee8c4fc7d55f2",
              "ship_strategy": "HASH",
              "exchange": "pipelined_bounded"
            },
            {
              "num": 1,
              "id": "6cdc5bb954874d922eaee11a8e7b5dd5",
              "ship_strategy": "HASH",
              "exchange": "pipelined_bounded"
            }
          ],
          "optimizer_properties": {}
        },
        {
          "id": "cbc357ccb763df2852fee8c4fc7d55f2",
          "parallelism": 2,
          "operator": "",
          "operator_strategy": "",
          "description": "Source: TableSourceScan(table=[[default_catalog, default_database, pv, watermark=[TO_TIMESTAMP(FROM_UNIXTIME(/($2, 1000)))]]], fields=[pageId, userId, startTime]) -&gt; Calc(select=[pageId, userId, Reinterpret(TO_TIMESTAMP(FROM_UNIXTIME((startTime / 1000)))) AS ts])",
          "optimizer_properties": {}
        },
        {
          "id": "6cdc5bb954874d922eaee11a8e7b5dd5",
          "parallelism": 2,
          "operator": "",
          "operator_strategy": "",
          "description": "Source: TableSourceScan(table=[[default_catalog, default_database, user_info]], fields=[id, userId, level, update_time]) -&gt; DropUpdateBefore -&gt; WatermarkAssigner(rowtime=[update_time], watermark=[update_time]) -&gt; Calc(select=[userId, level, update_time])",
          "optimizer_properties": {}
        }
      ]
    }
  };