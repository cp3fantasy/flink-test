var json = {
    "jid": "ac26f759b8faf7a84f5c66721781b0dd",
    "name": "insert-into_default_catalog.default_database.stat_level_1,default_catalog.default_database.stat_level_2,default_catalog.default_database.stat_level_3",
    "isStoppable": false,
    "state": "RUNNING",
    "start-time": 1657869854546,
    "end-time": -1,
    "duration": 83433,
    "maxParallelism": -1,
    "now": 1657869937979,
    "timestamps": {
      "CANCELED": 0,
      "RUNNING": 1657869919322,
      "FAILING": 0,
      "FINISHED": 0,
      "RESTARTING": 1657869918277,
      "INITIALIZING": 1657869854546,
      "FAILED": 0,
      "CANCELLING": 0,
      "RECONCILING": 0,
      "SUSPENDED": 0,
      "CREATED": 1657869854778
    },
    "vertices": [
      {
        "id": "cbc357ccb763df2852fee8c4fc7d55f2",
        "name": "Source: TableSourceScan(table=[[default_catalog, default_database, pv, watermark=[TO_TIMESTAMP(FROM_UNIXTIME(/($2, 1000)))]]], fields=[pageId, userId, startTime]) -> Calc(select=[pageId, userId, Reinterpret(TO_TIMESTAMP(FROM_UNIXTIME((startTime / 1000)))) AS ts])",
        "maxParallelism": 128,
        "parallelism": 2,
        "status": "INITIALIZING",
        "start-time": 1657869919336,
        "end-time": -1,
        "duration": 18643,
        "tasks": {
          "FINISHED": 0,
          "FAILED": 0,
          "SCHEDULED": 0,
          "DEPLOYING": 0,
          "CANCELING": 0,
          "CREATED": 0,
          "RECONCILING": 0,
          "CANCELED": 0,
          "RUNNING": 0,
          "INITIALIZING": 2
        },
        "metrics": {
          "read-bytes": 0,
          "read-bytes-complete": true,
          "write-bytes": 0,
          "write-bytes-complete": true,
          "read-records": 0,
          "read-records-complete": true,
          "write-records": 0,
          "write-records-complete": true
        }
      },
      {
        "id": "6cdc5bb954874d922eaee11a8e7b5dd5",
        "name": "Source: TableSourceScan(table=[[default_catalog, default_database, user_info]], fields=[id, userId, level, update_time]) -> DropUpdateBefore -> WatermarkAssigner(rowtime=[update_time], watermark=[update_time]) -> Calc(select=[userId, level, update_time])",
        "maxParallelism": 128,
        "parallelism": 2,
        "status": "RUNNING",
        "start-time": 1657869919336,
        "end-time": -1,
        "duration": 18643,
        "tasks": {
          "FINISHED": 0,
          "FAILED": 0,
          "SCHEDULED": 0,
          "DEPLOYING": 0,
          "CANCELING": 0,
          "CREATED": 0,
          "RECONCILING": 0,
          "CANCELED": 0,
          "RUNNING": 2,
          "INITIALIZING": 0
        },
        "metrics": {
          "read-bytes": 0,
          "read-bytes-complete": true,
          "write-bytes": 0,
          "write-bytes-complete": true,
          "read-records": 0,
          "read-records-complete": true,
          "write-records": 8,
          "write-records-complete": true
        }
      },
      {
        "id": "9b292e94298dd6ba47492a59dffbd251",
        "name": "TemporalJoin(joinType=[LeftOuterJoin], where=[((userId = userId0) AND __TEMPORAL_JOIN_CONDITION(ts, update_time, __TEMPORAL_JOIN_CONDITION_PRIMARY_KEY(userId0), __TEMPORAL_JOIN_LEFT_KEY(userId), __TEMPORAL_JOIN_RIGHT_KEY(userId0)))], select=[pageId, userId, ts, userId0, level, update_time]) -> Calc(select=[pageId, level])",
        "maxParallelism": 128,
        "parallelism": 2,
        "status": "RUNNING",
        "start-time": 1657869919336,
        "end-time": -1,
        "duration": 18643,
        "tasks": {
          "FINISHED": 0,
          "FAILED": 0,
          "SCHEDULED": 0,
          "DEPLOYING": 0,
          "CANCELING": 0,
          "CREATED": 0,
          "RECONCILING": 0,
          "CANCELED": 0,
          "RUNNING": 2,
          "INITIALIZING": 0
        },
        "metrics": {
          "read-bytes": 386,
          "read-bytes-complete": true,
          "write-bytes": 0,
          "write-bytes-complete": true,
          "read-records": 8,
          "read-records-complete": true,
          "write-records": 0,
          "write-records-complete": true
        }
      },
      {
        "id": "efea27896b0a201f4ced3798769d4f2e",
        "name": "GroupAggregate(groupBy=[pageId, level], select=[pageId, level, COUNT(*) AS cnt]) -> (Calc(select=[pageId, cnt], where=[(cnt > 1)]), Calc(select=[pageId, cnt], where=[(cnt > 2)]), Calc(select=[pageId, cnt], where=[(cnt > 3)]))",
        "maxParallelism": 128,
        "parallelism": 2,
        "status": "RUNNING",
        "start-time": 1657869919337,
        "end-time": -1,
        "duration": 18642,
        "tasks": {
          "FINISHED": 0,
          "FAILED": 0,
          "SCHEDULED": 0,
          "DEPLOYING": 0,
          "CANCELING": 0,
          "CREATED": 0,
          "RECONCILING": 0,
          "CANCELED": 0,
          "RUNNING": 2,
          "INITIALIZING": 0
        },
        "metrics": {
          "read-bytes": 16,
          "read-bytes-complete": true,
          "write-bytes": 0,
          "write-bytes-complete": true,
          "read-records": 0,
          "read-records-complete": true,
          "write-records": 0,
          "write-records-complete": true
        }
      },
      {
        "id": "d6a0b1528839ed709d1b6b93280ee2ac",
        "name": "GroupAggregate(groupBy=[pageId], select=[pageId, SUM_RETRACT(cnt) AS EXPR$1]) -> Sink: Sink(table=[default_catalog.default_database.stat_level_1], fields=[pageId, EXPR$1])",
        "maxParallelism": 128,
        "parallelism": 2,
        "status": "RUNNING",
        "start-time": 1657869919337,
        "end-time": -1,
        "duration": 18642,
        "tasks": {
          "FINISHED": 0,
          "FAILED": 0,
          "SCHEDULED": 0,
          "DEPLOYING": 0,
          "CANCELING": 0,
          "CREATED": 0,
          "RECONCILING": 0,
          "CANCELED": 0,
          "RUNNING": 2,
          "INITIALIZING": 0
        },
        "metrics": {
          "read-bytes": 16,
          "read-bytes-complete": true,
          "write-bytes": 0,
          "write-bytes-complete": true,
          "read-records": 0,
          "read-records-complete": true,
          "write-records": 0,
          "write-records-complete": true
        }
      },
      {
        "id": "287c71411d0a03f133852f11233ba9f5",
        "name": "GroupAggregate(groupBy=[pageId], select=[pageId, SUM_RETRACT(cnt) AS EXPR$1]) -> Sink: Sink(table=[default_catalog.default_database.stat_level_2], fields=[pageId, EXPR$1])",
        "maxParallelism": 128,
        "parallelism": 2,
        "status": "RUNNING",
        "start-time": 1657869919337,
        "end-time": -1,
        "duration": 18642,
        "tasks": {
          "FINISHED": 0,
          "FAILED": 0,
          "SCHEDULED": 0,
          "DEPLOYING": 0,
          "CANCELING": 0,
          "CREATED": 0,
          "RECONCILING": 0,
          "CANCELED": 0,
          "RUNNING": 2,
          "INITIALIZING": 0
        },
        "metrics": {
          "read-bytes": 16,
          "read-bytes-complete": true,
          "write-bytes": 0,
          "write-bytes-complete": true,
          "read-records": 0,
          "read-records-complete": true,
          "write-records": 0,
          "write-records-complete": true
        }
      },
      {
        "id": "a64a83ae8d1bda597c4180e209f192a9",
        "name": "GroupAggregate(groupBy=[pageId], select=[pageId, SUM_RETRACT(cnt) AS EXPR$1]) -> Sink: Sink(table=[default_catalog.default_database.stat_level_3], fields=[pageId, EXPR$1])",
        "maxParallelism": 128,
        "parallelism": 2,
        "status": "RUNNING",
        "start-time": 1657869919337,
        "end-time": -1,
        "duration": 18642,
        "tasks": {
          "FINISHED": 0,
          "FAILED": 0,
          "SCHEDULED": 0,
          "DEPLOYING": 0,
          "CANCELING": 0,
          "CREATED": 0,
          "RECONCILING": 0,
          "CANCELED": 0,
          "RUNNING": 2,
          "INITIALIZING": 0
        },
        "metrics": {
          "read-bytes": 16,
          "read-bytes-complete": true,
          "write-bytes": 0,
          "write-bytes-complete": true,
          "read-records": 0,
          "read-records-complete": true,
          "write-records": 0,
          "write-records-complete": true
        }
      }
    ],
    "status-counts": {
      "FINISHED": 0,
      "FAILED": 0,
      "SCHEDULED": 0,
      "DEPLOYING": 0,
      "CANCELING": 0,
      "CREATED": 0,
      "RECONCILING": 0,
      "CANCELED": 0,
      "RUNNING": 6,
      "INITIALIZING": 1
    },
    "plan": {
      "jid": "ac26f759b8faf7a84f5c66721781b0dd",
      "name": "insert-into_default_catalog.default_database.stat_level_1,default_catalog.default_database.stat_level_2,default_catalog.default_database.stat_level_3",
      "nodes": [
        {
          "id": "d6a0b1528839ed709d1b6b93280ee2ac",
          "parallelism": 2,
          "operator": "",
          "operator_strategy": "",
          "description": "GroupAggregate(groupBy=[pageId], select=[pageId, SUM_RETRACT(cnt) AS EXPR$1]) -&gt; Sink: Sink(table=[default_catalog.default_database.stat_level_1], fields=[pageId, EXPR$1])",
          "inputs": [
            {
              "num": 0,
              "id": "efea27896b0a201f4ced3798769d4f2e",
              "ship_strategy": "HASH",
              "exchange": "pipelined_bounded"
            }
          ],
          "optimizer_properties": {}
        },
        {
          "id": "287c71411d0a03f133852f11233ba9f5",
          "parallelism": 2,
          "operator": "",
          "operator_strategy": "",
          "description": "GroupAggregate(groupBy=[pageId], select=[pageId, SUM_RETRACT(cnt) AS EXPR$1]) -&gt; Sink: Sink(table=[default_catalog.default_database.stat_level_2], fields=[pageId, EXPR$1])",
          "inputs": [
            {
              "num": 0,
              "id": "efea27896b0a201f4ced3798769d4f2e",
              "ship_strategy": "HASH",
              "exchange": "pipelined_bounded"
            }
          ],
          "optimizer_properties": {}
        },
        {
          "id": "a64a83ae8d1bda597c4180e209f192a9",
          "parallelism": 2,
          "operator": "",
          "operator_strategy": "",
          "description": "GroupAggregate(groupBy=[pageId], select=[pageId, SUM_RETRACT(cnt) AS EXPR$1]) -&gt; Sink: Sink(table=[default_catalog.default_database.stat_level_3], fields=[pageId, EXPR$1])",
          "inputs": [
            {
              "num": 0,
              "id": "efea27896b0a201f4ced3798769d4f2e",
              "ship_strategy": "HASH",
              "exchange": "pipelined_bounded"
            }
          ],
          "optimizer_properties": {}
        },
        {
          "id": "efea27896b0a201f4ced3798769d4f2e",
          "parallelism": 2,
          "operator": "",
          "operator_strategy": "",
          "description": "GroupAggregate(groupBy=[pageId, level], select=[pageId, level, COUNT(*) AS cnt]) -&gt; (Calc(select=[pageId, cnt], where=[(cnt &gt; 1)]), Calc(select=[pageId, cnt], where=[(cnt &gt; 2)]), Calc(select=[pageId, cnt], where=[(cnt &gt; 3)]))",
          "inputs": [
            {
              "num": 0,
              "id": "9b292e94298dd6ba47492a59dffbd251",
              "ship_strategy": "HASH",
              "exchange": "pipelined_bounded"
            }
          ],
          "optimizer_properties": {}
        },
        {
          "id": "9b292e94298dd6ba47492a59dffbd251",
          "parallelism": 2,
          "operator": "",
          "operator_strategy": "",
          "description": "TemporalJoin(joinType=[LeftOuterJoin], where=[((userId = userId0) AND __TEMPORAL_JOIN_CONDITION(ts, update_time, __TEMPORAL_JOIN_CONDITION_PRIMARY_KEY(userId0), __TEMPORAL_JOIN_LEFT_KEY(userId), __TEMPORAL_JOIN_RIGHT_KEY(userId0)))], select=[pageId, userId, ts, userId0, level, update_time]) -&gt; Calc(select=[pageId, level])",
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