var json = {
  "jid": "f19a5f87b09115ed1d791fa88f84e676",
  "name": "insert-into_default_catalog.default_database.user_sink,default_catalog.default_database.level_stat",
  "isStoppable": false,
  "state": "RUNNING",
  "start-time": 1656660283394,
  "end-time": -1,
  "duration": 48038,
  "maxParallelism": -1,
  "now": 1656660331432,
  "timestamps": {
    "INITIALIZING": 1656660283394,
    "CANCELLING": 0,
    "RESTARTING": 0,
    "SUSPENDED": 0,
    "CANCELED": 0,
    "FAILED": 0,
    "CREATED": 1656660283632,
    "RUNNING": 1656660288278,
    "RECONCILING": 0,
    "FAILING": 0,
    "FINISHED": 0
  },
  "vertices": [
    {
      "id": "e3dfc0d7e9ecd8a43f85f0b68ebf3b80",
      "name": "Source: TableSourceScan(table=[[default_catalog, default_database, user_src]], fields=[id, userId, level, amount, update_time]) -> (NotNullEnforcer(fields=[id]) -> Map, Calc(select=[level]))",
      "maxParallelism": 128,
      "parallelism": 2,
      "status": "RUNNING",
      "start-time": 1656660288696,
      "end-time": -1,
      "duration": 42736,
      "tasks": {
        "RECONCILING": 0,
        "SCHEDULED": 0,
        "RUNNING": 2,
        "INITIALIZING": 0,
        "CREATED": 0,
        "CANCELED": 0,
        "FINISHED": 0,
        "FAILED": 0,
        "DEPLOYING": 0,
        "CANCELING": 0
      },
      "metrics": {
        "read-bytes": 0,
        "read-bytes-complete": true,
        "write-bytes": 1446,
        "write-bytes-complete": true,
        "read-records": 0,
        "read-records-complete": true,
        "write-records": 16,
        "write-records-complete": true
      }
    },
    {
      "id": "1e74c5939f495317f347b93785e6366d",
      "name": "bucket_assigner",
      "maxParallelism": 128,
      "parallelism": 2,
      "status": "RUNNING",
      "start-time": 1656660288713,
      "end-time": -1,
      "duration": 42719,
      "tasks": {
        "RECONCILING": 0,
        "SCHEDULED": 0,
        "RUNNING": 2,
        "INITIALIZING": 0,
        "CREATED": 0,
        "CANCELED": 0,
        "FINISHED": 0,
        "FAILED": 0,
        "DEPLOYING": 0,
        "CANCELING": 0
      },
      "metrics": {
        "read-bytes": 1414,
        "read-bytes-complete": true,
        "write-bytes": 1974,
        "write-bytes-complete": true,
        "read-records": 8,
        "read-records-complete": true,
        "write-records": 8,
        "write-records-complete": true
      }
    },
    {
      "id": "019ff6de1517d7ce1dc3950efee22026",
      "name": "hoodie_stream_write",
      "maxParallelism": 128,
      "parallelism": 4,
      "status": "RUNNING",
      "start-time": 1656660288722,
      "end-time": -1,
      "duration": 42710,
      "tasks": {
        "RECONCILING": 0,
        "SCHEDULED": 0,
        "RUNNING": 4,
        "INITIALIZING": 0,
        "CREATED": 0,
        "CANCELED": 0,
        "FINISHED": 0,
        "FAILED": 0,
        "DEPLOYING": 0,
        "CANCELING": 0
      },
      "metrics": {
        "read-bytes": 2310,
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
      "id": "7e50ce97058742b63bb0ceef1316eb8c",
      "name": "compact_plan_generate",
      "maxParallelism": 128,
      "parallelism": 1,
      "status": "RUNNING",
      "start-time": 1656660288725,
      "end-time": -1,
      "duration": 42707,
      "tasks": {
        "RECONCILING": 0,
        "SCHEDULED": 0,
        "RUNNING": 1,
        "INITIALIZING": 0,
        "CREATED": 0,
        "CANCELED": 0,
        "FINISHED": 0,
        "FAILED": 0,
        "DEPLOYING": 0,
        "CANCELING": 0
      },
      "metrics": {
        "read-bytes": 168,
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
      "id": "2d0c882dd6e428e6726006ac8646a868",
      "name": "compact_task",
      "maxParallelism": 128,
      "parallelism": 4,
      "status": "RUNNING",
      "start-time": 1656660288725,
      "end-time": -1,
      "duration": 42707,
      "tasks": {
        "RECONCILING": 0,
        "SCHEDULED": 0,
        "RUNNING": 4,
        "INITIALIZING": 0,
        "CREATED": 0,
        "CANCELED": 0,
        "FINISHED": 0,
        "FAILED": 0,
        "DEPLOYING": 0,
        "CANCELING": 0
      },
      "metrics": {
        "read-bytes": 168,
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
      "id": "af009a75eaec1cf0bcc8ccfb8aa8d948",
      "name": "Sink: compact_commit",
      "maxParallelism": 128,
      "parallelism": 1,
      "status": "RUNNING",
      "start-time": 1656660288727,
      "end-time": -1,
      "duration": 42705,
      "tasks": {
        "RECONCILING": 0,
        "SCHEDULED": 0,
        "RUNNING": 1,
        "INITIALIZING": 0,
        "CREATED": 0,
        "CANCELED": 0,
        "FINISHED": 0,
        "FAILED": 0,
        "DEPLOYING": 0,
        "CANCELING": 0
      },
      "metrics": {
        "read-bytes": 168,
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
      "id": "c82105323f1a61713019c6d1da10336c",
      "name": "GroupAggregate(groupBy=[level], select=[level, COUNT_RETRACT(*) AS EXPR$1]) -> NotNullEnforcer(fields=[level]) -> Map",
      "maxParallelism": 128,
      "parallelism": 2,
      "status": "RUNNING",
      "start-time": 1656660288730,
      "end-time": -1,
      "duration": 42702,
      "tasks": {
        "RECONCILING": 0,
        "SCHEDULED": 0,
        "RUNNING": 2,
        "INITIALIZING": 0,
        "CREATED": 0,
        "CANCELED": 0,
        "FINISHED": 0,
        "FAILED": 0,
        "DEPLOYING": 0,
        "CANCELING": 0
      },
      "metrics": {
        "read-bytes": 368,
        "read-bytes-complete": true,
        "write-bytes": 1524,
        "write-bytes-complete": true,
        "read-records": 8,
        "read-records-complete": true,
        "write-records": 12,
        "write-records-complete": true
      }
    },
    {
      "id": "340619d246ae24ca27930524c7e8b42d",
      "name": "bucket_assigner",
      "maxParallelism": 128,
      "parallelism": 2,
      "status": "RUNNING",
      "start-time": 1656660288731,
      "end-time": -1,
      "duration": 42701,
      "tasks": {
        "RECONCILING": 0,
        "SCHEDULED": 0,
        "RUNNING": 2,
        "INITIALIZING": 0,
        "CREATED": 0,
        "CANCELED": 0,
        "FINISHED": 0,
        "FAILED": 0,
        "DEPLOYING": 0,
        "CANCELING": 0
      },
      "metrics": {
        "read-bytes": 1692,
        "read-bytes-complete": true,
        "write-bytes": 2616,
        "write-bytes-complete": true,
        "read-records": 12,
        "read-records-complete": true,
        "write-records": 12,
        "write-records-complete": true
      }
    },
    {
      "id": "4de56b34b69b4b970adef3e468a069b3",
      "name": "hoodie_stream_write",
      "maxParallelism": 128,
      "parallelism": 4,
      "status": "RUNNING",
      "start-time": 1656660288732,
      "end-time": -1,
      "duration": 42700,
      "tasks": {
        "RECONCILING": 0,
        "SCHEDULED": 0,
        "RUNNING": 4,
        "INITIALIZING": 0,
        "CREATED": 0,
        "CANCELED": 0,
        "FINISHED": 0,
        "FAILED": 0,
        "DEPLOYING": 0,
        "CANCELING": 0
      },
      "metrics": {
        "read-bytes": 2952,
        "read-bytes-complete": true,
        "write-bytes": 0,
        "write-bytes-complete": true,
        "read-records": 12,
        "read-records-complete": true,
        "write-records": 0,
        "write-records-complete": true
      }
    },
    {
      "id": "4154978c804a705979ea92e2839f465b",
      "name": "compact_plan_generate",
      "maxParallelism": 128,
      "parallelism": 1,
      "status": "RUNNING",
      "start-time": 1656660288734,
      "end-time": -1,
      "duration": 42698,
      "tasks": {
        "RECONCILING": 0,
        "SCHEDULED": 0,
        "RUNNING": 1,
        "INITIALIZING": 0,
        "CREATED": 0,
        "CANCELED": 0,
        "FINISHED": 0,
        "FAILED": 0,
        "DEPLOYING": 0,
        "CANCELING": 0
      },
      "metrics": {
        "read-bytes": 168,
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
      "id": "1c6092224a70f9b221a05fc94a928f61",
      "name": "compact_task",
      "maxParallelism": 128,
      "parallelism": 4,
      "status": "RUNNING",
      "start-time": 1656660288735,
      "end-time": -1,
      "duration": 42697,
      "tasks": {
        "RECONCILING": 0,
        "SCHEDULED": 0,
        "RUNNING": 4,
        "INITIALIZING": 0,
        "CREATED": 0,
        "CANCELED": 0,
        "FINISHED": 0,
        "FAILED": 0,
        "DEPLOYING": 0,
        "CANCELING": 0
      },
      "metrics": {
        "read-bytes": 168,
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
      "id": "a4de81b4b7d874a4ecfa113c015dc863",
      "name": "Sink: compact_commit",
      "maxParallelism": 128,
      "parallelism": 1,
      "status": "RUNNING",
      "start-time": 1656660288737,
      "end-time": -1,
      "duration": 42695,
      "tasks": {
        "RECONCILING": 0,
        "SCHEDULED": 0,
        "RUNNING": 1,
        "INITIALIZING": 0,
        "CREATED": 0,
        "CANCELED": 0,
        "FINISHED": 0,
        "FAILED": 0,
        "DEPLOYING": 0,
        "CANCELING": 0
      },
      "metrics": {
        "read-bytes": 168,
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
    "RECONCILING": 0,
    "SCHEDULED": 0,
    "RUNNING": 12,
    "INITIALIZING": 0,
    "CREATED": 0,
    "CANCELED": 0,
    "FINISHED": 0,
    "FAILED": 0,
    "DEPLOYING": 0,
    "CANCELING": 0
  },
  "plan": {
    "jid": "f19a5f87b09115ed1d791fa88f84e676",
    "name": "insert-into_default_catalog.default_database.user_sink,default_catalog.default_database.level_stat",
    "nodes": [
      {
        "id": "af009a75eaec1cf0bcc8ccfb8aa8d948",
        "parallelism": 1,
        "operator": "",
        "operator_strategy": "",
        "description": "Sink: compact_commit",
        "inputs": [
          {
            "num": 0,
            "id": "2d0c882dd6e428e6726006ac8646a868",
            "ship_strategy": "REBALANCE",
            "exchange": "pipelined_bounded"
          }
        ],
        "optimizer_properties": {}
      },
      {
        "id": "2d0c882dd6e428e6726006ac8646a868",
        "parallelism": 4,
        "operator": "",
        "operator_strategy": "",
        "description": "compact_task",
        "inputs": [
          {
            "num": 0,
            "id": "7e50ce97058742b63bb0ceef1316eb8c",
            "ship_strategy": "REBALANCE",
            "exchange": "pipelined_bounded"
          }
        ],
        "optimizer_properties": {}
      },
      {
        "id": "7e50ce97058742b63bb0ceef1316eb8c",
        "parallelism": 1,
        "operator": "",
        "operator_strategy": "",
        "description": "compact_plan_generate",
        "inputs": [
          {
            "num": 0,
            "id": "019ff6de1517d7ce1dc3950efee22026",
            "ship_strategy": "REBALANCE",
            "exchange": "pipelined_bounded"
          }
        ],
        "optimizer_properties": {}
      },
      {
        "id": "019ff6de1517d7ce1dc3950efee22026",
        "parallelism": 4,
        "operator": "",
        "operator_strategy": "",
        "description": "hoodie_stream_write",
        "inputs": [
          {
            "num": 0,
            "id": "1e74c5939f495317f347b93785e6366d",
            "ship_strategy": "HASH",
            "exchange": "pipelined_bounded"
          }
        ],
        "optimizer_properties": {}
      },
      {
        "id": "1e74c5939f495317f347b93785e6366d",
        "parallelism": 2,
        "operator": "",
        "operator_strategy": "",
        "description": "bucket_assigner",
        "inputs": [
          {
            "num": 0,
            "id": "e3dfc0d7e9ecd8a43f85f0b68ebf3b80",
            "ship_strategy": "HASH",
            "exchange": "pipelined_bounded"
          }
        ],
        "optimizer_properties": {}
      },
      {
        "id": "a4de81b4b7d874a4ecfa113c015dc863",
        "parallelism": 1,
        "operator": "",
        "operator_strategy": "",
        "description": "Sink: compact_commit",
        "inputs": [
          {
            "num": 0,
            "id": "1c6092224a70f9b221a05fc94a928f61",
            "ship_strategy": "REBALANCE",
            "exchange": "pipelined_bounded"
          }
        ],
        "optimizer_properties": {}
      },
      {
        "id": "1c6092224a70f9b221a05fc94a928f61",
        "parallelism": 4,
        "operator": "",
        "operator_strategy": "",
        "description": "compact_task",
        "inputs": [
          {
            "num": 0,
            "id": "4154978c804a705979ea92e2839f465b",
            "ship_strategy": "REBALANCE",
            "exchange": "pipelined_bounded"
          }
        ],
        "optimizer_properties": {}
      },
      {
        "id": "4154978c804a705979ea92e2839f465b",
        "parallelism": 1,
        "operator": "",
        "operator_strategy": "",
        "description": "compact_plan_generate",
        "inputs": [
          {
            "num": 0,
            "id": "4de56b34b69b4b970adef3e468a069b3",
            "ship_strategy": "REBALANCE",
            "exchange": "pipelined_bounded"
          }
        ],
        "optimizer_properties": {}
      },
      {
        "id": "4de56b34b69b4b970adef3e468a069b3",
        "parallelism": 4,
        "operator": "",
        "operator_strategy": "",
        "description": "hoodie_stream_write",
        "inputs": [
          {
            "num": 0,
            "id": "340619d246ae24ca27930524c7e8b42d",
            "ship_strategy": "HASH",
            "exchange": "pipelined_bounded"
          }
        ],
        "optimizer_properties": {}
      },
      {
        "id": "340619d246ae24ca27930524c7e8b42d",
        "parallelism": 2,
        "operator": "",
        "operator_strategy": "",
        "description": "bucket_assigner",
        "inputs": [
          {
            "num": 0,
            "id": "c82105323f1a61713019c6d1da10336c",
            "ship_strategy": "HASH",
            "exchange": "pipelined_bounded"
          }
        ],
        "optimizer_properties": {}
      },
      {
        "id": "c82105323f1a61713019c6d1da10336c",
        "parallelism": 2,
        "operator": "",
        "operator_strategy": "",
        "description": "GroupAggregate(groupBy=[level], select=[level, COUNT_RETRACT(*) AS EXPR$1]) -&gt; NotNullEnforcer(fields=[level]) -&gt; Map",
        "inputs": [
          {
            "num": 0,
            "id": "e3dfc0d7e9ecd8a43f85f0b68ebf3b80",
            "ship_strategy": "HASH",
            "exchange": "pipelined_bounded"
          }
        ],
        "optimizer_properties": {}
      },
      {
        "id": "e3dfc0d7e9ecd8a43f85f0b68ebf3b80",
        "parallelism": 2,
        "operator": "",
        "operator_strategy": "",
        "description": "Source: TableSourceScan(table=[[default_catalog, default_database, user_src]], fields=[id, userId, level, amount, update_time]) -&gt; (NotNullEnforcer(fields=[id]) -&gt; Map, Calc(select=[level]))",
        "optimizer_properties": {}
      }
    ]
  }
}