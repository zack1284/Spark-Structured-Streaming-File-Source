## Spark-Structured-Streaming-File-Source
To implement testing with Spark Structured Streaming.

### File Dir
```
├── dir
│   ├── checkpoint
│   │   ├── commits
│   │   │   └── 0
│   │   ├── metadata
│   │   ├── offsets
│   │   │   └── 0
│   │   └── sources
│   │       └── 0
│   │           └── 0
│   ├── input
│   │   ├── 001.csv
│   │   ├── 002.csv
│   │   └── 003.csv
│   └── output
│       ├── part-00000-cd54af21-dcab-465d-b6e5-570b64e8052e-c000.json
│       └── _spark_metadata
│           └── 0
├── README.md
└── stream.py
```
### Input
* 001.CSV
* 002.CSV
* 003.CSV

### Output
``` {"id":4,"name":"david","age":25,"city":"taipei"}```
.....

### To start
` spark-submit stream.py `
