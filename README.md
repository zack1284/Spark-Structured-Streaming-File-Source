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
│       └── part-00000-7d775ec6-53f2-4047-bde6-a71403565b97-c000.json
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
