# streaming-log-analytics
This is streaming log analytics application using big data technologies. In this NodeJS web server sends the data to kafka producer. This data is consumed by spark structured streaming which performs microbatching and some aggregations and finally write the result into cassandra.
