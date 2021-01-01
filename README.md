Limitations:

PreStreaming:

Need to put google sheets in a google drive directory
The Sheets name should contain sheet
kafka zookeeper and Broker needs to be running


Spark:
To run the python application you need to run the below command in shell

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 --name kafkaStream kafka_streamer.py 