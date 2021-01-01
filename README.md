Limitations:

PreStreaming:
Need to put google sheets in a google drive directory
The Sheets name should contain keyword sheet
kafka zookeeper and Broker needs to be running

For creating the topics based on the sheets in the Drive's particular directory
1. Need to Run kafka_topic_creator.py
2. To Stream the data run kafka streamer.py
3. To Send data to kafka run kafka_producer.py

Spark:
To run the python application you need to run the below command in shell

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 --name kafkaStream kafka_streamer.py 