#HelloBeam
The HelloBeam is executed locally. Its is a vanila ApacheBeamn example with a few tranformations and DoFn.

#DFScore
This is the same as HelloBeam but executed on GCP and google cloud. there is no template used, but rather a vanila Apache Beam
mvn compile exec:java -Dexec.mainClass=com.nuneskris.study.beam.dataflow.DFScore


#StreamingDataFlow
This has input from pubsub reads a text message and dumps the text message to gcs
NOTE: the template naming in templateLocation is a bit different for streaming data.

mvn compile exec:java \
-Dexec.mainClass=com.nuneskris.study.beam.dataflow.streaming.StreamingDataFlow \
-Dexec.cleanupDaemonThreads=false \
-Dexec.args=" \
--project=java-maven-dataflow \
--stagingLocation=gs://cricket-score-study/staging \
--tempLocation=gs://cricket-score-study/temp \
--templateLocation=gs://cricket-score-study/templates/MyFirstStreaming \
--runner=DataflowRunner \
--useSubscription=true \
--region=us-west1"

JOB_NAME=pubsub-to-bigquery-$USER-`date +"%Y%m%d-%H%M%S%z"`

gcloud dataflow jobs run ${JOB_NAME} \
--gcs-location=gs://cricket-score-study/templates/MyFirstStreaming \
--zone=us-west1-a \
--parameters \
"inputSubscription=projects/java-maven-dataflow/subscriptions/my-sub,\
numShards=1,\
userTempLocation=gs://cricket-score-study/temp/,\
outputDirectory=gs://cricket-score-study/output/,\
outputFilenamePrefix=windowed-file,\
outputFilenameSuffix=.txt"







