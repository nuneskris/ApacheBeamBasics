package com.nuneskris.study.beam.dataflow;


import org.apache.beam.sdk.options.*;

/** Options that can be used to configure the Beam examples. */
public interface StudyOptions extends PipelineOptions, StreamingOptions {
    @Description("Whether to keep jobs running after local process exit")
    @Default.Boolean(false)
    boolean getKeepJobsRunning();

    void setKeepJobsRunning(boolean keepJobsRunning);

    @Description("Number of workers to use when executing the injector pipeline")
    @Default.Integer(1)
    int getInjectorNumWorkers();

    void setInjectorNumWorkers(int numWorkers);

    @Description("The Cloud Pub/Sub topic to read from.")
    @Validation.Required
    String getInputTopic();

    void setInputTopic(String value);

    @Description("Output file's window size in number of minutes.")
    @Default.Integer(1)
    Integer getWindowSize();

    void setWindowSize(Integer value);


}