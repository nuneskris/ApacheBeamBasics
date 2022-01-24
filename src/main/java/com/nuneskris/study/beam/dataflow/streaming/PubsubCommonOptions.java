package com.nuneskris.study.beam.dataflow.streaming;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation.Required;

/**
 * Common {@link PipelineOptions} for reading and writing data using {@link
 * org.apache.beam.sdk.io.gcp.pubsub.PubsubIO}.
 */
public final class PubsubCommonOptions {

    private PubsubCommonOptions() {}

    /**
     * Provides {@link PipelineOptions} to read records from a Pub/Sub subscription.
     */
    public interface ReadSubscriptionOptions extends PipelineOptions {

        @Description(
                "The Cloud Pub/Sub subscription to consume from. "
                        + "The name should be in the format of "
                        + "projects//subscriptions/.")
        @Required
        String getInputSubscription();
        void setInputSubscription(String inputSubscription);
    }

    /**
     * Provides {@link PipelineOptions} to read records from a Pub/Sub topic.
     */
    public interface ReadTopicOptions extends PipelineOptions {

        @Description(
                "The Cloud Pub/Sub topic to consume from. "
                        + "The name should be in the format of "
                        + "projects//topics/.")
        @Required
        String getInputTopic();

        void setInputTopic(String outputTopic);
    }

    /**
     * Provides {@link PipelineOptions} to write records to a Pub/Sub topic.
     */
    public interface WriteTopicOptions extends PipelineOptions {

        @Description(
                "The Cloud Pub/Sub topic to write to. "
                        + "The name should be in the format of "
                        + "projects//topics/.")
        @Required
        String getOutputTopic();

        void setOutputTopic(String outputTopic);
    }
}
