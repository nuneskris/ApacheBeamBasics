package com.nuneskris.study.beam;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class BeamScore {

    /** A {@link DoFn} that splits lines of text into individual column cells. */
    // The first variable of the doFn is the input and the second variable id the output
    public static class ExtractScore extends DoFn<String, String[]> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            String[] words = c.element().split(",");
            c.output(words);
        }
    }


    // We filter by not calling .output for those rows which are filtered out
    public static class FilterWickets extends  DoFn<String[], String[]> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            if(c.element()[11].equalsIgnoreCase("1")) {
                c.output(c.element());
            }
        }
    }

    public static class ConvertToKV extends  DoFn<String[], KV<String, Integer>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            String[] split = {c.element()[11]};
            String key = c.element()[4] + "," + c.element()[12];
            c.output(KV.of(key, new Integer(1)));
        }
    }

    public static class SumUpValuesByKey extends DoFn<KV<String, Iterable<Integer>>, String>{
        @ProcessElement
        public void processElement(ProcessContext context) {
            int totalWickets = 0;
            String playerAndWicketType = context.element().getKey();
            Iterable<Integer> wickets = context.element().getValue();
            for (Integer amount : wickets) {
                totalWickets += amount.intValue();
            }
            context.output(playerAndWicketType + "," + totalWickets);
        }
    }
}
