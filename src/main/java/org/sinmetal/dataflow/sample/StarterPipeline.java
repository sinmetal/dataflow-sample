package org.sinmetal.dataflow.sample;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.atilika.kuromoji.ipadic.Token;
import com.atilika.kuromoji.ipadic.Tokenizer;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;

public class StarterPipeline {
	private static final Logger LOG = LoggerFactory.getLogger(StarterPipeline.class);

	public static void main(String[] args) {
		Pipeline p = Pipeline.create(PipelineOptionsFactory.fromArgs(args).withValidation().create());

		p.apply(TextIO.Read.named("ReadMyFile").from("gs://cp300demo1.appspot.com/test.txt"))

				.apply(ParDo.of(new DoFn<String, KV<String, String>>() {
					@Override
					public void processElement(ProcessContext c) {
						Tokenizer tokenizer = new Tokenizer();
						List<Token> tokens = tokenizer.tokenize(c.element());
						for (Token token : tokens) {
							c.output(KV.of(token.getAllFeaturesArray()[0], token.getSurface()));
						}
					}
				}).named("Tokenizer")).apply(GroupByKey.<String, String> create())
				.apply(ParDo.of(new DoFn<KV<String, Iterable<String>>, Void>() {

					@Override
					public void processElement(DoFn<KV<String, Iterable<String>>, Void>.ProcessContext c)
							throws Exception {
						StringBuffer buf = new StringBuffer();
						for (String v : c.element().getValue()) {
							buf.append(v);
							buf.append(" ");
						}
						LOG.info(c.element().getKey() + " : " + buf.toString().trim());
					}

				}).named("Output Log"));

		p.run();
	}
}
