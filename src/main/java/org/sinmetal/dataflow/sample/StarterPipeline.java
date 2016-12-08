package org.sinmetal.dataflow.sample;

import static com.google.datastore.v1.client.DatastoreHelper.makeValue;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.atilika.kuromoji.ipadic.Token;
import com.atilika.kuromoji.ipadic.Tokenizer;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.io.datastore.DatastoreIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionTuple;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.cloud.dataflow.sdk.values.TupleTagList;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.Key.Builder;
import com.google.datastore.v1.Key.PathElement;

public class StarterPipeline {
	private static final Logger LOG = LoggerFactory.getLogger(StarterPipeline.class);

	// private static final String GCS_PATH =
	// "gs://cp300demo1.appspot.com/twitter/t20160721.json";
	private static final String GCS_PATH = "gs://cp300demo1.appspot.com/test.txt";
	private static final String INVALID_RECORD_GCS_PATH = "gs://cpb100demo1.appspot.com/invalid";

	private static final String PROJECT_ID = "cpb100demo1";

	final static TupleTag<KV<String, String>> packageObjects = new TupleTag<KV<String, String>>() {
		private static final long serialVersionUID = -5464035818684971907L;
	};

	final static TupleTag<String> invalidRecord = new TupleTag<String>() {
		private static final long serialVersionUID = 6506421296431928150L;
	};

	public static void main(String[] args) {
		Pipeline p = Pipeline.create(PipelineOptionsFactory.fromArgs(args).withValidation().create());

		PCollectionTuple results = p.apply(TextIO.Read.named("ReadMyFile").from(GCS_PATH))

				.apply(ParDo.withOutputTags(packageObjects, TupleTagList.of(invalidRecord))
						.of(new DoFn<String, KV<String, String>>() {

							private static final long serialVersionUID = 1L;

							@Override
							public void processElement(ProcessContext c) {
								Tokenizer tokenizer = new Tokenizer();
								List<Token> tokens = tokenizer.tokenize(c.element());
								for (Token token : tokens) {
									if (token.getAllFeaturesArray().length < 1
											|| token.getAllFeaturesArray()[0].equals("記号")) {
										c.sideOutput(invalidRecord, token.getSurface());
									} else {
										c.output(KV.of(token.getAllFeaturesArray()[0], token.getSurface()));
									}
								}
							}
						}).named("Tokenizer"));

		results.get(packageObjects).apply(GroupByKey.<String, String> create())
				.apply(ParDo.of(new DoFn<KV<String, Iterable<String>>, Entity>() {
					private static final long serialVersionUID = 1L;

					@Override
					public void processElement(DoFn<KV<String, Iterable<String>>, Entity>.ProcessContext c)
							throws Exception {
						StringBuffer buf = new StringBuffer();
						for (String v : c.element().getValue()) {
							buf.append(v);
							buf.append(" ");
						}
						LOG.info(c.element().getKey() + ":" + buf.toString().trim());

						Builder builder = Key.newBuilder();
						PathElement pathElement = builder.addPathBuilder().setKind("Word").setName(c.element().getKey())
								.build();
						Key key = builder.setPath(0, pathElement).build();

						Entity.Builder entityBuilder = Entity.newBuilder();
						entityBuilder.setKey(key).getMutableProperties().put("content",
								makeValue(buf.toString().trim()).build());
						Entity entity = entityBuilder.build();
						c.output(entity);
					}
				}).named("TokenToEntity")).apply(DatastoreIO.v1().write().withProjectId(PROJECT_ID));

		results.get(invalidRecord).apply(ParDo.named("InvalidRecord").of(new DoFn<String, String>() {
			private static final long serialVersionUID = -5953251432569167072L;

			@Override
			public void processElement(ProcessContext c) {
				LOG.warn(c.element());
				c.output(c.element());
			}
		})).apply(TextIO.Write.named("WriteInvalidRecordFile").to(INVALID_RECORD_GCS_PATH));

		p.run();

	}

	static class ComputeWordLengths extends PTransform<PCollection<String>, PCollection<Integer>> {
		private static final long serialVersionUID = 8778220848749131734L;

		@Override
		public PCollection<Integer> apply(PCollection<String> input) {
			return super.apply(input);
		}

	}
}
