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
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.Key.Builder;
import com.google.datastore.v1.Key.PathElement;

public class StarterPipeline {
	private static final Logger LOG = LoggerFactory.getLogger(StarterPipeline.class);

	// private static final String GCS_PATH =
	// "gs://cp300demo1.appspot.com/twitter/t20160721.json";
	private static final String GCS_PATH = "gs://cp300demo1.appspot.com/test.txt";
	
	private static final String PROJECT_ID = "cpb100demo1";

	public static void main(String[] args) {
		Pipeline p = Pipeline.create(PipelineOptionsFactory.fromArgs(args).withValidation().create());

		p.apply(TextIO.Read.named("ReadMyFile").from(GCS_PATH))

				.apply(ParDo.of(new DoFn<String, KV<String, String>>() {
					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public void processElement(ProcessContext c) {
						Tokenizer tokenizer = new Tokenizer();
						List<Token> tokens = tokenizer.tokenize(c.element());
						for (Token token : tokens) {
							c.output(KV.of(token.getAllFeaturesArray()[0], token.getSurface()));
						}
					}
				}).named("Tokenizer")).apply(GroupByKey.<String, String> create())
				.apply(ParDo.of(new DoFn<KV<String, Iterable<String>>, Entity>() {

					/**
					 * 
					 */
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

		p.run();
	}
}
