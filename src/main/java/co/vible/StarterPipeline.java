/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package co.vible;


import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.bigquery.model.JsonObject;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import co.vible.dataflow.models.CouchbaseModel;
import co.vible.dataflow.utils.FieldSchemaListBuilder;



/**
 * A starter example for writing Beam programs.
 *
 * <p>The example takes two strings, converts them to their upper-case
 * representation and logs them.
 *
 * <p>To run this starter example locally using DirectRunner, just
 * execute it without any additional parameters from your favorite development
 * environment.
 *
 * <p>To run this starter example using managed resource in Google Cloud
 * Platform, you should specify the following command-line options:
 *   --project=<YOUR_PROJECT_ID>
 *   --stagingLocation=<STAGING_LOCATION_IN_CLOUD_STORAGE>
 *   --runner=DataflowRunner
 * mvn compile exec:java -Dexec.mainClass=co.vible.StarterPipeline -Dexec.args="--inputFile=/Users/jay/vible/workspace/vible_dataflow/dataflow/src/main/resources/vible.example.json --output=results" -Pdirect-runner
 * gradle clean execute -DmainClass=co.vible.StarterPipeline -Dexec.args="--inputFile=/Users/jay/vible/workspace/vible_dataflow/dataflow/src/main/resources/vible.example.json --output=results" -Pdirect-runner
 */
public class StarterPipeline {
  private static final Logger LOG = LoggerFactory.getLogger(StarterPipeline.class);


  	static ObjectMapper mapper = new ObjectMapper();

  
	public interface CouchbaseJsonOptions extends PipelineOptions {

		@Description("Location of sample json.")
		@Default.String("gs://vible-couchbase")
		String getLoadingBucketURL();
		void setLoadingBucketURL(String loadingBucketURL);

		@Description("Big Query table name")
		String getBigQueryTablename();
		void setBigQueryTablename(String bigQueryTablename);

		@Description("Overwrite BigQuery table")
		@Default.Boolean(false)
		Boolean getOverwriteBigQueryTable();
		void setOverwriteBigQueryTable(Boolean overwriteBigQueryTable);
		
	}

	private static TableSchema bqSchema() {
		FieldSchemaListBuilder fieldSchemaListBuilder = new FieldSchemaListBuilder();

		fieldSchemaListBuilder.stringField("id")
			.stringField("_class")
			.stringField("type")
			.stringField("subtype")
			.timestampField("created")
			.timestampField("updated");
			

		return fieldSchemaListBuilder.schema();
	}

	public static void main(String[] args) {

		CouchbaseJsonOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(CouchbaseJsonOptions.class);

		Pipeline p = Pipeline.create(options);

		p.apply("ReadLines", TextIO.read().from(options.getLoadingBucketURL()))
		.apply(MapElements.via(new SimpleFunction<String, CouchbaseModel>() {
			@Override
			public CouchbaseModel apply(String input) {
				//LOG.info("input : {}", input);
				try{
					CouchbaseModel model = new CouchbaseModel();
					Map<String, Object> map = mapper.readValue(input, HashMap.class);
					model.setMap(map);
					return model;
				} catch(Exception e){
					return null;
				}	
			}
		}))
		.apply(ParDo.of(new ParseEventFn()))
		.apply(BigQueryIO.writeTableRows()
			.to(options.getBigQueryTablename()).withSchema(bqSchema())
			.withCustomGcsTempLocation(StaticValueProvider.of(options.getTempLocation()))
			.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
			.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
		);

		p.run();
	
	
	}

	static class ParseEventFn extends DoFn<CouchbaseModel, TableRow> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            c.output(mapToTableRows(c.element()));
        }
    }

	static TableRow mapToTableRows(CouchbaseModel model){
		TableRow row = new TableRow();
		
		model.getMap().forEach((key, value)->{
			
			if( (value!=null) && 
				("id".equals(key) || "_class".equals(key) || "type".equals(key) || "subtype".equals(key) )){

				row.set(key, value.toString());
			}
				
		
				
		});
		return row;
	}
}
