package com.csc.training.parse;

import java.io.IOException;
import java.util.regex.Matcher;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class ParseForm10kMapper extends MapReduceBase implements
		Mapper<NullWritable, ParseForm10kCustomRecord, ParseForm10kMapKey, IntWritable> {

	static Log log = LogFactory.getLog(ParseForm10kMapper.class);

	@Override
	public void map(NullWritable key, ParseForm10kCustomRecord values,
			OutputCollector<ParseForm10kMapKey, IntWritable> output, Reporter reporter)
			throws IOException {

		ParseForm10kMapKey mapKey = null;
		IntWritable mapValue = null;

		String actualContent = null;
		String companyName = values.getCompanyName();
		actualContent = values.getActualContent();

		if (actualContent != null && actualContent.trim().length() > 0) {
			Document doc = Jsoup.parse(actualContent);
			Element table = doc.select("table").first();
			String[][] trtd2D = null;
			Elements trs = table.select("tr");
			trtd2D = new String[trs.size()][];
			for (int i = 0; i < trs.size(); i++) {
				Elements ths = trs.get(i).select("th");
				if (ths.size() > 0) {
					trtd2D[i] = new String[ths.size()];
					for (int j = 0; j < ths.size(); j++) {
						trtd2D[i][j] = ths.get(j).text();
					}
					continue;
				}
				Elements tds = trs.get(i).select("td");
				trtd2D[i] = new String[tds.size()];
				for (int j = 0; j < tds.size(); j++) {
					trtd2D[i][j] = tds.get(j).text();
				}
			}
			
			int assetStartIndex = 0, assetStopIndex = 0, assetTotal = 0;

			for (int outerArray = 0; outerArray < trtd2D.length; outerArray++) {
				if (trtd2D[outerArray][0].indexOf("Current assets:") != -1) {
					assetStartIndex = outerArray + 1;
				} else if (trtd2D[outerArray][0].indexOf("Total current assets") != -1) {
					assetTotal = outerArray;
				} else if (trtd2D[outerArray][0].indexOf("Current liabilities:") != -1) {
					assetStopIndex = outerArray - 2;
				}
			}
			for (int outerArray = assetStartIndex; outerArray <= assetStopIndex; outerArray++) {
				if(outerArray != assetTotal){
					int col1 = Integer.parseInt(trtd2D[0][1].trim()
							.substring(trtd2D[0][1].trim().length() - 4,
									trtd2D[0][1].trim().length()));
					int col2 = Integer.parseInt(trtd2D[0][2].trim()
							.substring(trtd2D[0][2].trim().length() - 4,
									trtd2D[0][2].trim().length()));
					
					String val1 = trtd2D[outerArray][1];
					val1 = val1.replaceAll(Matcher.quoteReplacement("$"), "");
					val1 = val1.replaceAll(",", "");
										
					mapKey = new ParseForm10kMapKey(companyName, col1);
					mapValue = new IntWritable();;
					mapValue.set(Integer.parseInt(val1.trim()));
					output.collect(mapKey, mapValue);
					
					String val2 = trtd2D[outerArray][2];
					val2 = val2.replaceAll(Matcher.quoteReplacement("$"), "");
					val2 = val2.replaceAll(",", "");
					
					mapKey = new ParseForm10kMapKey(companyName, col2);
					mapValue = new IntWritable();
					mapValue.set(Integer.parseInt(val2.trim()));
					output.collect(mapKey, mapValue);
				}
				
			}
			
		}

	}

}
