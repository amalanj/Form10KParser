package com.csc.training.parse;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.FloatWritable;
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
		Mapper<NullWritable, ParseForm10kCustomRecord, ParseForm10kMapKey, FloatWritable> {

	static Log log = LogFactory.getLog(ParseForm10kMapper.class);

	@Override
	public void map(NullWritable key, ParseForm10kCustomRecord values,
			OutputCollector<ParseForm10kMapKey, FloatWritable> output, Reporter reporter)
			throws IOException {

		ParseForm10kMapKey mapKey = null;
		FloatWritable mapValue = null;

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
			
			//int assetStartIndex = 0, assetStopIndex = 0, assetTotal = 0, nonAssetTotal = 0;
			Map<String, Integer> skipIndexMap = new HashMap<String, Integer>();

			for (int outerArray = 0; outerArray < trtd2D.length; outerArray++) {
				if (trtd2D[outerArray][0].toLowerCase().startsWith("Current assets:".toLowerCase().trim())) {
					skipIndexMap.put("assetStartIndex", Integer.valueOf(outerArray+1));
				} else if (trtd2D[outerArray][0].toLowerCase().startsWith("Total current assets".toLowerCase().trim())) {
					skipIndexMap.put("assetTotal", Integer.valueOf(outerArray));
				} else if(trtd2D[outerArray][0].toLowerCase().startsWith("Total non-current assets".toLowerCase())){
					skipIndexMap.put("nonAssetTotal", Integer.valueOf(outerArray));
				} else if(trtd2D[outerArray][0].toLowerCase().startsWith("NON-CURRENT ASSETS:".toLowerCase())){
					skipIndexMap.put("nonAsset", Integer.valueOf(outerArray));
				} else if (trtd2D[outerArray][0].toLowerCase().startsWith("Current liabilities:".toLowerCase().trim())) {
					skipIndexMap.put("assetStopIndex", Integer.valueOf(outerArray-2));
				}
			}
			for (int outerArray = skipIndexMap.get("assetStartIndex"); outerArray <= skipIndexMap.get("assetStopIndex"); outerArray++) {
				if(outerArray != skipIndexMap.get("assetTotal") 
						&& (skipIndexMap.get("nonAssetTotal") == null || outerArray != skipIndexMap.get("nonAssetTotal")) 
						&& (skipIndexMap.get("nonAsset") == null || outerArray != skipIndexMap.get("nonAsset"))){
					/*int col1 = Integer.parseInt(trtd2D[0][1].trim()
							.substring(trtd2D[0][1].trim().length() - 4,
									trtd2D[0][1].trim().length()));
					int col2 = Integer.parseInt(trtd2D[0][2].trim()
							.substring(trtd2D[0][2].trim().length() - 4,
									trtd2D[0][2].trim().length()));*/
					int col1 = Integer.parseInt(trtd2D[0][1].substring(9,13));
					int col2 = Integer.parseInt(trtd2D[0][2].substring(9,13));
					String val1 = trtd2D[outerArray][1];
					val1 = convert(val1);
					if(val1.trim().length() > 1){
						mapKey = new ParseForm10kMapKey(companyName, col1);
						mapValue = new FloatWritable();
						mapValue.set(Float.parseFloat(val1.trim()));
						output.collect(mapKey, mapValue);
					}
					
					String val2 = trtd2D[outerArray][2];
					val2 = convert(val2);				
					if(val2.trim().length() > 1){
						mapKey = new ParseForm10kMapKey(companyName, col2);
						mapValue = new FloatWritable();
						mapValue.set(Float.parseFloat(val2.trim()));
						output.collect(mapKey, mapValue);
					}
				}
				
			}
			
		}

	}

	private String convert(String val) {
		
		val = val.replaceAll(Matcher.quoteReplacement("$"), "");
		val = val.replaceAll(",", "");
		if(val.trim().startsWith("(") && val.trim().endsWith(")")){
			val = val.replace(Matcher.quoteReplacement("("), "-");
			val = val.replace(Matcher.quoteReplacement(")"), "");
		}
		return val;
	}

}
