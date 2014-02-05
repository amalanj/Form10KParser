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

		String companyName = values.getCompanyName();
		String inUnits = values.getUnits();
		String actualContent = values.getActualContent();
		

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
					if (! trtd2D[outerArray][0].toLowerCase().startsWith("Total".toLowerCase().trim()) 
							|| trtd2D[outerArray][0].contains(":")){
						for(int i=1; i< trtd2D[0].length; i++){
							int col = Integer.parseInt(trtd2D[0][i].substring(9,13));
							String val = trtd2D[outerArray][i];
							val = convert(val);
							if(val.trim().length() > 1){
								mapKey = new ParseForm10kMapKey(companyName, col);
								mapValue = new FloatWritable();
								Float floatVal = Float.parseFloat(val.trim());
								if(inUnits.trim().equalsIgnoreCase("Thousands".trim())){
									floatVal = floatVal / 1000;
								}
								mapValue.set(floatVal);
								output.collect(mapKey, mapValue);
							}
						}
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
