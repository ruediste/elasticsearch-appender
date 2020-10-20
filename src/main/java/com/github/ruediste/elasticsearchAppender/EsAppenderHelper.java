package com.github.ruediste.elasticsearchAppender;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.function.LongConsumer;

public class EsAppenderHelper {
	public String indexPattern = "'logstash-'yyyy.MM.dd";
	private DateTimeFormatter indexFormatter;
	public int maxStringLength = 10 * 1024;

	/**
	 * Comma separated tags of all log records
	 */
	public String tags;

	/**
	 * Labels to apply to log records in the format
	 * 
	 * <pre>
	 * key1=value1,key2=value2,...
	 * </pre>
	 */
	public String labels;

	public void start() {
		indexFormatter = DateTimeFormatter.ofPattern(indexPattern);
	}

	public void stop() {
	}

	public void prepareLogRecord(EsLogRecord record, long timeStamp) {
		calcNextTimestamp(timeStamp, x -> record.time = x, x -> record.timeAdjustment = x);
		if (tags != null && tags.length() > 0) {
			record.tags = new LinkedHashSet<>();
			for (String tag : tags.split(",")) {
				record.tags.add(tag);
			}
		}
		if (labels != null && labels.length() > 0) {
			record.labels = new LinkedHashMap<>();
			for (String label : labels.split(",")) {
				int idx = label.indexOf('=');
				record.labels.put(label.substring(0, idx), label.substring(idx + 1));
			}
		}
	}

	public String getIndex(long timeStamp) {
		ZonedDateTime timeStampUtc = ZonedDateTime.ofInstant(Instant.ofEpochMilli(timeStamp), ZoneOffset.UTC);
		return indexFormatter.format(timeStampUtc);
	}

	/**
	 * Truncate a string to {@link #maxStringLength}
	 */
	public String truncate(String s) {
		if (s == null) {
			return null;
		}
		if (s.length() > maxStringLength) {
			return s.substring(0, maxStringLength) + "...";
		}
		return s;
	}

	public String getStackTrace(Throwable aThrowable) {
		final Writer result = new StringWriter();
		final PrintWriter printWriter = new PrintWriter(result);
		aThrowable.printStackTrace(printWriter);
		return truncate(result.toString());
	}

	private static ThreadLocal<Long> lastLogTimeStamp = new ThreadLocal<>();

	/**
	 * adjust time stamp such that only one event is logged for the same milli
	 * second
	 */
	public void calcNextTimestamp(long origTimeStamp, LongConsumer timestampConsumer, LongConsumer adjustmentConsumer) {
		long timeStamp = origTimeStamp;
		{
			Long lastStamp = lastLogTimeStamp.get();
			if (lastStamp != null && lastStamp >= timeStamp) {
				timeStamp = lastStamp + 1;
				adjustmentConsumer.accept(timeStamp - origTimeStamp);
			}
			lastLogTimeStamp.set(timeStamp);

			timestampConsumer.accept(timeStamp);
		}
	}

}
