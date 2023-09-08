package com.realtimefin.redditfetcher.model;

import org.apache.kafka.connect.source.SourceRecord;

public interface SourceRecordConverter<Thing> {

    SourceRecord convert(Thing thing);

}
