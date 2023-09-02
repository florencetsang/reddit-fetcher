package com.realtimefina.redditfetcher.stream;

import com.realtimefina.redditfetcher.model.PostSourceRecordConverter;
import net.dean.jraw.models.Submission;
import net.dean.jraw.pagination.Stream;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class PostsStreamReader extends StreamReader<Submission> {

    private final PostSourceRecordConverter recordConverter;

    public PostsStreamReader(
            Map<Map<String, Object>, Map<String, Object>> offsets,
            Stream<Submission> stream,
            Consumer<Throwable> onError,
            List<String> subreddits,
            String topic
    ) {
        super(offsets, stream, onError, "posts", subreddits);
        this.recordConverter = new PostSourceRecordConverter(topic);
    }

    @Override
    protected SourceRecord convertThing(Submission submission) {
        return recordConverter.convert(submission);
    }

    @Override
    protected String subredditForThing(Submission submission) {
        return submission.getSubreddit();
    }

    @Override
    protected Map<String, Object> partitionForSubreddit(String subreddit) {
        return recordConverter.sourcePartition(subreddit);
    }

    @Override
    protected Date dateForThing(Submission submission) {
        return submission.getCreated();
    }
}
