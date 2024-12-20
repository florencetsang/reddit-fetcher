package com.realtimefin.redditfetcher;

import com.realtimefin.redditfetcher.stream.CommentsStreamReader;
import com.realtimefin.redditfetcher.stream.PostsStreamReader;
import com.realtimefin.redditfetcher.stream.Reddit;
import com.realtimefin.redditfetcher.model.CommentSourceRecordConverter;
import com.realtimefin.redditfetcher.model.PostSourceRecordConverter;
import com.realtimefin.redditfetcher.stream.StreamReader;
import com.realtimefin.redditfetcher.version.Version;

import net.dean.jraw.models.Comment;
import net.dean.jraw.models.Submission;
import net.dean.jraw.pagination.Stream;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class RedditSourceTask extends SourceTask {

    private static final Logger log = LoggerFactory.getLogger(RedditSourceTask.class);

    private AtomicReference<Throwable> error;
    private List<StreamReader<?>> streamReaders;

    @Override
    public void start(Map<String, String> props) {
        this.error = new AtomicReference<>();

        RedditSourceConnectorConfig config = new RedditSourceConnectorConfig(props);
        Reddit reddit = config.createClient();
        Stream<Submission> postsStream = reddit.posts(config.getPostSubreddits());
        Stream<Comment> commentsStream = reddit.comments(config.getCommentSubreddits());

        Collection<Map<String, Object>> postPartitions = new HashSet<>();
        for (String postsSubreddit : config.getPostSubreddits()) {
            postPartitions.add(PostSourceRecordConverter.sourcePartition(postsSubreddit));
        }
        Collection<Map<String, Object>> commentPartitions = new HashSet<>();
        for (String commentsSubreddit : config.getCommentSubreddits()) {
            commentPartitions.add(CommentSourceRecordConverter.sourcePartition(commentsSubreddit));
        }

        Map<Map<String, Object>, Map<String, Object>> postOffsets =
                context.offsetStorageReader().offsets(postPartitions);
        Map<Map<String, Object>, Map<String, Object>> commentOffsets =
                context.offsetStorageReader().offsets(commentPartitions);

        streamReaders = new ArrayList<>();

        if (postsStream != null) {
            PostsStreamReader postsReader = new PostsStreamReader(
                    postOffsets,
                    postsStream,
                    this::onError,
                    config.getPostSubreddits(),
                    config.getPostsTopic()
            );
            postsReader.startReaderThread();
            streamReaders.add(postsReader);
        }
        if (commentsStream != null) {
            CommentsStreamReader commentsReader = new CommentsStreamReader(
                    commentOffsets,
                    commentsStream,
                    this::onError,
                    config.getCommentSubreddits(),
                    config.getCommentsTopic()
            );
            commentsReader.startReaderThread();
            streamReaders.add(commentsReader);
        }
    }

    @Override
    public List<SourceRecord> poll() {
        if (error.get() != null) {
            throw new ConnectException("Error occurred while reading from Reddit", error.get());
        }

        if (streamReaders == null) {
            log.warn("poll() invoked after task has been stopped; ignoring");
            return Collections.emptyList();
        }

        return streamReaders.stream().map(StreamReader::pollRecords).flatMap(List::stream).collect(Collectors.toList());
    }

    @Override
    public void stop() {
        streamReaders.forEach(StreamReader::close);
        streamReaders = null;
    }

    @Override
    public String version() {
        return Version.get();
    }

    private void onError(Throwable t) {
        this.error.compareAndSet(null, t);
    }
}
