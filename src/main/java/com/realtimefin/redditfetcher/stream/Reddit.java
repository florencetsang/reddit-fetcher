package com.realtimefin.redditfetcher.stream;

import com.realtimefin.redditfetcher.version.Version;
import net.dean.jraw.ApiException;
import net.dean.jraw.RedditClient;
import net.dean.jraw.http.NetworkAdapter;
import net.dean.jraw.http.OkHttpNetworkAdapter;
import net.dean.jraw.http.UserAgent;
import net.dean.jraw.models.Comment;
import net.dean.jraw.models.Submission;
import net.dean.jraw.models.SubredditSort;
import net.dean.jraw.oauth.Credentials;
import net.dean.jraw.oauth.OAuthHelper;
import net.dean.jraw.pagination.Stream;
import net.dean.jraw.references.SubredditReference;

import java.util.List;
import java.util.UUID;

public class Reddit {

    private static final UserAgent userAgent = new UserAgent(
            "kafka",
            "com.github.florencetsang.real-time-finance",
            Version.get(),
            "florencetsang"
    );

    private static final UUID DEVICE_ID = UUID.randomUUID();

    private final int limit;
    private final RedditClient reddit;

    public Reddit(String oAuthClientId, int limit, boolean logHttpRequests) {
        this.limit = limit;
        this.reddit = createClient(oAuthClientId, logHttpRequests);
    }

    static RedditClient createClient(String oAuthClientId, boolean logHttpRequests) {
        Credentials credentials = Credentials.userless("pkCmk3d-QXk9POcAZDK0zQ", "-PcvrHFlGyOZ-vxlZwrQpsPfT4aygw", DEVICE_ID);;

        // This is what really sends HTTP requests
        NetworkAdapter adapter = new OkHttpNetworkAdapter(userAgent);

        // Authenticate and get a RedditClient instance
        RedditClient result = OAuthHelper.automatic(adapter, credentials);
//        RedditClient result = OAuthHelper.automatic(
//                new OkHttpNetworkAdapter(userAgent),
//                Credentials.userlessApp(oAuthClientId, DEVICE_ID)
//        );
        result.setLogHttp(logHttpRequests);
        return result;
    }

    public boolean canAccessSubreddit(String subreddit) {
        try {
            reddit.subreddit(subreddit).about();
            return true;
        } catch (ApiException e) {
            return false;
        }
    }

    public Stream<Comment> comments(List<String> subreddits) {
        SubredditReference multireddit = subreddits(subreddits);
        return multireddit != null ?
                multireddit.comments().limit(limit).build().stream()
                : null;
    }

    public Stream<Submission> posts(List<String> subreddits) {
        SubredditReference multireddit = subreddits(subreddits);
        return multireddit != null ?
                multireddit.posts().limit(limit).sorting(SubredditSort.NEW).build().stream()
                : null;
    }

    private SubredditReference subreddits(List<String> subreddits) {
        switch (subreddits.size()) {
            case 0:
                return null;
            case 1:
                return reddit.subreddit(subreddits.get(0));
            case 2:
                return reddit.subreddits(subreddits.get(0), subreddits.get(1));
            default: {
                String[] others = subreddits.subList(2, subreddits.size()).toArray(new String[0]);
                return reddit.subreddits(subreddits.get(0), subreddits.get(1), others);
            }
        }
    }
}
