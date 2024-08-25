package io.youngwon.bufferable;

public interface Bufferable {

    IsProceedAndRank isProceed(String token);

    Boolean isDone(String token);

    Long getWaitSize();

    Long moveWaitToProceed();

}
