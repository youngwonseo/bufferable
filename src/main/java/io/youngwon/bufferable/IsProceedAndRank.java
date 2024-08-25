package io.youngwon.bufferable;

public class IsProceedAndRank {
    private Boolean isProceed;
    private Long rank;

    static IsProceedAndRank of(Boolean isProceed, Long rank) {
        IsProceedAndRank instance = new IsProceedAndRank();
        instance.isProceed = isProceed;
        instance.rank = rank;
        return instance;
    }

    static IsProceedAndRank ofFail() {
        IsProceedAndRank instance = new IsProceedAndRank();
        instance.isProceed = false;
        instance.rank = -1L;
        return instance;
    }


    public Boolean getIsProceed() {
        return isProceed;
    }

    public Long getRank() {
        return rank;
    }

    @Override
    public String toString() {
        return "IsProceedAndRank{" +
                "isProceed=" + isProceed +
                ", rank=" + rank +
                '}';
    }
}
