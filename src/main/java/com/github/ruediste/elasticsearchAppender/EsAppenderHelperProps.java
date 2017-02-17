package com.github.ruediste.elasticsearchAppender;

public interface EsAppenderHelperProps {

    EsAppenderHelper getHelper();

    default void setMaxStringLength(String maxStringLength) {
        getHelper().maxStringLength = Integer.valueOf(maxStringLength);
    }

    default void setIndexPattern(String indexPattern) {
        getHelper().indexPattern = indexPattern;
    }

    default void setTags(String tags) {
        getHelper().tags = tags;
    }

    default void setLabels(String labels) {
        getHelper().labels = labels;
    }
}
