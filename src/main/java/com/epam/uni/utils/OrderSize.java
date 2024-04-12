package com.epam.uni.utils;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@AllArgsConstructor
@RequiredArgsConstructor
public enum OrderSize {
    ERRONEOUS("erroneous", 0),
    TINY("tiny", 1),
    SMALL("small", 3),
    MEDIUM("medium", 10),
    LARGE("large");

    private final String value;
    private int orderSize;
}
