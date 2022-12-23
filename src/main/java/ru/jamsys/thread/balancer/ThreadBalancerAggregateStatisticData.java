package ru.jamsys.thread.balancer;

import lombok.Data;

import java.util.HashMap;
import java.util.Map;

@Data
public class ThreadBalancerAggregateStatisticData {

    String name = getClass().getSimpleName();

    Map<String, ThreadBalancerStatisticData> map = new HashMap<>();
}
