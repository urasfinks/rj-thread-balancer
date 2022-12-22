package ru.jamsys.thread.balancer;

import lombok.Data;

import java.util.HashMap;
import java.util.Map;

@Data
public class ThreadBalancerAggregateStatisticData {
    Map<String, ThreadBalancerStatisticData> map = new HashMap<>();
}
