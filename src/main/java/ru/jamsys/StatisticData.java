package ru.jamsys;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import ru.jamsys.thread.balancer.ThreadBalancerStatistic;

import java.util.Map;

//@JsonAlias({"name", "wName"})
//@JsonIgnore
//@JsonProperty("cpu_load")

@Data
public class StatisticData {

    @JsonProperty("timestamp")
    public long timestamp = System.currentTimeMillis();
    public double cpu;

    Map<String, ThreadBalancerStatistic> service = null;
    Map<String, Integer> share = null;
}
