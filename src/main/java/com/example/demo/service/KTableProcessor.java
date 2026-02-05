package com.example.demo.service;

import com.example.demo.config.AggregateTotalSerdes;
import com.example.demo.model.AggregateTotal;
import com.example.demo.model.Car;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.Stores;
import org.springframework.stereotype.Component;

@Component
public class KTableProcessor {
    public void process(KStream<String, Car> stream){
        KeyValueBytesStoreSupplier dealerSales = Stores.persistentKeyValueStore("dealer-sales-amount");
        KGroupedStream<String,Double> salesByDealerId = stream
                .map((key,sales) ->new KeyValue<>(sales.getDealerId(),Double.parseDouble(sales.getPrice())))
                .groupByKey();
        KTable<String, AggregateTotal> dealerAggregate = salesByDealerId.aggregate(() ->
                new AggregateTotal(), (k,v,aggregate) ->{
            aggregate.setAmount(aggregate.getAmount());
            aggregate.setCount(aggregate.getCount()+1);
            return aggregate;
        }, Materialized.with(Serdes.String(),new AggregateTotalSerdes()));
        final KTable<String, Double> dealerTotal = dealerAggregate.mapValues(value -> value.getAmount(),Materialized.as(dealerSales));
    }
}
