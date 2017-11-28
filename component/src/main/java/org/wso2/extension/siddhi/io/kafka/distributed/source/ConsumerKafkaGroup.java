/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.extension.siddhi.io.kafka.distributed.source;

import org.apache.log4j.Logger;
import org.wso2.siddhi.core.stream.input.source.SourceEventListener;
import org.wso2.siddhi.query.api.exception.SiddhiAppValidationException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;

import static java.lang.Math.abs;
import static java.lang.Math.min;

/**
 * This processes the Kafka messages using a thread pool.
 */
public class ConsumerKafkaGroup {
    private static final Logger LOG = Logger.getLogger(
            ConsumerKafkaGroup.class);
    private static volatile LinkedBlockingQueue<String> eventsList0 = new LinkedBlockingQueue<>();
    private static volatile LinkedBlockingQueue<String> eventsList1 = new LinkedBlockingQueue<>();
    private static volatile LinkedBlockingQueue<String> eventsList2 = new LinkedBlockingQueue<>();
    private static volatile LinkedBlockingQueue<String> eventsList3 = new LinkedBlockingQueue<>();
    private static volatile LinkedBlockingQueue<String> outputList = new LinkedBlockingQueue<>();
    private final String topics[];
    private final String partitions[];
    private final Properties props;
    private Boolean orderEnabled;
    private List<KafkaConsumerThreadDistributed> kafkaConsumerThreadDistributedList = new ArrayList<>();
    private Map<String, Map<Integer, Long>> topicOffsetMap = new HashMap<>();
    private ScheduledExecutorService executorService;
    private Map<String, Map<SequenceKey, Integer>> perConsumerLastReceivedSeqNo = new HashMap<>();

    ConsumerKafkaGroup(String topics[], String partitions[], Properties props, Map<String, Map<Integer, Long>>
            topicOffsetMap, Map<String, Map<SequenceKey, Integer>> perConsumerLastReceivedSeqNo, String threadingOption,
                       ScheduledExecutorService executorService, Boolean orderEnabled) {
        this.orderEnabled = orderEnabled;
        this.topicOffsetMap = topicOffsetMap;
        this.perConsumerLastReceivedSeqNo = perConsumerLastReceivedSeqNo;
        this.topics = topics;
        this.partitions = partitions;
        this.props = props;
        this.executorService = executorService;
    }

    void pause() {
        kafkaConsumerThreadDistributedList.forEach(KafkaConsumerThreadDistributed::pause);
    }

    void resume() {
        kafkaConsumerThreadDistributedList.forEach(KafkaConsumerThreadDistributed::resume);
    }

    void restore(final Map<String, Map<Integer, Long>> topic) {
        kafkaConsumerThreadDistributedList
                .forEach(kafkaConsumerThreadDistributed -> kafkaConsumerThreadDistributed.restore(topic));
    }

    void shutdown() {
        kafkaConsumerThreadDistributedList.forEach(KafkaConsumerThreadDistributed::shutdownConsumer);
    }

    void run(SourceEventListener sourceEventListener) {
        try {
            int len;
            if (orderEnabled && topics.length == 1 && partitions.length > 1) {
                KafkaConsumerThreadDistributed kafkaConsumerThreadDistributed =
                        new KafkaConsumerThreadDistributed(topics, partitions, props,
                                                           topicOffsetMap, false,
                                                           eventsList0, eventsList1, eventsList2,
                                                           eventsList3);
                LOG.info("Kafka Consumer thread starting to listen on topic/s: " + Arrays.toString(topics) +
                                 " with partition/s: " + Arrays.toString(partitions));
                kafkaConsumerThreadDistributedList.add(kafkaConsumerThreadDistributed);
                len = partitions.length;

            } else if (orderEnabled && topics.length > 1 && partitions.length == 1) {
                KafkaConsumerThreadDistributed kafkaConsumerThreadDistributed =
                        new KafkaConsumerThreadDistributed(topics, partitions, props,
                                                           topicOffsetMap, false,
                                                           eventsList0, eventsList1, eventsList2,
                                                           eventsList3);
                LOG.info("Kafka Consumer thread starting to listen on topic/s: " + Arrays.toString(topics) +
                                 " with partition/s: " + Arrays.toString(partitions));
                kafkaConsumerThreadDistributedList.add(kafkaConsumerThreadDistributed);
                len = topics.length;

            } else {
                throw new SiddhiAppValidationException("This ordering cannot be applied to multiple topics containing"
                                                               + " multiple partitions");
                //TODO : Do it for Multiple partitioned topics
            }
            for (KafkaConsumerThreadDistributed consumerThread : kafkaConsumerThreadDistributedList) {
                if (perConsumerLastReceivedSeqNo != null) {
                    Map<SequenceKey, Integer> seqNoMap = perConsumerLastReceivedSeqNo
                            .get(consumerThread.getConsumerThreadId());
                    seqNoMap = (seqNoMap != null) ? seqNoMap : new HashMap<>();
                    consumerThread.setLastReceivedSeqNoMap(seqNoMap);
                }

                executorService.submit(consumerThread);
            }

            if (len == 2) {
                Reorder
                        reorder2 = new Reorder(eventsList0, eventsList1, outputList, 2);
                reorder2.start();

            } else if (len == 3) {
                Reorder reorder3 =
                        new Reorder(eventsList0, eventsList1, eventsList2, outputList, 3);
                reorder3.start();

            } else if (len == 4) {
                Reorder
                        reorder4 = new Reorder(eventsList0, eventsList1, eventsList2, eventsList3,
                                               outputList, 4);
                reorder4.start();
            }

            while (true) {
                String message = outputList.take();

                sourceEventListener.onEvent(message, new String[0]);
            }


        } catch (Throwable t) {
            LOG.error("Error while creating KafkaConsumerThread for topic/s: " + Arrays.toString(topics), t);
        }
    }

    public Map<String, Map<Integer, Long>> getTopicOffsetMap() {
        Map<String, Map<Integer, Long>> topicOffsetMap = new HashMap<>();
        for (KafkaConsumerThreadDistributed kafkaConsumerThreadDistributed : kafkaConsumerThreadDistributedList) {
            Map<String, Map<Integer, Long>> topicOffsetMapTemp = kafkaConsumerThreadDistributed.getTopicOffsetMap();
            for (Map.Entry<String, Map<Integer, Long>> entry : topicOffsetMapTemp.entrySet()) {
                topicOffsetMap.put(entry.getKey(), entry.getValue());
            }
        }
        return topicOffsetMap;
    }

    public Map<String, Map<SequenceKey, Integer>> getPerConsumerLastReceivedSeqNo() {
        if (perConsumerLastReceivedSeqNo != null) {
            for (KafkaConsumerThreadDistributed consumer : kafkaConsumerThreadDistributedList) {
                perConsumerLastReceivedSeqNo.put(consumer.getConsumerThreadId(), consumer.getLastReceivedSeqNoMap());
            }
        }
        return perConsumerLastReceivedSeqNo;
    }
}


/**
 * This processes the Kafka messages and order them.
 */
class Reorder extends Thread {

    private static final Logger log = Logger.getLogger(Reorder.class);
    private volatile LinkedBlockingQueue<String> inputList1;
    private volatile LinkedBlockingQueue<String> inputList2;
    private volatile LinkedBlockingQueue<String> inputList3;
    private volatile LinkedBlockingQueue<String> inputList4;
    private volatile LinkedBlockingQueue<String> outputList;
    private Integer listNum;

    Reorder(LinkedBlockingQueue<String> inputList1, LinkedBlockingQueue<String> inputList2,
            LinkedBlockingQueue<String> outputList, Integer listNum) {
        this.inputList1 = inputList1;
        this.inputList2 = inputList2;
        this.outputList = outputList;
        this.listNum = listNum;

    }

    Reorder(LinkedBlockingQueue<String> inputList1, LinkedBlockingQueue<String> inputList2,
            LinkedBlockingQueue<String> inputList3,
            LinkedBlockingQueue<String> outputList, Integer listNum) {
        this.inputList1 = inputList1;
        this.inputList2 = inputList2;
        this.inputList3 = inputList3;
        this.outputList = outputList;
        this.listNum = listNum;

    }

    Reorder(LinkedBlockingQueue<String> inputList1, LinkedBlockingQueue<String> inputList2,
            LinkedBlockingQueue<String> inputList3, LinkedBlockingQueue<String> inputList4,
            LinkedBlockingQueue<String> outputList, Integer listNum) {
        this.inputList1 = inputList1;
        this.inputList2 = inputList2;
        this.inputList3 = inputList3;
        this.inputList4 = inputList4;
        this.outputList = outputList;
        this.listNum = listNum;

    }


    public void run() {
        try {
            synchronized (this) {

                double s1, s2, s3 = 0, s4 = 0;
                String event1 = "", event2 = "", event3 = "", event4 = "";


                event1 = inputList1.take();
                s1 = Double.parseDouble((event1.split(","))[0].split(":")[2]);
                event2 = inputList2.take();
                s2 = Double.parseDouble((event2.split(","))[0].split(":")[2]);

                if (listNum >= 3) {
                    event3 = inputList3.take();
                    s3 = Double.parseDouble((event3.split(","))[0].split(":")[2]);

                    if (listNum == 4) {
                        event4 = inputList4.take();
                        s4 = Double.parseDouble((event4.split(","))[0].split(":")[2]);
                    }
                }

                while (true) {
                    double minNum;
                    if (listNum == 4) {
                        minNum = min(min(s1, s2), min(s3, s4));
                    } else if (listNum == 3) {
                        minNum = min(min(s1, s2), s3);
                    } else {
                        minNum = min(s1, s2);
                    }
                    if (abs(minNum - s1) == 0) {
                        outputList.put(event1);
                        event1 = inputList1.take();
                        s1 = Double.parseDouble((event1.split(","))[0].split(":")[2]);
//
                    } else if (abs(minNum - s2) == 0) {
                        outputList.put(event2);
                        event2 = inputList2.take();
                        s2 = Double.parseDouble((event2.split(","))[0].split(":")[2]);
                    } else if (listNum >= 3) {
                        if (abs(minNum - s3) == 0) {
                            outputList.put(event3);
                            event3 = inputList3.take();
                            s3 = Double.parseDouble((event3.split(","))[0].split(":")[2]);

                        } else if (listNum == 4) {
                            if (abs(minNum - s4) == 0) {
                                outputList.put(event4);
                                event4 = inputList4.take();
                                s4 = Double.parseDouble((event4.split(","))[0].split(":")[2]);
                            }
                        }
                    }
                }

            }
        } catch (InterruptedException e1) {
            log.error("Error ", e1);
        }
    }
}
