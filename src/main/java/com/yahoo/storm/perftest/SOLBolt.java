/*
 * Copyright (c) 2013 Yahoo! Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

package com.yahoo.storm.perftest;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.sun.javafx.scene.control.skin.VirtualFlow;
import com.yahoo.storm.perftest.data.DataTuple;
import com.yahoo.storm.perftest.data.PartialQueryResult;
import jdk.nashorn.internal.ir.debug.ObjectSizeCalculator;

public class SOLBolt extends BaseRichBolt {
  private OutputCollector _collector;
  LinkedBlockingQueue<DataTuple> dataTuples;
  List<PartialQueryResult> partialQueryResultList;
  PartialQueryResult partialQueryResult;
  private String [] _messages = null;
  private Random _rand = null;
  int index = 0;
  long startTime = 0;

  public SOLBolt() {
    //Empty
  }

  @Override
  public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
    _collector = collector;

//    _rand = new Random();
//    final int differentMessages = 1300;
//    _messages = new String[differentMessages];
//    for(int i = 0; i < differentMessages; i++) {
//      StringBuilder sb = new StringBuilder(300000);
//      //Even though java encodes strings in UCS2, the serialized version sent by the tuples
//      // is UTF8, so it should be a single byte
//      for(int j = 0; j < 300000; j++) {
//        sb.append(_rand.nextInt(9));
//      }
//      _messages[i] = sb.toString();
//    }
//    System.out.println("messages size :" + _messages[1].getBytes().length);
//    System.out.println("messages size :" + ObjectSizeCalculator.getObjectSize(_messages[1]));




    final int tuples = 4 * 1024;
    dataTuples = new LinkedBlockingQueue<>();
    partialQueryResultList = new ArrayList<>();
    for(int j = 0; j < 1300; j++){
      partialQueryResult = new PartialQueryResult(4096);
      for (int i = 0; i < tuples; i++) {
        Random random = new Random();
        int id = i;
        double x = random.nextDouble() + 116;
        double y = random.nextDouble() + 39;
        int z = random.nextInt(4096) + 10000;
        long t = System.currentTimeMillis();
        DataTuple tuple = new DataTuple(id, z, x, y, t);
        partialQueryResult.dataTuples.add(tuple);
        //            partialQueryResult.dataTuples.add(tuple);
      }
      System.out.println("Partial result bytes : " + ObjectSizeCalculator.getObjectSize(partialQueryResult));
      partialQueryResultList.add(partialQueryResult);
    }

  }

  @Override
  public void execute(Tuple input) {
    System.out.println("Running Bolt1!");
    System.out.println(input.getSourceStreamId());
    if (input.getSourceStreamId().equals("spout")){
      System.out.println("Receive message");
      _collector.ack(input);

      startTime = System.currentTimeMillis();
//      while(index < _messages.length){
//        final String message = _messages[index++];
//        _collector.emit("bolt1", new Values(message, false));
//      }
//      _collector.emit("bolt1", new Values(null, true));


      while (partialQueryResultList.size() > 0){

        PartialQueryResult partialQueryResult = partialQueryResultList.get(0);
        _collector.emit("bolt1", new Values(partialQueryResult, false));
        partialQueryResultList.remove(0);
//        System.out.println("Emit message");
      }
      _collector.emit("bolt1", new Values(null, true));

    }else if(input.getSourceStreamId().equals("bolt2")){
      long elapseTime = System.currentTimeMillis() - startTime;
      System.out.println("Elapse time : " + elapseTime + "ms");
    }



  }

  @Override
  public void cleanup() {
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declareStream("bolt1", new Fields("message", "flag"));

  }
}
