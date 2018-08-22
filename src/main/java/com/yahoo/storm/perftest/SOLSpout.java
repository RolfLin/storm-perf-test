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

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import com.yahoo.storm.perftest.data.DataTuple;
import com.yahoo.storm.perftest.data.PartialQueryResult;
import jdk.nashorn.internal.ir.debug.ObjectSizeCalculator;

public class SOLSpout extends BaseRichSpout {
  private int _sizeInBytes;
  private long _messageCount;
  private SpoutOutputCollector _collector;
  private String [] _messages = null;
  private boolean _ackEnabled;
  private Random _rand = null;

  LinkedBlockingQueue<DataTuple> dataTuples;
  List<PartialQueryResult> partialQueryResultList;
  PartialQueryResult partialQueryResult;

  int index = 0;
  public SOLSpout(int sizeInBytes, boolean ackEnabled) {
    if(sizeInBytes < 0) {
      sizeInBytes = 0;
    }
    _sizeInBytes = sizeInBytes;
    _messageCount = 0;
    _ackEnabled = ackEnabled;
  }

  public boolean isDistributed() {
    return true;
  }

  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    _rand = new Random();
    _collector = collector;
//    final int differentMessages = 100;
//    _messages = new String[differentMessages];
//    for(int i = 0; i < differentMessages; i++) {
//      StringBuilder sb = new StringBuilder(_sizeInBytes);
//      //Even though java encodes strings in UCS2, the serialized version sent by the tuples
//      // is UTF8, so it should be a single byte
//      for(int j = 0; j < _sizeInBytes; j++) {
//        sb.append(_rand.nextInt(9));
//      }
//      _messages[i] = sb.toString();
//    }
//    System.out.println("messages size :" + _messages[1].getBytes().length);
//    System.out.println("messages size :" + ObjectSizeCalculator.getObjectSize(_messages[1]));


//    final int tuples = 4 * 1024;
//    dataTuples = new LinkedBlockingQueue<>();
//    partialQueryResultList = new ArrayList<>();
//    for(int j = 0; j < 1300; j++){
//      partialQueryResult = new PartialQueryResult(4096);
//      for (int i = 0; i < tuples; i++) {
//        Random random = new Random();
//        int id = i;
//        double x = random.nextDouble() + 116;
//        double y = random.nextDouble() + 39;
//        int z = random.nextInt(4096) + 10000;
//        long t = System.currentTimeMillis();
//        DataTuple tuple = new DataTuple(id, z, x, y, t);
//  //            tuple.add(id);
//  //            tuple.add(x);
//  //            tuple.add(y);
//  //            tuple.add(t);
//
////        try {
////          dataTuples.put(tuple);
////        } catch (InterruptedException e) {
////          e.printStackTrace();
////        }
//        partialQueryResult.dataTuples.add(tuple);
//        //            partialQueryResult.dataTuples.add(tuple);
//      }
//      System.out.println("Partial result bytes : " + ObjectSizeCalculator.getObjectSize(partialQueryResult));
//      partialQueryResultList.add(partialQueryResult);
//    }
  }

  @Override
  public void close() {
    //Empty
  }

  @Override
  public void nextTuple() {
//    final String message = _messages[_rand.nextInt(_messages.length)];
//    if(_ackEnabled) {
//      _collector.emit(new Values(message), _messageCount);
//    } else {



//    while (partialQueryResultList.size() > 0){
//
//      PartialQueryResult partialQueryResult = partialQueryResultList.get(0);
//      _collector.emit(new Values(partialQueryResult));
//      partialQueryResultList.remove(0);
//      System.out.println("Emit message");
//    }

    if (index > 0){

      Utils.sleep(120000);
    }
    _collector.emit("spout", new Values("Message from spout"));
    System.out.println("Message send from spout");
    index ++;
   //    }
//    _messageCount++;
  }


  @Override
  public void ack(Object msgId) {
    //Empty
  }

  @Override
  public void fail(Object msgId) {
    //Empty
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declareStream("spout", new Fields("message"));
  }
}
