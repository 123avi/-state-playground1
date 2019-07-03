package org.example

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._
import org.apache.flink.contrib.streaming.state.{PredefinedOptions, RocksDBStateBackend}


/**
 * Skeleton for a Flink Job.
 *
 * For a full example of a Flink Job, see the WordCountJob.scala file in the
 * same package/directory or have a look at the website.
 *
 * You can also generate a .jar file that you can submit on your Flink
 * cluster. Just type
 * {{{
 *   sbt clean assembly
 * }}}
 * in the projects root directory. You will find the jar in
 * target/scala-2.11/Flink\ Project-assembly-0.1-SNAPSHOT.jar
 *
 */
object Job {
  def main(args: Array[String]): Unit = {
    // set up the execution environment
    val stateDir = "file:///tmp/lookalike-df/"
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val b = new RocksDBStateBackend(stateDir,true)
    env.setStateBackend(b)

    // get input data
    val words = ("To be, or not to be,--that is the question:-- Whether 'tis nobler in the mind to suffer, " +
      "The slings and arrows of outrageous fortune Or to take arms against a sea of troubles").toLowerCase.split(("\\W+")).toList
    val text: DataStream[String] = env.fromElements(words: _*)

    val counts = text
      .keyBy(w => w)
      .process(new FancyCounter)

    // execute and print result
    counts.print()

    // execute program
    env.execute("Flink Scala API Skeleton")
  }
}
case class WordCnt(word: String, count: Int)
class FancyCounter extends KeyedProcessFunction[String, String, WordCnt]{
  val counterDescriptor = new ValueStateDescriptor("seen_domains", Types.CASE_CLASS[WordCnt])
  lazy val counterState = getRuntimeContext.getState(counterDescriptor)

  override def processElement(value: String, ctx: KeyedProcessFunction[String, String, WordCnt]#Context, out: Collector[WordCnt]): Unit = {
    val w = if (null == counterState.value()) {
      WordCnt(value, 1)
    } else {
      val tmp: WordCnt = counterState.value()
      tmp.copy(count = tmp.count + 1)
    }
    counterState.update(w)
    out.collect(w)
  }
}