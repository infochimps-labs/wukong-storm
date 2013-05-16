# Wukong-Storm

The Hadoop plugin for Wukong lets you run <a
href="http://github.com/infochimps-labs/wukong/tree/3.0.0">Wukong</a>
processors and dataflows as <a
href="https://github.com/nathanmarz/storm">Storm</a> topologies reading data in and out from <a href="http://kafka.apache.org/">Kafka</a>.

Before you use Wukong-Storm to develop, test, and write your Hadoop
jobs, you might want to read about <a
href="http://github.com/infochimps-labs/wukong/tree/3.0.0">Wukong</a>,
write some <a
href="http://github.com/infochimps-labs/wukong/tree/3.0.0#writing-simple-processors">simple
processors</a>, and read about some of Storm's <a
href="https://github.com/nathanmarz/storm/wiki/Concepts">core
concepts</a>.

You might also want to check out some other projects which enrich the
Wukong and Hadoop experience:

* <a href="http://github.com/infochimps-labs/wukong-hadoop">wukong-hadoop</a>: Run Wukong processors and dataflows as mappers and/or reducers within the Hadoop framework.  Model jobs locally before you run them.
* <a href="http://github.com/infochimps-labs/wukong-load">wukong-load</a>: Load the output data from your local Wukong jobs and flows into a variety of different data stores.
* <a href="http://github.com/infochimps-labs/wukong-deploy">wukong-deploy</a>: Orchestrate Wukong and other wu-tools together to support an application running on the Infochimps Platform.

<a name="installation"></a>
## Installation & Setup

Wukong-Storm can be installed as a RubyGem:

```
$ sudo gem install wukong-storm
```

If you actually want to run your dataflows as functioning Storm
topologies reading/writing to/from Kafka, you'll of course need access
to Storm and Kafka installations.  <a
href="http://github.com/infochimps-labs/ironfan">Ironfan</a> is a
great tool for building and managing Storm clusters and other
distributed infrastructure quickly and easily.

To run Storm jobs through Wukong-Storm, you'll need to move your your
Wukong code to each worker of the Storm cluster, install Wukong-Storm
on each, and log in and launch your job fron one of them.  Ironfan
again helps with configuring this.

<a name="anatomy"></a>
## Anatomy of a running topology

Storm defines the concept of a **topology**.  A topology contains
spouts and bolts.  A **spout** is a source of data.  A **bolt**
processes data.  Bolts can be connected to each other and to spouts in
arbitrary ways.

Tooplogies submitted to Storm's Nimbus but run within a Storm
supervisor.  Each supervisor can dedicate a certain number of
**workers** to a topology. Within each worker, **parallelism**
controls the number of threads the worker assigns to the topology.

Wukong-Storm runs each Wukong dataflow as a single bolt within a
single topology.  Data is passed to this bolt over STDIN and collected
over STDOUT, similar to the way <a
href="http://hadoop.apache.org/docs/r0.15.2/streaming.html">Hadoop
streaming </a> operates.

This topology is hooked up to a
`storm.kafka.trident.OpaqueTridentKafkaSpout` (part of
[storm-contrib](https://github.com/nathanmarz/storm-contrib)) which
reads from a single input topic within Kafka.

Output records are written to a default Kafka topic but this can be
overridden on a per-record basis.

<a name="protocol"></a>
### Communication protocol

A Wukong dataflow launched within Storm runs as a single bolt (see
[`com.infochimps.wukong.storm.SubprocessFunction`](https://github.com/infochimps-labs/wukong-storm/blob/master/src/main/java/com/infochimps/wukong/storm/SubprocessFunction.java)).
This bolt works by launching an arbitrary command-line and sending it
records over STDIN and reading its output over STDOUT.  The
`SubprocessFunction` class expects whatever command it launched to
obey a protocol under which the output after **each** input consists
of each output record followed by a newline, with the full batch of
output records followed by a batch terminator (default: `---`) then
another newline.

Wukong-Storm comes with a command `wu-bolt` which works very similarly
to `wu-local` but implements this protocol.  Here's an example of
using `wu-bolt` directly with a processor:

```
$ echo 2 | wu-bolt prime_factorizer.rb
2
---
$ echo 12 | wu-bolt prime_factorizer.rb
2
2
3
---
$ echo 19 | wu-bolt prime_factorizer.rb
---
```

Notice that in the last example, the presence of the batch delimiter
after each input record make it easy to tell the difference between
"no output records" and "no output records yet" which, over
STDIN/STDOUT, is rather hard to tell otherwise.

## Running a dataflow

### A simple processor

Assuming you have correctly installed Wukong-Storm, Storm, Kafka,
Zookeeper, &c., and you have defined a simple dataflow (or in this
case, just a single processor) like this:

```ruby
# in upcaser.rb
Wukong.processor(:upcaser) do
  def process line
    yield line.upcase
  end
end
```

Then you can launch it directly into Storm:

```
$ wu-storm upcaser.rb --input=some_input_topic --output=some_output_topic
```

If a topology named `upcaser` already exists, you'll get an error.
Add the `--rm` flag to first kill the running topology before
launching the new one:

```
$ wu-storm upcaser.rb --input=some_input_topic --output=some_output_topic --rm
```

The default amount of time to wait for the topology to die is 300
seconds (5 minutes), just like the `storm kill` command (which is used
under the hood).  When debugging a topology in development, it's
helpful to add `--wait=1` to immediately kill the topology.

See exactly what happened behind the scenes by adding the `--dry_run`
flag which will print commands and not execute them:

```
$ wu-storm upcaser.rb --input=some_input_topic --output=some_output_topic --rm --dry_run
```

### A more complicated example

Say you have a dataflow:

```ruby
# in my_flow.rb
Wukong.dataflow(:my_flow) do
  my_parser | does_something | then_something_else | to_json
end
```

You can launch it using a different topology name as well as target
arbitrary locations for your Zookeeper, Kafka, and Storm servers:

```
$ wu-storm my_flow.rb --name=my_flow_attempt_3 --zookeeper_hosts=10.121.121.121,10.122.122.122 --kafka_hosts=10.123.123.123 --nimbus_host=10.124.124.124 --input=some_input_topic --output=some_output_topic
```

### Running non-Wukong or non-Ruby code

You can also use Wukong-Storm as a harness to run non-Wukong or
non-Ruby code.  As long as you can specificy a command-line to run
which supports the [communication protocol](#protocol), then you can
run it with `wu-storm`:

```
$ wu-storm --bolt_command='my_cmd --some-option=value -af -q 3' --input=some_input_topic --output=some_output_topic
```

### Scaling options

Storm provides several options for scaling up or down a topology.
Wukong-Storm makes them accessible at launch time via the following
options:

* `--workers` specify the number of workers (a.k.a. "executors" or "slots") for the topology.  Defaults to 1.
* `--input_parallelism` specify the number of threads within the spout reading from Kafka within each worker.  Defaults to 1.
* `--parallelism` specify the number of threads within the bolt running Wukong code within each worker.  Defaults to 1.
