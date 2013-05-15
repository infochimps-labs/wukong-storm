package com.infochimps.wukong.storm;

import java.util.Map;
import java.util.Iterator;
import java.io.*;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import backtype.storm.tuple.Values;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

// env.put("LANG",   "en_US.UTF-8");
// env.put("LC_ALL", "en_US.UTF-8");
// env.put("PATH",   "/usr/local/bin:/usr/local/sbin:/opt/chef/embedded/bin/:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin");

public class SubprocessFunction extends BaseFunction {
    
    private static Logger LOG = Logger.getLogger(SubprocessFunction.class);
    
    protected String      directory;
    protected Map         environment;
    protected String[]    args;

    private ProcessBuilder      builder;
    private Process             subprocess;
    private OutputStreamWriter  stdin;
    private BufferedReader      stdout;
    private BufferedReader      stderr;

    private String              batchTerminator = "---";

    public SubprocessFunction(String directory, Map environment, String[] args) {
	this.directory   = directory;
	this.environment = environment;
	this.args        = args;
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
	createBuilder();
    }

    @Override
    public void cleanup() {
	stopSubprocess();
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
	if (!sendThroughDataflow(tuple, collector)) { return; };
	collectOutputFromDataflow(collector);
    }

    private void createBuilder() {
	builder =  new ProcessBuilder(subprocessArgs());
	builder.directory(new File(directory));

	Iterator env = subprocessEnvironment().entrySet().iterator();
	while (env.hasNext()) {
	    Map.Entry pair = (Map.Entry) env.next();
	    builder.environment().put((String) pair.getKey(), (String) pair.getValue());
	}
    }

    private String[] subprocessArgs() {
	return args;
    }

    private Map subprocessEnvironment() {
	return environment;
    }
    
    private void startSubprocess(TridentCollector collector) {
	try {
	    stopSubprocess();
	    LOG.info("Starting in directory:    " + builder.directory());
	    LOG.info("Running command: " + builder.command());
	    subprocess = builder.start();
	    stdin  = new OutputStreamWriter(subprocess.getOutputStream());
	    stdout = new BufferedReader(new InputStreamReader(subprocess.getInputStream()));
	    stderr = new BufferedReader(new InputStreamReader(subprocess.getErrorStream()));
	    startStderrListener();
	} catch (IOException e) {
	    LOG.error(e);
	    collector.reportError(e);
	}
    }

    private void stopSubprocess() {
	try {
	    if (subprocess == null) { return; };
	    LOG.info("Stopping...");
	    stdin.close();
	    stdout.close();
	    stderr.close();
	    subprocess.destroy();
	} catch (IOException e) {
	    LOG.error(e);
	}
    }

    private void startStderrListener() {
	new Thread("stderr") {
	    public void run() {
		String line;
		try {
		    while ((line = stderr.readLine()) != null) { LOG.debug(line); };
		    int exitStatus = subprocess.waitFor();
		    LOG.debug("Subprocess terminated unexpectedly with status " + exitStatus);
		} catch (IOException e) {
		    LOG.error("Error reading from stderr of subprocess", e);
		} catch (InterruptedException e) {
		    LOG.warn("Received exit signal, terminating.");
		}
	    }
	}.start();
    }
    
    private Boolean sendThroughDataflow(TridentTuple tuple, TridentCollector collector) {
	if (subprocess == null) startSubprocess(collector);

	if (stdin == null) {
	    return false;
	}
	try {
	    String line = tuple.getString(0);
	    stdin.write(line);
	    stdin.write("\n");
	    stdin.flush();
	    return true;
	} catch (IOException e) {
	    collector.reportError(e);
	    LOG.error("Error writing to stdin of subprocess", e);
	    startSubprocess(collector);
	    return false;
	} catch (RuntimeException e) {
	    collector.reportError(e);
	    LOG.error(e);
	    startSubprocess(collector);
	    return false;
	}
    }

    private void collectOutputFromDataflow(TridentCollector collector) {
	try {
	    String line = null, read = null;
	    while (((read = stdout.readLine()) != null) && !read.equals(batchTerminator)) {
		line = read;
		collector.emit(new Values(read));
	    }
	    if (read == null) {
		LOG.error("Unexpected EOF from subprocess, last record was: " + line);
	    }
	} catch (IOException e) {
	    LOG.error("Error reading from stdout of subprocess", e);
	}
    }
    
}
