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
	setupLog();
	createBuilder();
	startSubprocess();
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

    private void setupLog() {
	LOG.setLevel(Level.TRACE);
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
    
    private void startSubprocess() {
	try {
	    stopSubprocess();
	    LOG.debug("Starting subprocess");
	    subprocess = builder.start();
	    stdin  = new OutputStreamWriter(subprocess.getOutputStream());
	    stdout = new BufferedReader(new InputStreamReader(subprocess.getInputStream()));
	    stderr = new BufferedReader(new InputStreamReader(subprocess.getErrorStream()));
	    startStderrListener();
	} catch (IOException e) {
	    LOG.error(e);
	}
    }

    private void stopSubprocess() {
	try {
	    if (subprocess == null) { return; };
	    LOG.debug("Stopping subprocess");
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
		    LOG.trace("Waiting for subprocess to terminate...");
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
	try {
	    stdin.write(tuple.getString(0));
	    stdin.write("\n");
	    stdin.flush();
	    return true;
	} catch (IOException e) {
	    collector.reportError(e);
	    LOG.error("Error writing to stdin of subprocess", e);
	    startSubprocess();
	    return false;
	} catch (RuntimeError e) {
	    collector.reportError(e);
	    LOG.error(e);
	    startSubprocess();
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
