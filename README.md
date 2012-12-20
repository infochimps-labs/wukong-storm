# Wukong Storm

## Usage

The Wukong Storm plugin is very basic at the moment. It functions entirely over STDIN and STDOUT. Taken from the `wu-storm` executable:

```
usage: wu-storm PROCESSOR|FLOW [...--param=value...]

wu-storm is a commandline tool for running Wukong processors and flows in
a storm or trident topology.

wu-storm operates over STDIN and STDOUT and has a one-to-one message guarantee.
For example, when using an identity processor, wu-storm, given an event 'foo', will return
'foo\n|\n'. The '|' character is the specified End-Of-File delimiter.

If there is ever a suppressed error in processing, or a skipped record for any reason,
wu-storm will still respond with a '|\n', signifying an empty return event.

If there are multiple messages that have resulted from a single event, wu-storm will return
them newline separated, followed by the delimite, e.g. 'foo\nbar\nbaz\n|\n'.


Params:
   -t, --delimiter=String       The EOF specifier when returning events [Default: |]
   -r, --run=String             Name of the processor or dataflow to use. Defaults to basename of the given path
```

## TODO

The configuration file has __all__ of the options for storm listed. Slowly translating into real Configliere options.
