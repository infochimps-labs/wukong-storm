# Wukong Storm

## TODO

The configuration file has __all__ of the options for storm listed. Slowly translating into real Configliere options.

`wu-storm` the executable needs to be finished. The execution of which must satisfy the guarantee of 1 record in, 1 record out and have a agreed upon serialization method. In order to get there, code will be copied from `wu-local` and and the `TCPScoketServer` in Wukong core. This should get us close enough to be coded against.