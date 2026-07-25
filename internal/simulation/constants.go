package simulation

// ChannelBufferSize is the depth of each per-direction transport channel.
// 1024 messages is large enough to absorb a 5-second delay at 200 msg/s
// without backpressure. Tighter budgets can be set by callers who care.
const ChannelBufferSize = 1024

// Default initial process count when no config is provided.
const DefaultInitialProcesses = 3

// MaxSimulationProcesses caps the number of processes in a single
// simulation. Above this the vector clock maps become O(N²) in memory.
const MaxSimulationProcesses = 100
