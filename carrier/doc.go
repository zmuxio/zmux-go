// Package carrier holds the building blocks of carrier adapters: Pool
// shares underlying connections (HTTP/2 and HTTP/3 request streams, QUIC
// streams) between pipes within PoolLimits, ChannelDemux routes the
// datagrams of a connection shared by several pipes to their channels,
// and WriteFlat turns a batch of buffers into one write for a writer whose
// every call is costly.
package carrier
