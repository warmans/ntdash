package main

import (
	"fmt"
	"math"
	"testing"
)

func TestParseStatus(t *testing.T) {
	rawStatus := `Datacenter: DC1
==================
Status=Up/Down
|/ State=Normal/Leaving/Joining/Moving
--  Address       Load       Tokens  Owns    Host ID                               Rack
UN  10.0.0.6   35.32 GB   256     ?       99ca9b90-ba59-4411-be56-aafcabedc9c6  5AB
UN  10.0.0.7   157.74 GB  256     50%     4da97bcf-9831-438b-863c-8a15a19a904e  5AE
UN  10.0.0.4   49.86 GB   256     ?       f62dd765-2fa8-49b2-8cc2-e10a3b90680b  5AB
UN  10.0.0.5   43.31 GB   256     ?       76881341-7f76-4ef8-b254-c446245830ca  5AB
UN  10.0.0.10  47.67 GB   256     ?       8450f8ec-f8c0-4b89-b9e2-67c88cda87b2  5AB
UN  10.0.0.11  32.24 GB   256     ?       bebf7e78-1376-411f-be27-60deae5af0b6  5AB
UN  10.0.0.8   148.5 GB   256     ?       283be4bb-20ad-428a-a6c0-04389b750ada  5AE
UN  10.0.0.9   173.03 GB  256     ?       8064c532-15f6-4b3b-9e85-6140ca93c9c5  6AB
UN  10.0.0.15  30.74 GB   256     ?       e23e5bbd-b8c8-4da2-ba2a-64c32e046413  5AB
UN  10.0.0.14  29.23 GB   256     ?       6aef2c77-7296-4cbe-a1c7-7b2faa03e1b3  5AB
DN  10.0.0.13  148.05 GB  256     ?       a08c5397-76df-4665-b942-1c99ce220189  6AB
UN  10.0.0.12  55.06 GB   256     ?       431c7e1f-a2a0-41be-b307-ebdad8a92147  5AB
UN  10.0.0.2   50.15 GB   256     ?       2dcabd19-8042-47df-a6be-c1611a34c1e6  5AB
UN  10.0.0.3   44.76 GB   256     ?       1c782853-3b32-470d-869b-099f48b277e3  5AB
UN  10.0.0.1   47.25 GB   256     ?       db28e0b4-b502-4c37-9c3a-45579987df89  5AB
Datacenter: DC2
======================
Status=Up/Down
|/ State=Normal/Leaving/Joining/Moving
--  Address       Load       Tokens  Owns    Host ID                               Rack
DN  10.1.0.4   94.79 GB   256     ?       b999feef-ef84-4e24-aed2-f1a2d3deb116  G4
UN  10.1.0.5   102.77 GB  256     ?       21b0c0f1-cf18-48aa-b802-8f8f1419c6c5  G5
UN  10.1.0.6   74.88 GB   256     ?       93fad3e9-c466-4f69-9bc9-ff5f4413f0b4  G4
UN  10.1.0.7   79.36 GB   256     ?       81d6b021-392c-418a-bf56-43d526126fcb  I7
UN  10.1.0.8   57.83 GB   256     ?       f1fefbe2-d33e-4f97-b242-1238e71a9f66  I8
DN  10.1.0.9   85.31 GB   256     ?       6a3a5f1f-0faa-4956-8b17-8098873363ff  I6
UN  10.1.0.10  56.06 GB   256     ?       38445d41-7b0e-47a5-855a-48fba9c72a44  I8
DN  10.1.0.11  95.92 GB   256     ?       e1336fb7-0c46-42a0-932d-350ab946c2bc  I7
UN  10.1.0.13  69.13 GB   256     ?       a2dba4e5-84d0-47b1-9d09-8018aec7b657  G4
DN  10.1.0.12  78.27 GB   256     ?       23c1a562-4bcb-4fbe-acef-84188e467833  I7
UN  10.1.0.15  66.79 GB   256     ?       4c6109f3-8a22-4350-a8d3-203c17920e9d  G4
UN  10.1.0.14  58.81 GB   256     ?       6791dc03-7752-42f8-bdaa-a694881a72b0  I8
DN  10.1.0.1   94.91 GB   256     ?       e31938bf-08e3-4018-91cd-35d1fb95be14  I8
DN  10.1.0.2   89.14 GB   256     ?       7ef73c6e-01cb-47da-a596-90a98e4bc191  I8
DN  10.1.0.3   86.81 GB   256     ?       7044a478-6594-4692-bc75-18625fe8dfd8  G4

Note: Non-system keyspaces don't have the same replication settings, effective ownership information is meaningless`

	nt := NewNodetool()
	status := nt.ParseStatus(rawStatus)

	if len(status.Datacenters) != 2 {
		fmt.Print(status.Datacenters)
		t.Errorf("List of DCs was incorrect %v", len(status.Datacenters))

		return
	}

	if len(status.Datacenters[0].Nodes) != 15 {
		t.Error("DC2 did not contain all 15 nodes")
		return
	}

	if len(status.Datacenters[1].Nodes) != 15 {
		t.Error("DC2 did not contain all 15 nodes")
		return
	}

	if status.Datacenters[0].Nodes[0].Address != "10.0.0.6" {
		t.Error("Node 0 does not have the correct Address")
		return
	}

	if status.Datacenters[0].Nodes[0].Load != "35.32 GB" {
		t.Error("Node 0 does not have the correct Load")
		return
	}

	if status.Datacenters[0].Nodes[0].Tokens != "256" {
		t.Error("Node 0 does not have the correct Tokens")
		return
	}

	if status.Datacenters[0].Nodes[0].Owns != "?" {
		t.Error("Node 0 does not have the correct Owns")
		return
	}

	if status.Datacenters[0].Nodes[0].HostID != "99ca9b90-ba59-4411-be56-aafcabedc9c6" {
		t.Error("Node 0 does not have the correct HostID")
		return
	}

	if status.Datacenters[0].Nodes[0].Rack != "5AB" {
		t.Error("Node 0 does not have the correct HostID")
		return
	}
}

func TestParseCfStats(t *testing.T) {
	rawData := `Keyspace: system_traces
    Read Count: 0
    Read Latency: NaN ms.
    Write Count: 0
    Write Latency: NaN ms.
    Pending Flushes: 0
    Table: events
        SSTable count: 0
        Space used (live): 0
        Space used (total): 0
        Space used by snapshots (total): 0
        SSTable Compression Ratio: 0.0
        Memtable cell count: 0
        Memtable data size: 0
        Memtable switch count: 0
        Local read count: 0
        Local read latency: NaN ms
        Local write count: 0
        Local write latency: NaN ms
        Pending flushes: 0
        Bloom filter false positives: 0
        Bloom filter false ratio: 0.00000
        Bloom filter space used: 0
        Compacted partition minimum bytes: 0
        Compacted partition maximum bytes: 0
        Compacted partition mean bytes: 0
        Average live cells per slice (last five minutes): 0.0
        Maximum live cells per slice (last five minutes): 0.0
        Average tombstones per slice (last five minutes): 0.0
        Maximum tombstones per slice (last five minutes): 0.0

    Table: sessions
        SSTable count: 0
        Space used (live): 0
        Space used (total): 0
        Space used by snapshots (total): 0
        SSTable Compression Ratio: 0.0
        Memtable cell count: 0
        Memtable data size: 0
        Memtable switch count: 0
        Local read count: 0
        Local read latency: NaN ms
        Local write count: 0
        Local write latency: NaN ms
        Pending flushes: 0
        Bloom filter false positives: 0
        Bloom filter false ratio: 0.00000
        Bloom filter space used: 0
        Compacted partition minimum bytes: 0
        Compacted partition maximum bytes: 0
        Compacted partition mean bytes: 0
        Average live cells per slice (last five minutes): 0.0
        Maximum live cells per slice (last five minutes): 0.0
        Average tombstones per slice (last five minutes): 0.0
        Maximum tombstones per slice (last five minutes): 0.0

----------------
Keyspace: system
    Read Count: 2711500
    Read Latency: 1.4712197562234925 ms.
    Write Count: 627466930
    Write Latency: 0.03867109357779222 ms.
    Pending Flushes: 1
    Table: IndexInfo
        SSTable count: 0
        Space used (live): 0
        Space used (total): 0
        Space used by snapshots (total): 0
        SSTable Compression Ratio: 0.0
        Memtable cell count: 0
        Memtable data size: 0
        Memtable switch count: 0
        Local read count: 0
        Local read latency: NaN ms
        Local write count: 0
        Local write latency: NaN ms
        Pending flushes: 0
        Bloom filter false positives: 0
        Bloom filter false ratio: 0.00000
        Bloom filter space used: 0
        Compacted partition minimum bytes: 0
        Compacted partition maximum bytes: 0
        Compacted partition mean bytes: 0
        Average live cells per slice (last five minutes): 0.0
        Maximum live cells per slice (last five minutes): 0.0
        Average tombstones per slice (last five minutes): 0.0
        Maximum tombstones per slice (last five minutes): 0.0

    Table: batchlog
        SSTable count: 0
        Space used (live): 0
        Space used (total): 0
        Space used by snapshots (total): 0
        SSTable Compression Ratio: 0.0
        Memtable cell count: 0
        Memtable data size: 0
        Memtable switch count: 0
        Local read count: 0
        Local read latency: NaN ms
        Local write count: 0
        Local write latency: NaN ms
        Pending flushes: 0
        Bloom filter false positives: 0
        Bloom filter false ratio: 0.00000
        Bloom filter space used: 0
        Compacted partition minimum bytes: 0
        Compacted partition maximum bytes: 0
        Compacted partition mean bytes: 0
        Average live cells per slice (last five minutes): 0.0
        Maximum live cells per slice (last five minutes): 0.0
        Average tombstones per slice (last five minutes): 0.0
        Maximum tombstones per slice (last five minutes): 0.0`

	nt := NewNodetool()
	stats := nt.ParseCfStats(rawData)

	if len(stats.Keyspaces) != 2 {
		t.Error("Expected 2 keyspaces in result. Actually ", len(stats.Keyspaces))
	}

	if stats.Keyspaces[0].ReadCount != 0 || stats.Keyspaces[1].ReadCount != 2711500 {
		t.Error("Keyspace ReadCount is incorrect ", stats.Keyspaces[0].ReadCount, stats.Keyspaces[1].ReadCount)
	}

	if stats.Keyspaces[0].ReadLatency != 0 || stats.Keyspaces[1].ReadLatency != 1.4712197562234925 {
		t.Error("Keyspace ReadLatency is incorrect ", stats.Keyspaces[0].ReadLatency, stats.Keyspaces[1].ReadLatency)
	}

	if stats.Keyspaces[0].WriteCount != 0 || stats.Keyspaces[1].WriteCount != 627466930 {
		t.Error("Keyspace WriteCount is incorrect ", stats.Keyspaces[0].WriteCount, stats.Keyspaces[1].WriteCount)
	}

	if stats.Keyspaces[0].WriteLatency != 0 || stats.Keyspaces[1].WriteLatency != 0.03867109357779222 {
		t.Error("Keyspace WriteLatency is incorrect ", stats.Keyspaces[0].WriteLatency, stats.Keyspaces[1].WriteLatency)
	}

	if stats.Keyspaces[0].PendingFlushes != 0 || stats.Keyspaces[1].PendingFlushes != 1 {
		t.Error("Keyspace PendingFlushes is incorrect ", stats.Keyspaces[0].PendingFlushes, stats.Keyspaces[1].PendingFlushes)
	}
}

func TestParseInfo(t *testing.T) {
	rawData := `ID               : db28e0b4-b502-4c37-9c3a-45579987df89
    Gossip active    : true
    Thrift active    : true
    Native Transport active: true
    Load             : 49.11 GB
    Generation No    : 1422527983
    Uptime (seconds) : 5186606
    Heap Memory (MB) : 3688.81 / 7916.00
    Data Center      : DC1
    Rack             : 5AB
    Exceptions       : 108
    Key Cache        : entries 3319, size 65.05 MB, capacity 100 MB, 23999063 hits, 29014197 requests, 0.827 recent hit rate, 14400 save period in seconds
    Row Cache        : entries 0, size 0 bytes, capacity 0 bytes, 0 hits, 0 requests, NaN recent hit rate, 7200 save period in seconds
    Counter Cache    : entries 0, size 0 bytes, capacity 50 MB, 0 hits, 0 requests, NaN recent hit rate, 7200 save period in seconds
    Token            : (invoke with -T/--tokens to see all 256 tokens)`

	nt := NewNodetool()
	info := nt.ParseInfo(rawData)

	if info.ID != "db28e0b4-b502-4c37-9c3a-45579987df89" {
		t.Error("ID is incorrect", info.ID)
	}

	if info.GossipActive != true {
		t.Error("GossipActive is incorrect", info.GossipActive)
	}

	if info.ThriftActive != true {
		t.Error("ThriftActive is incorrect", info.ThriftActive)
	}

	if info.NativeTransportActive != true {
		t.Error("NativeTransportActive is incorrect", info.NativeTransportActive)
	}

	if info.Load != "49.11 GB" {
		t.Error("Load is incorrect", info.Load)
	}

	if info.GenerationNo != 1422527983 {
		t.Error("GenerationNo is incorrect", info.GenerationNo)
	}

	if info.Uptime != 5186606 {
		t.Error("Uptime is incorrect", info.Uptime)
	}

	if info.HeapUsage != 46.599418898433555 {
		t.Error("HeapUsage is incorrect", info.HeapUsage)
	}

	if info.DataCenter != "DC1" {
		t.Error("DataCenter is incorrect", info.DataCenter)
	}

	if info.Rack != "5AB" {
		t.Error("Rack is incorrect", info.Rack)
	}

	if info.Exceptions != 108 {
		t.Error("Exceptions is incorrect", info.Exceptions)
	}

	if info.KeyCache.Entries != 3319 {
		t.Error("KeyCache.Entries is incorrect", info.KeyCache.Entries)
	}

	if info.KeyCache.Size != "65.05 MB" {
		t.Error("KeyCache.Size is incorrect", info.KeyCache.Size)
	}

	if info.KeyCache.Capacity != "100 MB" {
		t.Error("KeyCache.Size is incorrect", info.KeyCache.Capacity)
	}

	if info.KeyCache.Hits != 23999063 {
		t.Error("KeyCache.Size is incorrect", info.KeyCache.Hits)
	}

	if info.KeyCache.Requests != 29014197 {
		t.Error("KeyCache.Requests is incorrect", info.KeyCache.Requests)
	}

	if info.KeyCache.RecentHitRate != 0.827 {
		t.Error("KeyCache.RecentHitRate is incorrect", info.KeyCache.RecentHitRate)
	}

	if info.KeyCache.SavePeriod != 14400 {
		t.Error("KeyCache.SavePeriod is incorrect", info.KeyCache.SavePeriod)
	}

	if info.RowCache.Entries != 0 {
		t.Error("RowCache.Entries is incorrect", info.RowCache.Entries)
	}

	if info.RowCache.Size != "0 bytes" {
		t.Error("RowCache.Size is incorrect", info.RowCache.Size)
	}

	if info.RowCache.Capacity != "0 bytes" {
		t.Error("RowCache.Capacity is incorrect", info.RowCache.Capacity)
	}

	if info.RowCache.Hits != 0 {
		t.Error("RowCache.Hits is incorrect", info.RowCache.Hits)
	}

	if info.RowCache.Requests != 0 {
		t.Error("RowCache.Requests is incorrect", info.RowCache.Requests)
	}

	if math.IsNaN(info.RowCache.RecentHitRate) == false {
		t.Error("RowCache.RecentHitRate is incorrect", info.RowCache.RecentHitRate)
	}

	if info.RowCache.SavePeriod != 7200 {
		t.Error("RowCache.SavePeriod is incorrect", info.RowCache.SavePeriod)
	}
}
