package main

import (
	"log"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
)

//Status is the result of nodetool status
type Status struct {
	Datacenters []Datacenter
}

func (s *Status) GetPcntUpNormal() int64 {
	numTotal := 0
	numUN := 0
	for _, dc := range s.Datacenters {
		for _, node := range dc.Nodes {
			if node.State == "UN" {
				numUN++
			}
			numTotal++
		}
	}

	return int64((float64(numUN) / float64(numTotal)) * 100)
}

//Datacenter is a component of nodetool status
type Datacenter struct {
	Name  string
	Nodes []Node
}

//Node is a component of nodetool status
type Node struct {
	State   string
	Address string
	Load    string
	Tokens  string
	Owns    string
	HostID  string
	Rack    string
}

//CfStats is the result of nodetool cfstats
type CfStats struct {
	Keyspaces []Keyspace
}

//GetAvgReadLatency returns the average read latency across all keyspaces
func (cfs *CfStats) GetAvgReadLatency() float64 {
	sum := 0.0
	for _, keyspace := range cfs.Keyspaces {
		sum += keyspace.ReadLatency
	}
	return sum / float64(len(cfs.Keyspaces))
}

//GetAvgWriteLatency returns the average write latency across all keyspaces
func (cfs *CfStats) GetAvgWriteLatency() float64 {
	sum := 0.0
	for _, keyspace := range cfs.Keyspaces {
		sum += keyspace.WriteLatency
	}
	return sum / float64(len(cfs.Keyspaces))
}

//Keyspace is the result of cfstats
type Keyspace struct {
	Name           string
	ReadCount      int64
	ReadLatency    float64
	WriteCount     int64
	WriteLatency   float64
	PendingFlushes int64
}

//Info is the result of nodetool info
type Info struct {
	ID                    string
	GossipActive          bool
	ThriftActive          bool
	NativeTransportActive bool
	Load                  string
	GenerationNo          int64
	Uptime                int64
	HeapUsage             float64
	DataCenter            string
	Rack                  string
	Exceptions            int64
	KeyCache              Cache
	RowCache              Cache
	CounterCache          Cache
}

//Cache stores information on a cache e.g. RowCache
type Cache struct {
	Entries       int64
	Size          string
	Capacity      string
	Hits          int64
	Requests      int64
	RecentHitRate float64
	SavePeriod    int64
}

//Nodetool provides acesss to nodetool data
type Nodetool struct {
}

func (nt *Nodetool) Execute(args ...string) string {
	out, err := exec.Command("nodetool", args...).Output()
	if err != nil {
		log.Fatal(err)
	}
	return string(out)
}

//GetStatus returns nodetool status result
func (nt *Nodetool) GetStatus() Status {
	return nt.ParseStatus(nt.Execute("status"))
}

//ParseStatus parses a raw nodetool status output
func (nt *Nodetool) ParseStatus(rawStatus string) Status {

	datacenters := make([]Datacenter, 0)

	for _, line := range strings.Split(rawStatus, "\n") {
		var datacenterPat = regexp.MustCompile(`^\s*Datacenter: (.+)$`)
		if dcParts := datacenterPat.FindAllStringSubmatch(line, 2); dcParts != nil {
			if len(dcParts[0]) != 2 {
				continue
			}

			datacenters = append(datacenters, Datacenter{Name: dcParts[0][1], Nodes: make([]Node, 0)})
			continue
		}

		if len(datacenters) < 1 {
			continue //without a DC we can't do much else
		}

		var nodePat = regexp.MustCompile(`^\s*([UD][NLJM])\s+([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+)\s+([0-9\.]+ [PGMK]?B)\s+([0-9]+)\s+([0-9\?\.\%]+)\s+([a-zA-Z0-9\-]+)\s+(.+)$`)
		if nodeParts := nodePat.FindAllStringSubmatch(line, 7); nodeParts != nil {
			if len(nodeParts[0]) != 8 {
				continue
			}
			datacenters[len(datacenters)-1].Nodes = append(datacenters[len(datacenters)-1].Nodes, Node{State: nodeParts[0][1], Address: nodeParts[0][2], Load: nodeParts[0][3], Tokens: nodeParts[0][4], Owns: nodeParts[0][5], HostID: nodeParts[0][6], Rack: nodeParts[0][7]})
			continue
		}

	}

	return Status{Datacenters: datacenters}
}

func (nt *Nodetool) GetCfStats() CfStats {
	return nt.ParseCfStats(nt.Execute("cfstats"))
}

//ParseCfStats parses a raw cfstats output
func (nt *Nodetool) ParseCfStats(rawData string) CfStats {
	keyspaces := make([]Keyspace, 0)
	for _, line := range strings.Split(rawData, "\n") {
		if parts := regexp.MustCompile(`^\s*Keyspace: (.+)$`).FindAllStringSubmatch(line, 2); parts != nil {
			//init new keyspace
			keyspaces = append(keyspaces, Keyspace{Name: parts[0][1]})
		}

		if len(keyspaces) < 1 {
			continue
		}
		curKeyspace := &keyspaces[len(keyspaces)-1]

		//parse keyspace stats
		if parts := regexp.MustCompile(`^\s*Read Count: ([0-9]+)$`).FindAllStringSubmatch(line, 2); parts != nil {
			curKeyspace.ReadCount, _ = strconv.ParseInt(parts[0][1], 10, 64)
		} else if parts := regexp.MustCompile(`^\s*Read Latency: ([0-9\.]+) ms\.$`).FindAllStringSubmatch(line, 2); parts != nil {
			curKeyspace.ReadLatency, _ = strconv.ParseFloat(parts[0][1], 64)
		} else if parts := regexp.MustCompile(`^\s*Write Count: ([0-9]+)$`).FindAllStringSubmatch(line, 2); parts != nil {
			curKeyspace.WriteCount, _ = strconv.ParseInt(parts[0][1], 10, 64)
		} else if parts := regexp.MustCompile(`^\s*Write Latency: ([0-9\.]+) ms.$`).FindAllStringSubmatch(line, 2); parts != nil {
			curKeyspace.WriteLatency, _ = strconv.ParseFloat(parts[0][1], 64)
		} else if parts := regexp.MustCompile(`^\s*Pending Flushes: ([0-9]+)$`).FindAllStringSubmatch(line, 2); parts != nil {
			curKeyspace.PendingFlushes, _ = strconv.ParseInt(parts[0][1], 10, 64)
		}

	}
	return CfStats{Keyspaces: keyspaces}
}

func (nt *Nodetool) ParseInfo(rawData string) Info {

	info := Info{}
	for _, line := range strings.Split(rawData, "\n") {
		//basic info
		if parts := regexp.MustCompile(`^\s*ID\s*: ([a-z0-9\-]+)$`).FindAllStringSubmatch(line, 2); parts != nil {
			info.ID = parts[0][1]
		} else if parts := regexp.MustCompile(`^\s*Gossip active\s*: (true|false)$`).FindAllStringSubmatch(line, 2); parts != nil {
			info.GossipActive, _ = strconv.ParseBool(parts[0][1])
		} else if parts := regexp.MustCompile(`^\s*Thrift active\s*: (true|false)$`).FindAllStringSubmatch(line, 2); parts != nil {
			info.ThriftActive, _ = strconv.ParseBool(parts[0][1])
		} else if parts := regexp.MustCompile(`^\s*Native Transport active\s*: (true|false)$`).FindAllStringSubmatch(line, 2); parts != nil {
			info.NativeTransportActive, _ = strconv.ParseBool(parts[0][1])
		} else if parts := regexp.MustCompile(`^\s*Load\s*: ([0-9\.]+ [KMGP]?B)$`).FindAllStringSubmatch(line, 2); parts != nil {
			info.Load = parts[0][1]
		} else if parts := regexp.MustCompile(`^\s*Generation No\s*: ([0-9]+)$`).FindAllStringSubmatch(line, 2); parts != nil {
			info.GenerationNo, _ = strconv.ParseInt(parts[0][1], 10, 64)
		} else if parts := regexp.MustCompile(`^\s*Uptime \(seconds\)\s*: ([0-9]+)$`).FindAllStringSubmatch(line, 2); parts != nil {
			info.Uptime, _ = strconv.ParseInt(parts[0][1], 10, 64)
		} else if parts := regexp.MustCompile(`^\s*Heap Memory \(MB\)\s*: ([0-9\.]+) / ([0-9\.]+)$`).FindAllStringSubmatch(line, 3); parts != nil {
			heapUsed, _ := strconv.ParseFloat(parts[0][1], 64)
			heapSize, _ := strconv.ParseFloat(parts[0][2], 64)
			info.HeapUsage = (heapUsed / heapSize) * 100
		} else if parts := regexp.MustCompile(`^\s*Data Center\s*: (.+)$`).FindAllStringSubmatch(line, 2); parts != nil {
			info.DataCenter = parts[0][1]
		} else if parts := regexp.MustCompile(`^\s*Rack\s*: (.+)$`).FindAllStringSubmatch(line, 2); parts != nil {
			info.Rack = parts[0][1]
		} else if parts := regexp.MustCompile(`^\s*Exceptions\s*: (.+)$`).FindAllStringSubmatch(line, 2); parts != nil {
			info.Exceptions, _ = strconv.ParseInt(parts[0][1], 10, 64)
		}

		cacheDataToCache := func(data []string) Cache {
			cache := Cache{Size: data[2], Capacity: data[3]}
			cache.Entries, _ = strconv.ParseInt(data[1], 10, 64)
			cache.Hits, _ = strconv.ParseInt(data[4], 10, 64)
			cache.Requests, _ = strconv.ParseInt(data[5], 10, 64)
			cache.SavePeriod, _ = strconv.ParseInt(data[7], 10, 64)

			//handle NaN
			if recentCacheHitRate, err := strconv.ParseFloat(data[6], 64); err == nil {
				cache.RecentHitRate = recentCacheHitRate
			}

			return cache
		}

		//caches
		cacheDataRegexp := `entries ([0-9]+), size ([0-9\.]+ .+), capacity ([0-9\.]+ .+), ([0-9]+) hits, ([0-9]+) requests, ([0-9\.Na]+) recent hit rate, ([0-9]+) save period in seconds`
		if parts := regexp.MustCompile(`^\s*Key Cache \s*: `+cacheDataRegexp+`$`).FindAllStringSubmatch(line, 2); parts != nil {
			info.KeyCache = cacheDataToCache(parts[0])
		} else if parts := regexp.MustCompile(`^\s*Row Cache \s*: `+cacheDataRegexp+`$`).FindAllStringSubmatch(line, 2); parts != nil {
			info.RowCache = cacheDataToCache(parts[0])
		} else if parts := regexp.MustCompile(`^\s*Counter Cache \s*: `+cacheDataRegexp+`$`).FindAllStringSubmatch(line, 2); parts != nil {
			info.CounterCache = cacheDataToCache(parts[0])
		}

	}

	return info
}

func (nt *Nodetool) GetInfo() Info {
	return nt.ParseInfo(nt.Execute("info"))
}

//NewNodetool constructs a new nodetool instance
func NewNodetool() Nodetool {
	return Nodetool{}
}
