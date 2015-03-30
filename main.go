package main

import (
	"fmt"
	"os"
	"time"

	ui "github.com/gizak/termui"
	tm "github.com/nsf/termbox-go"
)

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func boolToUnicode(val bool) string {
	if val == true {
		return "✔"
	}
	return "✘"
}

//Data provides acess to nodetool data in the correct format
type Data struct {
	nodetool      Nodetool
	readLatency   []float64
	writeLatency  []float64
	numExceptions []float64
	heapUsage     []float64
}

func (d *Data) GetPcntNodesUN() int {
	status := d.nodetool.GetStatus()
	return int(status.GetPcntUpNormal())
}

//GetLatencies returns a timeseries for read and write latency
func (d *Data) GetCfMetrics() (read []float64, write []float64) {
	cfstats := d.nodetool.GetCfStats()

	if len(d.readLatency) > 60 {
		d.readLatency = d.readLatency[1:]
	}
	d.readLatency = append(d.readLatency, cfstats.GetAvgReadLatency())

	if len(d.writeLatency) > 60 {
		d.writeLatency = d.writeLatency[1:]
	}
	d.writeLatency = append(d.writeLatency, cfstats.GetAvgWriteLatency())

	return d.readLatency, d.writeLatency
}

//GetInfoMetrics returns metrics from nodetool info
func (d *Data) GetInfoMetrics() (numExceptions []float64, heapUsage []float64) {
	info := d.nodetool.GetInfo()

	if len(d.numExceptions) > 60 {
		d.numExceptions = d.numExceptions[1:]
	}
	d.numExceptions = append(d.numExceptions, float64(info.Exceptions))

	if len(d.heapUsage) > 60 {
		d.heapUsage = d.heapUsage[1:]
	}
	d.heapUsage = append(d.heapUsage, float64(info.HeapUsage))

	return d.numExceptions, d.heapUsage
}

//GetNodeDescription shows identification info about the current node as well as some status details
func (d *Data) GetNodeDescription() string {
	info := d.nodetool.GetInfo()
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = "Unknown"
	}

	return fmt.Sprintf("%s::%s::%s | %s GOSSIP %s THRIFT %s NATIVE", info.DataCenter, info.Rack, hostname, boolToUnicode(info.GossipActive), boolToUnicode(info.ThriftActive), boolToUnicode(info.NativeTransportActive))
}

func main() {
	err := ui.Init()
	if err != nil {
		panic(err)
	}
	defer ui.Close()

	ui.UseTheme("helloworld")

	data := Data{nodetool: NewNodetool(), readLatency: []float64{0}, writeLatency: []float64{0}, numExceptions: []float64{0}, heapUsage: []float64{0}}

	title := ui.NewPar(data.GetNodeDescription())
	title.Height = 3

	numUpNodes := ui.NewGauge()
	numUpNodes.Percent = 0
	numUpNodes.Height = 3
	numUpNodes.Border.Label = "Num UN Nodes"
	numUpNodes.BarColor = ui.ColorGreen
	numUpNodes.BgColor = ui.ColorRed

	exceptions := ui.NewLineChart()
	exceptions.Data = []float64{0}
	exceptions.Height = 8
	exceptions.AxesColor = ui.ColorWhite
	exceptions.LineColor = ui.ColorRed

	heapUsage := ui.NewLineChart()
	heapUsage.Data = []float64{0}
	heapUsage.Height = 8
	heapUsage.AxesColor = ui.ColorWhite
	heapUsage.LineColor = ui.ColorGreen

	readLatency := ui.NewLineChart()
	readLatency.Data = []float64{0}
	readLatency.Height = 8
	readLatency.AxesColor = ui.ColorWhite
	readLatency.LineColor = ui.ColorGreen

	writeLatency := ui.NewLineChart()
	writeLatency.Data = []float64{0}
	writeLatency.Height = 8
	writeLatency.AxesColor = ui.ColorWhite
	writeLatency.LineColor = ui.ColorGreen

	// build layout
	ui.Body.AddRows(
		ui.NewRow(ui.NewCol(12, 0, title)),
		ui.NewRow(ui.NewCol(12, 0, numUpNodes)),
		ui.NewRow(ui.NewCol(6, 0, readLatency), ui.NewCol(6, 0, writeLatency)),
		ui.NewRow(ui.NewCol(6, 0, heapUsage), ui.NewCol(6, 0, exceptions)))

	//render function
	draw := func() {

		numUpNodes.Percent = data.GetPcntNodesUN()

		//update latencies
		readLatency.Data, writeLatency.Data = data.GetCfMetrics()
		readLatency.Border.Label = fmt.Sprintf("Read Latency (%.3f)", readLatency.Data[len(readLatency.Data)-1])
		writeLatency.Border.Label = fmt.Sprintf("Write Latency (%.3f)", writeLatency.Data[len(writeLatency.Data)-1])

		//update metrics from info cmd
		exceptions.Data, heapUsage.Data = data.GetInfoMetrics()
		exceptions.Border.Label = fmt.Sprintf("Exceptions (%v)", exceptions.Data[len(exceptions.Data)-1])
		heapUsage.Border.Label = fmt.Sprintf("Heap Used (%.3f)", heapUsage.Data[len(heapUsage.Data)-1])

		//do render
		ui.Body.Align()
		ui.Render(ui.Body)
	}

	//handle events (e.g. resize)
	evt := make(chan tm.Event)
	go func() {
		for {
			evt <- tm.PollEvent()
		}
	}()

	go func() {
		var lastRunTs int32
		for {
			if int32(time.Now().Unix()) >= lastRunTs+10 {
				draw()
				lastRunTs = int32(time.Now().Unix())
			}
			time.Sleep(time.Millisecond)
		}
	}()

	for {
		select {
		case e := <-evt:
			if e.Type == tm.EventKey && e.Ch == 'q' {
				return
			}
			if e.Type == tm.EventResize {
				ui.Body.Width = ui.TermWidth()
				ui.Body.Align()
				ui.Render(ui.Body)
			}
		}
	}
}
