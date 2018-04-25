package log_producer

import (
	"github.com/robfig/cron"
	"log"
	"time"
)

type ControlWorker struct {
	ScheduleFilterTimeoutPackageJob *cron.Cron
	PackageManager                  *PackageManager
}

func (c *ControlWorker) ScheduleFilterTimeoutPackageTask() {
	log.Println("scheduleFilterTimeoutPackageTask")
	p := c.PackageManager
	if p == nil {
		return
	}

	p.DataLocker.Lock()
	for key, data := range p.DataMap {
		if data == nil {
			continue
		}
		curr := time.Now().UnixNano() / (1000 * 1000)
		data.Lock.Lock()
		p.DataLocker.Unlock()
		if curr-data.ArriveTimeInMS >= p.Config.PackageTimeoutInMS {
			p.DataMap[key] = nil
			p.Worker.addPackage(data)
		}
		data.Lock.Unlock()
		p.DataLocker.Lock()
	}
	p.DataLocker.Unlock()
}

func (c *ControlWorker) Init() {
	spec := "*/1, *, *, *, *, *" // run 1 second

	filterTimeoutPackageJob := cron.New()
	filterTimeoutPackageJob.AddFunc(spec, c.ScheduleFilterTimeoutPackageTask)
	filterTimeoutPackageJob.Start()

	c.ScheduleFilterTimeoutPackageJob = filterTimeoutPackageJob
}

func (c *ControlWorker) Stop() {
	c.ScheduleFilterTimeoutPackageJob.Stop()
}
