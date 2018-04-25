package log_producer

import (
	"fmt"
	aliyun_log "github.com/aliyun/aliyun-log-go-sdk"
	"log"
)

type ProjectPool struct {
	projectMap  map[string]*aliyun_log.LogProject
	logStoreMap map[string]*aliyun_log.LogStore
}

func (p *ProjectPool) UpdateProject(config *aliyun_log.LogProject) {
	if p.projectMap == nil {
		p.projectMap = make(map[string]*aliyun_log.LogProject)
	}

	if _, ok := p.projectMap[config.Name]; !ok {
		p.projectMap[config.Name] = config
	}
}

func (p *ProjectPool) GetProject(name string) *aliyun_log.LogProject {
	if p.projectMap == nil {
		return nil
	}

	return p.projectMap[name]
}

func (p *ProjectPool) getLogstore(projectName string, logstoreName string) *aliyun_log.LogStore {
	if p.logStoreMap == nil {
		p.logStoreMap = make(map[string]*aliyun_log.LogStore)
	}

	key := fmt.Sprintf("project|%s|logstore|%s", projectName, logstoreName)

	if _, ok := p.logStoreMap[key]; !ok {
		log_project := p.GetProject(projectName)
		if log_project == nil {
			log.Printf("can't get project %s\n", projectName)
			panic(aliyun_log.NewClientError(aliyun_log.PROJECT_NOT_EXIST))
			return nil
		}

		log_logstore, err2 := log_project.GetLogStore(logstoreName)
		if err2 != nil {
			log.Printf("can't get logstore %s, %v\n", projectName, err2)
			panic(aliyun_log.NewClientError(aliyun_log.PROJECT_NOT_EXIST))
			return nil
		}

		p.logStoreMap[key] = log_logstore
	}

	return p.logStoreMap[key]
}
