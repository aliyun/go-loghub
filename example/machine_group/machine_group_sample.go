package main

import (
	"fmt"
	"os"

	sls "github.com/aliyun/aliyun-log-go-sdk"
	"github.com/aliyun/aliyun-log-go-sdk/example/util"
)

func main() {
	// machine group example
	project := util.ProjectName
	logstore := "test-logstore"
	testConf := "test-conf"
	testMachineGroup := "test-mg"
	testService := "demo-service"
	exist, err := checkMachineGroupExist(testMachineGroup)
	if err != nil {
		fmt.Println("check machine group fail:", err)
		os.Exit(1)
	}
	if exist {
		util.Client.DeleteMachineGroup(project, testMachineGroup)
	}

	err = createMachineGroup(testMachineGroup)
	if err != nil {
		fmt.Println("create machine group:" + testMachineGroup + " fail")
		fmt.Println(err)
		os.Exit(1)
	}

	err = getMachineGroup(testMachineGroup)
	if err != nil {
		fmt.Println("get machine group:" + testMachineGroup + " fail")
		fmt.Println(err)
		os.Exit(1)
	}

	exist, err = checkMachineGroupExist(testMachineGroup)
	if err != nil {
		fmt.Println("check machine group exist fail:")
		fmt.Println(err)
		os.Exit(1)
	}
	if !exist {
		fmt.Println("machine group:" + testMachineGroup + " should be exist")
		os.Exit(1)
	}

	exist, err = util.Client.CheckConfigExist(project, testConf)
	if err != nil {
		fmt.Println("check config exist fail:", err)
		os.Exit(1)
	}
	if exist {
		util.Client.DeleteConfig(project, testConf)
	}

	err = createLogConfig(testConf, project, logstore, testService)
	if err != nil {
		fmt.Println("create config fail:")
		fmt.Println(err)
		os.Exit(1)
	}

	err = applyConfToMachineGroup(testConf, testMachineGroup)
	if err != nil {
		fmt.Println("apply config to machine group fail:")
		fmt.Println(err)
		os.Exit(1)
	}

	err = deleteConfig(testConf)
	if err != nil {
		fmt.Println("delete config fail:")
		fmt.Println(err)
		os.Exit(1)
	}

	err = deleteMachineGroup(testMachineGroup)
	if err != nil {
		fmt.Println("delte machine group fail:")
		fmt.Println(err)
		os.Exit(1)
	}

	exist, err = checkMachineGroupExist(testMachineGroup)
	if err != nil {
		fmt.Println("check machine group exist fail:")
		fmt.Println(err)
		os.Exit(1)
	}
	if exist {
		fmt.Println("machine group:" + testMachineGroup + " should not be exist")
	}
	fmt.Println("machine group sample end")
}

func applyConfToMachineGroup(confName string, mgname string) (err error) {
	err = util.Client.ApplyConfigToMachineGroup(util.ProjectName, confName, mgname)
	if err != nil {
		return err
	}
	return nil
}

func createLogConfig(configName string, project, logstore string, serviceName string) (err error) {
	// 日志所在的父目录
	logPath := "/var/log/lambda/" + serviceName
	// 日志文件的pattern，如functionName.LOG
	filePattern := "*.LOG"
	// 日志时间格式
	timeFormat := "%Y/%m/%d %H:%M:%S"
	// 日志提取后所生成的Key
	key := make([]string, 1)
	// 用于过滤日志所用到的key，只有key的值满足对应filterRegex列中设定的正则表达式日志才是符合要求的
	filterKey := make([]string, 1)
	// 和每个filterKey对应的正正则表达式， filterRegex的长度和filterKey的长度必须相同
	filterRegex := make([]string, 1)
	// topicFormat
	// 1. 用于将日志文件路径的某部分作为topic
	// 2. none 表示topic为空
	// 3. default 表示将日志文件路径作为topic
	// 4. group_topic 表示将应用该配置的机器组topic属性作为topic
	// 以serviceName为topic的正则：/var/log/lambda/([^/]*)/.*
	// 日志路径: /var/log/lambda/my-service/fjaishgaidhfiajf2343/func1.LOG
	topicFormat := "/var/log/lambda/([^/]*)/.*" // topicFormat is right
	inputDetail := sls.InputDetail{
		LogType:       "common_reg_log",
		LogPath:       logPath,
		FilePattern:   filePattern,
		LocalStorage:  true,
		TimeFormat:    timeFormat,
		LogBeginRegex: "", // 日志首行特征
		Regex:         "", // 日志对提取正则表达式
		Keys:          key,
		FilterKeys:    filterKey,
		FilterRegex:   filterRegex,
		TopicFormat:   topicFormat,
	}
	outputDetail := sls.OutputDetail{
		ProjectName:  project,
		LogStoreName: logstore,
	}
	config := &sls.LogConfig{
		Name:         configName,
		InputType:    "file",       //现在只支持file
		OutputType:   "LogService", //现在只支持LogService
		InputDetail:  inputDetail,
		OutputDetail: outputDetail,
	}
	err = util.Client.CreateConfig(project, config)
	if err != nil {
		return err
	}
	return nil
}

func checkMachineGroupExist(groupName string) (exist bool, err error) {
	exist, err = util.Client.CheckMachineGroupExist(util.ProjectName, groupName)
	if err != nil {
		return false, err
	}
	return exist, nil
}
func getMachineGroup(groupName string) (err error) {
	_, err = util.Client.GetMachineGroup(util.ProjectName, groupName)
	if err != nil {
		return err
	}
	return nil
}

func deleteMachineGroup(groupName string) (err error) {
	err = util.Client.DeleteMachineGroup(util.ProjectName, groupName)
	if err != nil {
		return err
	}
	return nil
}

func createMachineGroup(groupName string) (err error) {
	attribute := sls.MachinGroupAttribute{
		ExternalName: "",
		TopicName:    "",
	}
	machineList := []string{"mac-user-defined-id-value"}
	var machineGroup = &sls.MachineGroup{
		Name:          groupName,
		MachineIDType: "userdefined",
		MachineIDList: machineList,
		Attribute:     attribute,
	}
	err = util.Client.CreateMachineGroup(util.ProjectName, machineGroup)
	if err != nil {
		return err
	}
	return nil
}

func deleteConfig(confName string) (err error) {
	err = util.Client.DeleteConfig(util.ProjectName, confName)
	if err != nil {
		return err
	}
	return nil
}
