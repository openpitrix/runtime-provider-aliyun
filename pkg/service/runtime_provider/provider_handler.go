// Copyright 2018 The OpenPitrix Authors. All rights reserved.
// Use of this source code is governed by a Apache license
// that can be found in the LICENSE file.

package runtime_provider

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/aliyun/alibaba-cloud-sdk-go/sdk/requests"
	"github.com/aliyun/alibaba-cloud-sdk-go/services/ecs"

	runtimeclient "openpitrix.io/openpitrix/pkg/client/runtime"
	"openpitrix.io/openpitrix/pkg/constants"
	"openpitrix.io/openpitrix/pkg/logger"
	"openpitrix.io/openpitrix/pkg/models"
	"openpitrix.io/openpitrix/pkg/pb"
	"openpitrix.io/openpitrix/pkg/plugins/vmbased"
	"openpitrix.io/openpitrix/pkg/util/funcutil"
	"openpitrix.io/openpitrix/pkg/util/jsonutil"
	"openpitrix.io/openpitrix/pkg/util/pbutil"
	"openpitrix.io/openpitrix/pkg/util/retryutil"
)

var DevicePattern = regexp.MustCompile("/dev/xvd(.)")

type ProviderHandler struct {
	vmbased.FrameHandler
}

func (p *ProviderHandler) initInstanceService(ctx context.Context, runtimeId string) (*ecs.Client, error) {
	runtime, err := runtimeclient.NewRuntime(ctx, runtimeId)
	if err != nil {
		return nil, err
	}

	return p.initInstanceServiceWithCredential(ctx, runtime.RuntimeUrl, runtime.RuntimeCredentialContent, runtime.Zone)
}

func (p *ProviderHandler) initInstanceServiceWithCredential(ctx context.Context, runtimeUrl, runtimeCredential, zone string) (*ecs.Client, error) {
	credential := new(vmbased.Credential)
	err := jsonutil.Decode([]byte(runtimeCredential), credential)
	if err != nil {
		logger.Error(ctx, "Parse credential failed: %+v", err)
		return nil, err
	}

	ecsClient, err := ecs.NewClientWithAccessKey(zone, credential.AccessKeyId, credential.SecretAccessKey)
	if err != nil {
		return nil, err
	}

	return ecsClient, nil
}

func (p *ProviderHandler) getSubnet(ctx context.Context, service *ecs.Client, zone, subnetId string) (*ecs.VSwitch, error) {
	describeVSwitchsRequest := ecs.CreateDescribeVSwitchesRequest()
	describeVSwitchsRequest.ZoneId = zone
	describeVSwitchsRequest.VSwitchId = subnetId
	describeVSwitchsResponse, err := service.DescribeVSwitches(describeVSwitchsRequest)
	if err != nil {
		logger.Error(ctx, "DescribeVSwitches [%s] failed: %+v", subnetId, err)
		return nil, err
	}

	if len(describeVSwitchsResponse.VSwitches.VSwitch) == 0 {
		logger.Error(ctx, "DescribeVSwitches failed with 0 output subnets")
		return nil, fmt.Errorf("DescribeVSwitches failed with 0 output subnets")
	}
	return &describeVSwitchsResponse.VSwitches.VSwitch[0], nil
}

func (p *ProviderHandler) getSecurityGroup(ctx context.Context, service *ecs.Client, zone, vpcId string) (*ecs.SecurityGroup, error) {
	describeSecurityGroupsRequest := ecs.CreateDescribeSecurityGroupsRequest()
	describeSecurityGroupsRequest.VpcId = vpcId
	describeSecurityGroupsRequest.RegionId = ConvertZoneToRegion(zone)
	describeSecurityGroups, err := service.DescribeSecurityGroups(describeSecurityGroupsRequest)
	if err != nil {
		logger.Error(ctx, "DescribeSecurityGroups vpc [%s] failed: %+v", vpcId, err)
		return nil, err
	}
	if len(describeSecurityGroups.SecurityGroups.SecurityGroup) == 0 {
		logger.Error(ctx, "DescribeSecurityGroups failed with 0 output security group")
		return nil, fmt.Errorf("DescribeSecurityGroups failed with 0 output security group")
	}
	return &describeSecurityGroups.SecurityGroups.SecurityGroup[0], nil
}

func (p *ProviderHandler) RunInstances(ctx context.Context, task *models.Task) (*models.Task, error) {
	if task.Directive == "" {
		logger.Warn(ctx, "Skip task without directive")
		return task, nil
	}
	instance, err := models.NewInstance(task.Directive)
	if err != nil {
		return task, err
	}
	instanceService, err := p.initInstanceService(ctx, instance.RuntimeId)
	if err != nil {
		logger.Error(ctx, "Init api service failed: %+v", err)
		return task, err
	}

	instanceType, err := ConvertToInstanceType(instance.Cpu, instance.Memory)
	if err != nil {
		logger.Error(ctx, "Could not find an aliyun instance type: %+v", err)
		return task, err
	}

	logger.Info(ctx, "RunInstances with name [%s] instance type [%s]", instance.Name, instanceType)

	input := ecs.CreateRunInstancesRequest()
	input.InstanceName = instance.Name
	input.ImageId = instance.ImageId
	input.InstanceType = instanceType
	input.VSwitchId = instance.Subnet
	input.ZoneId = instance.Zone
	input.Password = DefaultLoginPassword
	input.InternetMaxBandwidthIn = requests.NewInteger(1)
	input.InternetChargeType = "PayByTraffic"
	input.InternetMaxBandwidthOut = requests.NewInteger(1)
	input.SystemDiskSize = "20"
	input.SystemDiskCategory, _ = ConvertToVolumeType(DefaultVolumeClass)

	if instance.NeedUserData == 1 {
		input.UserData = instance.UserDataValue
	}

	subnet, err := p.getSubnet(ctx, instanceService, instance.Zone, instance.Subnet)
	if err != nil {
		return nil, err
	}
	securityGroup, err := p.getSecurityGroup(ctx, instanceService, instance.Zone, subnet.VpcId)
	if err != nil {
		return nil, err
	}
	input.SecurityGroupId = securityGroup.SecurityGroupId

	logger.Debug(ctx, "RunInstances with input: %s", jsonutil.ToString(input))
	output, err := instanceService.RunInstances(input)
	if err != nil {
		logger.Error(ctx, "RunInstances failed: %+v", err)
		return task, err
	}
	logger.Debug(ctx, "RunInstances get output: %s", jsonutil.ToString(output))

	if len(output.InstanceIdSets.InstanceIdSet) == 0 {
		logger.Error(ctx, "RunInstances response with 0 output")
		return task, fmt.Errorf("failed to get instance id")
	}

	instance.InstanceId = output.InstanceIdSets.InstanceIdSet[0]

	// write back
	task.Directive = jsonutil.ToString(instance)

	return task, nil
}

func (p *ProviderHandler) StopInstances(ctx context.Context, task *models.Task) (*models.Task, error) {
	if task.Directive == "" {
		logger.Warn(ctx, "Skip task without directive")
		return task, nil
	}
	instance, err := models.NewInstance(task.Directive)
	if err != nil {
		return task, err
	}
	if instance.InstanceId == "" {
		logger.Warn(ctx, "Skip task without instance id")
		return task, nil
	}
	instanceService, err := p.initInstanceService(ctx, instance.RuntimeId)
	if err != nil {
		logger.Error(ctx, "Init api service failed: %+v", err)
		return task, err
	}

	describeInput := ecs.CreateDescribeInstancesRequest()
	describeInput.InstanceIds = fmt.Sprintf("[\"%s\"]", instance.InstanceId)
	describeOutput, err := instanceService.DescribeInstances(describeInput)
	if err != nil {
		logger.Error(ctx, "DescribeInstances failed: %+v", err)
		return task, err
	}

	if len(describeOutput.Instances.Instance) == 0 {
		return task, fmt.Errorf("instance [%s] not exist", instance.InstanceId)
	}

	status := describeOutput.Instances.Instance[0].Status

	if status == strings.Title(constants.StatusStopped) {
		logger.Warn(ctx, "Instance [%s] has already been [%s], do nothing", instance.InstanceId, status)
		return task, nil
	}

	logger.Info(ctx, "StopInstances [%s]", instance.Name)

	input := ecs.CreateStopInstanceRequest()
	input.InstanceId = instance.InstanceId

	_, err = instanceService.StopInstance(input)
	if err != nil {
		logger.Error(ctx, "StopInstances failed: %+v", err)
		return task, err
	}

	// write back
	task.Directive = jsonutil.ToString(instance)

	return task, nil
}

func (p *ProviderHandler) StartInstances(ctx context.Context, task *models.Task) (*models.Task, error) {
	if task.Directive == "" {
		logger.Warn(ctx, "Skip task without directive")
		return task, nil
	}
	instance, err := models.NewInstance(task.Directive)
	if err != nil {
		return task, err
	}
	if instance.InstanceId == "" {
		logger.Warn(ctx, "Skip task without instance id")
		return task, nil
	}
	instanceService, err := p.initInstanceService(ctx, instance.RuntimeId)
	if err != nil {
		logger.Error(ctx, "Init api service failed: %+v", err)
		return task, err
	}

	describeInput := ecs.CreateDescribeInstancesRequest()
	describeInput.InstanceIds = fmt.Sprintf("[\"%s\"]", instance.InstanceId)
	describeOutput, err := instanceService.DescribeInstances(describeInput)
	if err != nil {
		logger.Error(ctx, "DescribeInstances failed: %+v", err)
		return task, err
	}

	if len(describeOutput.Instances.Instance) == 0 {
		return task, fmt.Errorf("instance [%s] not exist", instance.InstanceId)
	}

	status := describeOutput.Instances.Instance[0].Status

	if status == strings.Title(constants.StatusRunning) {
		logger.Warn(ctx, "Instance [%s] has already been [%s], do nothing", instance.InstanceId, status)
		return task, nil
	}

	logger.Info(ctx, "StartInstances [%s]", instance.Name)

	input := ecs.CreateStartInstanceRequest()
	input.InstanceId = instance.InstanceId

	_, err = instanceService.StartInstance(input)
	if err != nil {
		logger.Error(ctx, "StartInstances failed: %+v", err)
		return task, err
	}

	// write back
	task.Directive = jsonutil.ToString(instance)

	return task, nil
}

func (p *ProviderHandler) DeleteInstances(ctx context.Context, task *models.Task) (*models.Task, error) {
	if task.Directive == "" {
		logger.Warn(ctx, "Skip task without directive")
		return task, nil
	}
	instance, err := models.NewInstance(task.Directive)
	if err != nil {
		return task, err
	}
	if instance.InstanceId == "" {
		logger.Warn(ctx, "Skip task without instance id")
		return task, nil
	}
	instanceService, err := p.initInstanceService(ctx, instance.RuntimeId)
	if err != nil {
		logger.Error(ctx, "Init api service failed: %+v", err)
		return task, err
	}

	describeInput := ecs.CreateDescribeInstancesRequest()
	describeInput.InstanceIds = fmt.Sprintf("[\"%s\"]", instance.InstanceId)
	describeOutput, err := instanceService.DescribeInstances(describeInput)
	if err != nil {
		logger.Error(ctx, "DescribeInstances failed: %+v", err)
		return task, err
	}

	if len(describeOutput.Instances.Instance) == 0 {
		return task, fmt.Errorf("instance [%s] not exist", instance.InstanceId)
	}

	status := describeOutput.Instances.Instance[0].Status
	if status == strings.Title(constants.StatusRunning) {
		logger.Info(ctx, "StopInstance [%s] before delete it", instance.Name)
		task, err := p.StopInstances(ctx, task)
		if err != nil {
			logger.Error(ctx, "StopInstances failed: %+v", err)
			return task, err
		}

		task, err = p.WaitStopInstances(ctx, task)
		if err != nil {
			return task, err
		}
	}

	logger.Info(ctx, "DeleteInstance [%s]", instance.Name)

	input := ecs.CreateDeleteInstanceRequest()
	input.InstanceId = instance.InstanceId

	_, err = instanceService.DeleteInstance(input)
	if err != nil {
		logger.Error(ctx, "DeleteInstance failed: %+v", err)
		return task, err
	}

	// write back
	task.Directive = jsonutil.ToString(instance)

	return task, nil
}

func (p *ProviderHandler) ResizeInstances(ctx context.Context, task *models.Task) (*models.Task, error) {
	if task.Directive == "" {
		logger.Warn(ctx, "Skip task without directive")
		return task, nil
	}
	instance, err := models.NewInstance(task.Directive)
	if err != nil {
		return task, err
	}
	if instance.InstanceId == "" {
		logger.Warn(ctx, "Skip task without instance id")
		return task, nil
	}
	instanceService, err := p.initInstanceService(ctx, instance.RuntimeId)
	if err != nil {
		logger.Error(ctx, "Init api service failed: %+v", err)
		return task, err
	}

	describeInput := ecs.CreateDescribeInstancesRequest()
	describeInput.InstanceIds = fmt.Sprintf("[\"%s\"]", instance.InstanceId)
	describeOutput, err := instanceService.DescribeInstances(describeInput)
	if err != nil {
		logger.Error(ctx, "DescribeInstances failed: %+v", err)
		return task, err
	}

	if len(describeOutput.Instances.Instance) == 0 {
		return task, fmt.Errorf("instance [%s] not exist", instance.InstanceId)
	}

	status := describeOutput.Instances.Instance[0].Status

	if status != strings.Title(constants.StatusStopped) {
		logger.Warn(ctx, "Instance [%s] is in status [%s], can not resize", instance.InstanceId, status)
		return task, fmt.Errorf("instance [%s] is in status [%s], can not resize", instance.InstanceId, status)
	}

	instanceType, err := ConvertToInstanceType(instance.Cpu, instance.Memory)
	if err != nil {
		logger.Error(ctx, "Could not find an aliyun instance type: %+v", err)
		return task, err
	}

	logger.Info(ctx, "ResizeInstances [%s] with instance type [%s]", instance.Name, instanceType)

	input := ecs.CreateModifyInstanceSpecRequest()
	input.InstanceId = instance.InstanceId
	input.InstanceType = instanceType

	_, err = instanceService.ModifyInstanceSpec(input)
	if err != nil {
		logger.Error(ctx, "ResizeInstances failed: %+v", err)
		return task, err
	}

	// write back
	task.Directive = jsonutil.ToString(instance)
	return task, nil
}

func (p *ProviderHandler) CreateVolumes(ctx context.Context, task *models.Task) (*models.Task, error) {
	if task.Directive == "" {
		logger.Warn(ctx, "Skip task without directive")
		return task, nil
	}
	volume, err := models.NewVolume(task.Directive)
	if err != nil {
		return task, err
	}
	instanceService, err := p.initInstanceService(ctx, volume.RuntimeId)
	if err != nil {
		logger.Error(ctx, "Init api service failed: %+v", err)
		return task, err
	}

	volumeType, err := ConvertToVolumeType(DefaultVolumeClass)
	if err != nil {
		return task, err
	}

	logger.Info(ctx, "CreateVolumes with name [%s] volume type [%s] size [%d]", volume.Name, volumeType, volume.Size)

	input := ecs.CreateCreateDiskRequest()
	input.DiskName = volume.Name
	input.ZoneId = volume.Zone
	input.Size = requests.NewInteger(volume.Size)
	input.DiskCategory = volumeType

	output, err := instanceService.CreateDisk(input)
	if err != nil {
		logger.Error(ctx, "CreateVolumes failed: %+v", err)
		return task, err
	}

	volume.VolumeId = output.DiskId

	// write back
	task.Directive = jsonutil.ToString(volume)

	return task, nil
}

func (p *ProviderHandler) DetachVolumes(ctx context.Context, task *models.Task) (*models.Task, error) {
	if task.Directive == "" {
		logger.Warn(ctx, "Skip task without directive")
		return task, nil
	}
	volume, err := models.NewVolume(task.Directive)
	if err != nil {
		return task, err
	}
	if volume.VolumeId == "" {
		logger.Warn(ctx, "Skip task without volume id")
		return task, nil
	}
	if volume.InstanceId == "" {
		logger.Warn(ctx, "Skip task without instance id")
		return task, nil
	}
	instanceService, err := p.initInstanceService(ctx, volume.RuntimeId)
	if err != nil {
		logger.Error(ctx, "Init api service failed: %+v", err)
		return task, err
	}

	describeInput := ecs.CreateDescribeDisksRequest()
	describeInput.DiskIds = fmt.Sprintf("[\"%s\"]", volume.VolumeId)
	describeOutput, err := instanceService.DescribeDisks(describeInput)
	if err != nil {
		logger.Error(ctx, "DescribeVolumes failed: %+v", err)
		return task, err
	}

	if len(describeOutput.Disks.Disk) == 0 {
		return task, fmt.Errorf("volume [%s] not exist", volume.VolumeId)
	}

	status := describeOutput.Disks.Disk[0].Status

	if status == strings.Title(constants.StatusAvailable) {
		logger.Warn(ctx, "Volume [%s] is in status [%s], no need to detach.", volume.VolumeId, status)
		return task, nil
	}

	logger.Info(ctx, "DetachVolume [%s] from instance [%s]", volume.Name, volume.InstanceId)

	input := ecs.CreateDetachDiskRequest()
	input.InstanceId = volume.InstanceId
	input.DiskId = volume.VolumeId

	_, err = instanceService.DetachDisk(input)
	if err != nil {
		logger.Error(ctx, "DetachVolumes failed: %+v", err)
		return task, err
	}

	// write back
	task.Directive = jsonutil.ToString(volume)

	return task, nil
}

func (p *ProviderHandler) AttachVolumes(ctx context.Context, task *models.Task) (*models.Task, error) {
	if task.Directive == "" {
		logger.Warn(ctx, "Skip task without directive")
		return task, nil
	}
	volume, err := models.NewVolume(task.Directive)
	if err != nil {
		return task, err
	}
	if volume.VolumeId == "" {
		logger.Warn(ctx, "Skip task without volume id")
		return task, nil
	}
	if volume.InstanceId == "" {
		logger.Warn(ctx, "Skip task without instance id")
		return task, nil
	}
	instanceService, err := p.initInstanceService(ctx, volume.RuntimeId)
	if err != nil {
		logger.Error(ctx, "Init api service failed: %+v", err)
		return task, err
	}

	logger.Info(ctx, "AttachVolume [%s] to instance [%s]", volume.VolumeId, volume.InstanceId)

	input := ecs.CreateAttachDiskRequest()
	input.InstanceId = volume.InstanceId
	input.DiskId = volume.VolumeId

	err = retryutil.Retry(5, 3*time.Second, func() error {
		// Already check instance status, but may failed here because of IncorrectInstanceStatus
		_, err = instanceService.AttachDisk(input)
		return err
	})
	if err != nil {
		logger.Error(ctx, "AttachVolumes failed: %+v", err)
		return task, err
	}

	// write back
	task.Directive = jsonutil.ToString(volume)

	return task, nil
}

func (p *ProviderHandler) DeleteVolumes(ctx context.Context, task *models.Task) (*models.Task, error) {
	if task.Directive == "" {
		logger.Warn(ctx, "Skip task without directive")
		return task, nil
	}
	volume, err := models.NewVolume(task.Directive)
	if err != nil {
		return task, err
	}
	if volume.VolumeId == "" {
		logger.Warn(ctx, "Skip task without volume id")
		return task, nil
	}
	instanceService, err := p.initInstanceService(ctx, volume.RuntimeId)
	if err != nil {
		logger.Error(ctx, "Init api service failed: %+v", err)
		return task, err
	}

	describeInput := ecs.CreateDescribeDisksRequest()
	describeInput.DiskIds = fmt.Sprintf("[\"%s\"]", volume.VolumeId)
	describeOutput, err := instanceService.DescribeDisks(describeInput)
	if err != nil {
		logger.Error(ctx, "DescribeVolumes failed: %+v", err)
		return task, err
	}

	if len(describeOutput.Disks.Disk) == 0 {
		return task, fmt.Errorf("volume [%s] not exist", volume.VolumeId)
	}

	disk := describeOutput.Disks.Disk[0]

	logger.Info(ctx, "DeleteVolume [%s] with status [%s]", volume.Name, disk.Status)
	if disk.Status == strings.Title(constants.StatusInUse2) {
		task, err := p.WaitVolumeState(ctx, task, strings.Title(constants.StatusAvailable))
		if err != nil {
			return task, err
		}
	}

	input := ecs.CreateDeleteDiskRequest()
	input.DiskId = volume.VolumeId

	_, err = instanceService.DeleteDisk(input)
	if err != nil {
		logger.Error(ctx, "DeleteVolumes failed: %+v", err)
		return task, err
	}

	// write back
	task.Directive = jsonutil.ToString(volume)

	return task, nil
}

func (p *ProviderHandler) ResizeVolumes(ctx context.Context, task *models.Task) (*models.Task, error) {
	if task.Directive == "" {
		logger.Warn(ctx, "Skip task without directive")
		return task, nil
	}
	volume, err := models.NewVolume(task.Directive)
	if err != nil {
		return task, err
	}
	if volume.VolumeId == "" {
		logger.Warn(ctx, "Skip task without volume")
		return task, nil
	}
	instanceService, err := p.initInstanceService(ctx, volume.RuntimeId)
	if err != nil {
		logger.Error(ctx, "Init api service failed: %+v", err)
		return task, err
	}

	describeInput := ecs.CreateDescribeDisksRequest()
	describeInput.DiskIds = fmt.Sprintf("[\"%s\"]", volume.VolumeId)
	describeOutput, err := instanceService.DescribeDisks(describeInput)
	if err != nil {
		logger.Error(ctx, "DescribeVolumes failed: %+v", err)
		return task, err
	}

	if len(describeOutput.Disks.Disk) == 0 {
		return task, fmt.Errorf("volume [%s] not exist", volume.VolumeId)
	}

	status := describeOutput.Disks.Disk[0].Status
	if status != strings.Title(constants.StatusAvailable) {
		logger.Warn(ctx, "Volume [%s] is in status [%s], can not resize.", volume.VolumeId, status)
		return task, fmt.Errorf("volume [%s] is in status [%s], can not resize", volume.VolumeId, status)
	}

	logger.Info(ctx, "ResizeVolumes [%s] with size [%d]", volume.Name, volume.Size)

	input := ecs.CreateResizeDiskRequest()
	input.DiskId = volume.VolumeId
	input.NewSize = requests.NewInteger(volume.Size)

	_, err = instanceService.ResizeDisk(input)
	if err != nil {
		logger.Error(ctx, "ResizeVolumes failed: %+v", err)
		return task, err
	}

	// write back
	task.Directive = jsonutil.ToString(volume)
	return task, nil
}

func (p *ProviderHandler) waitInstanceVolume(ctx context.Context, instanceService *ecs.Client, task *models.Task, instance *models.Instance) (*models.Task, error) {
	logger.Debug(ctx, "Waiting for volume [%s] attached to Instance [%s]", instance.VolumeId, instance.InstanceId)

	task, err := p.AttachVolumes(ctx, task)
	if err != nil {
		logger.Error(ctx, "Attach volume [%s] to Instance [%s] failed: %+v", instance.VolumeId, instance.InstanceId, err)
		return task, err
	}

	task, err = p.WaitAttachVolumes(ctx, task)
	if err != nil {
		logger.Error(ctx, "Waiting for volume [%s] attached to Instance [%s] failed: %+v", instance.VolumeId, instance.InstanceId, err)
		return task, err
	}

	describeInput := ecs.CreateDescribeDisksRequest()
	describeInput.DiskIds = fmt.Sprintf("[\"%s\"]", instance.VolumeId)
	describeOutput, err := instanceService.DescribeDisks(describeInput)
	if err != nil {
		logger.Error(ctx, "DescribeVolumes failed: %+v", err)
		return task, err
	}

	if len(describeOutput.Disks.Disk) == 0 {
		return task, fmt.Errorf("volume [%s] not exist", instance.VolumeId)
	}

	vol := describeOutput.Disks.Disk[0]
	instance.Device = vol.Device

	describeInput2 := ecs.CreateDescribeInstancesRequest()
	describeInput2.InstanceIds = fmt.Sprintf("[\"%s\"]", instance.InstanceId)
	describeOutput2, err := instanceService.DescribeInstances(describeInput2)
	if err != nil {
		logger.Error(ctx, "DescribeInstances failed: %+v", err)
		return task, err
	}

	if len(describeOutput2.Instances.Instance) == 0 {
		return task, fmt.Errorf("instance [%s] not exist", instance.InstanceId)
	}

	ins := describeOutput2.Instances.Instance[0]

	if ins.IoOptimized {
		instance.Device = DevicePattern.ReplaceAllString(instance.Device, "/dev/vd$1")
	}

	logger.Info(ctx, "Instance [%s] with io optimized [%t] attached volume [%s] as device [%s]", instance.InstanceId, ins.IoOptimized, instance.VolumeId, instance.Device)
	return task, nil
}

func (p *ProviderHandler) waitInstanceNetwork(ctx context.Context, instanceService *ecs.Client, instance *models.Instance, timeout time.Duration, waitInterval time.Duration) error {
	err := funcutil.WaitForSpecificOrError(func() (bool, error) {
		describeInput := ecs.CreateDescribeInstancesRequest()
		describeInput.InstanceIds = fmt.Sprintf("[\"%s\"]", instance.InstanceId)
		describeOutput, err := instanceService.DescribeInstances(describeInput)
		if err != nil {
			return false, err
		}

		if len(describeOutput.Instances.Instance) == 0 {
			return false, fmt.Errorf("instance [%s] not exist", instance.InstanceId)
		}

		ins := describeOutput.Instances.Instance[0]

		if len(ins.VpcAttributes.PrivateIpAddress.IpAddress) == 0 {
			return false, nil
		}

		instance.PrivateIp = ins.VpcAttributes.PrivateIpAddress.IpAddress[0]
		instance.Eip = ins.PublicIpAddress.IpAddress[0]
		return true, nil
	}, timeout, waitInterval)

	logger.Info(ctx, "Instance [%s] get private IP address [%s]", instance.InstanceId, instance.PrivateIp)

	if instance.Eip != "" {
		logger.Info(ctx, "Instance [%s] get EIP address [%s]", instance.InstanceId, instance.Eip)
	}

	return err
}

func (p *ProviderHandler) WaitRunInstances(ctx context.Context, task *models.Task) (*models.Task, error) {
	if task.Directive == "" {
		logger.Warn(ctx, "Skip task without directive")
		return task, nil
	}
	instance, err := models.NewInstance(task.Directive)
	if err != nil {
		return task, err
	}
	if instance.InstanceId == "" {
		logger.Warn(ctx, "Skip task without instance id")
		return task, nil
	}
	instanceService, err := p.initInstanceService(ctx, instance.RuntimeId)
	if err != nil {
		logger.Error(ctx, "Init api service failed: %+v", err)
		return task, err
	}

	task, err = p.WaitInstanceState(ctx, task, strings.Title(constants.StatusRunning))
	if err != nil {
		return task, err
	}

	if instance.VolumeId != "" {
		task, err := p.waitInstanceVolume(ctx, instanceService, task, instance)
		if err != nil {
			logger.Error(ctx, "Attach volume [%s] to Instance [%s] failed: %+v", instance.VolumeId, instance.InstanceId, err)
			return task, err
		}
	}

	err = p.waitInstanceNetwork(ctx, instanceService, instance, task.GetTimeout(constants.WaitTaskTimeout), constants.WaitTaskInterval)
	if err != nil {
		logger.Error(ctx, "Wait instance [%s] network failed: %+v", instance.InstanceId, err)
		return task, err
	}

	// write back
	task.Directive = jsonutil.ToString(instance)

	logger.Debug(ctx, "WaitRunInstances task [%s] directive: %s", task.TaskId, task.Directive)

	return task, nil
}

func (p *ProviderHandler) WaitInstanceState(ctx context.Context, task *models.Task, state string) (*models.Task, error) {
	if task.Directive == "" {
		logger.Warn(ctx, "Skip task without directive")
		return task, nil
	}
	instance, err := models.NewInstance(task.Directive)
	if err != nil {
		return task, err
	}
	if instance.InstanceId == "" {
		logger.Warn(ctx, "Skip task without instance id")
		return task, nil
	}
	instanceService, err := p.initInstanceService(ctx, instance.RuntimeId)
	if err != nil {
		logger.Error(ctx, "Init api service failed: %+v", err)
		return task, err
	}

	err = funcutil.WaitForSpecificOrError(func() (bool, error) {
		input := ecs.CreateDescribeInstancesRequest()
		input.InstanceIds = fmt.Sprintf("[\"%s\"]", instance.InstanceId)
		output, err := instanceService.DescribeInstances(input)
		if err != nil {
			return true, err
		}

		// may happen, so can not return err here
		if len(output.Instances.Instance) == 0 {
			logger.Error(ctx, "Instance [%s] not exist", instance.InstanceId)
			return false, nil
		}

		if output.Instances.Instance[0].Status == state {
			return true, nil
		}

		return false, nil
	}, task.GetTimeout(constants.WaitTaskTimeout), constants.WaitTaskInterval)
	if err != nil {
		logger.Error(ctx, "Wait instance [%s] status become to [%s] failed: %+v", instance.InstanceId, state, err)
		return task, err
	}

	logger.Info(ctx, "Wait instance [%s] status become to [%s] success", instance.InstanceId, state)

	return task, nil
}

func (p *ProviderHandler) WaitVolumeState(ctx context.Context, task *models.Task, state string) (*models.Task, error) {
	if task.Directive == "" {
		logger.Warn(ctx, "Skip task without directive")
		return task, nil
	}
	volume, err := models.NewVolume(task.Directive)
	if err != nil {
		return task, err
	}
	if volume.VolumeId == "" {
		logger.Warn(ctx, "Skip task without volume id")
		return task, nil
	}
	instanceService, err := p.initInstanceService(ctx, volume.RuntimeId)
	if err != nil {
		logger.Error(ctx, "Init api service failed: %+v", err)
		return task, err
	}

	err = funcutil.WaitForSpecificOrError(func() (bool, error) {
		input := ecs.CreateDescribeDisksRequest()
		input.DiskIds = fmt.Sprintf("[\"%s\"]", volume.VolumeId)

		output, err := instanceService.DescribeDisks(input)
		if err != nil {
			return true, err
		}

		if len(output.Disks.Disk) == 0 {
			return true, fmt.Errorf("volume [%s] not found", volume.VolumeId)
		}

		if output.Disks.Disk[0].Status == state {
			return true, nil
		}

		return false, nil
	}, task.GetTimeout(constants.WaitTaskTimeout), constants.WaitTaskInterval)
	if err != nil {
		logger.Error(ctx, "Wait volume [%s] status become to [%s] failed: %+v", volume.VolumeId, state, err)
		return task, err
	}

	logger.Info(ctx, "Wait volume [%s] status become to [%s] success", volume.VolumeId, state)

	return task, nil
}

func (p *ProviderHandler) WaitStopInstances(ctx context.Context, task *models.Task) (*models.Task, error) {
	return p.WaitInstanceState(ctx, task, strings.Title(constants.StatusStopped))
}

func (p *ProviderHandler) WaitStartInstances(ctx context.Context, task *models.Task) (*models.Task, error) {
	return p.WaitInstanceState(ctx, task, strings.Title(constants.StatusRunning))
}

func (p *ProviderHandler) WaitDeleteInstances(ctx context.Context, task *models.Task) (*models.Task, error) {
	if task.Directive == "" {
		logger.Warn(ctx, "Skip task without directive")
		return task, nil
	}
	instance, err := models.NewInstance(task.Directive)
	if err != nil {
		return task, err
	}
	if instance.InstanceId == "" {
		logger.Warn(ctx, "Skip task without instance id")
		return task, nil
	}
	instanceService, err := p.initInstanceService(ctx, instance.RuntimeId)
	if err != nil {
		logger.Error(ctx, "Init api service failed: %+v", err)
		return task, err
	}

	err = funcutil.WaitForSpecificOrError(func() (bool, error) {
		input := ecs.CreateDescribeInstancesRequest()
		input.InstanceIds = fmt.Sprintf("[\"%s\"]", instance.InstanceId)
		output, err := instanceService.DescribeInstances(input)
		if err != nil {
			return true, err
		}

		if len(output.Instances.Instance) == 0 {
			logger.Info(ctx, "Wait instance [%s] to be deleted successfully", instance.InstanceId)
			return true, nil
		}

		return false, nil
	}, task.GetTimeout(constants.WaitTaskTimeout), constants.WaitTaskInterval)
	return task, err
}

func (p *ProviderHandler) WaitResizeInstances(ctx context.Context, task *models.Task) (*models.Task, error) {
	return p.WaitInstanceState(ctx, task, strings.Title(constants.StatusStopped))
}

func (p *ProviderHandler) WaitCreateVolumes(ctx context.Context, task *models.Task) (*models.Task, error) {
	return p.WaitVolumeState(ctx, task, strings.Title(constants.StatusAvailable))
}

func (p *ProviderHandler) WaitAttachVolumes(ctx context.Context, task *models.Task) (*models.Task, error) {
	return p.WaitVolumeState(ctx, task, strings.Title(constants.StatusInUse2))
}

func (p *ProviderHandler) WaitDetachVolumes(ctx context.Context, task *models.Task) (*models.Task, error) {
	return p.WaitVolumeState(ctx, task, strings.Title(constants.StatusAvailable))
}

func (p *ProviderHandler) WaitDeleteVolumes(ctx context.Context, task *models.Task) (*models.Task, error) {
	if task.Directive == "" {
		logger.Warn(ctx, "Skip task without directive")
		return task, nil
	}
	volume, err := models.NewVolume(task.Directive)
	if err != nil {
		return task, err
	}
	if volume.VolumeId == "" {
		logger.Warn(ctx, "Skip task without volume")
		return task, nil
	}
	instanceService, err := p.initInstanceService(ctx, volume.RuntimeId)
	if err != nil {
		logger.Error(ctx, "Init api service failed: %+v", err)
		return task, err
	}

	err = funcutil.WaitForSpecificOrError(func() (bool, error) {
		input := ecs.CreateDescribeDisksRequest()
		input.DiskIds = fmt.Sprintf("[\"%s\"]", volume.VolumeId)
		output, err := instanceService.DescribeDisks(input)
		if err != nil {
			return true, err
		}

		if len(output.Disks.Disk) == 0 {
			logger.Info(ctx, "Wait volume [%s] to be deleted successfully", volume.VolumeId)
			return true, nil
		}

		return false, nil
	}, task.GetTimeout(constants.WaitTaskTimeout), constants.WaitTaskInterval)
	return task, err
}

func (p *ProviderHandler) WaitResizeVolumes(ctx context.Context, task *models.Task) (*models.Task, error) {
	return p.WaitVolumeState(ctx, task, strings.Title(constants.StatusAvailable))
}

func (p *ProviderHandler) DescribeSubnets(ctx context.Context, req *pb.DescribeSubnetsRequest) (*pb.DescribeSubnetsResponse, error) {
	instanceService, err := p.initInstanceService(ctx, req.GetRuntimeId().GetValue())
	if err != nil {
		logger.Error(ctx, "Init api service failed: %+v", err)
		return nil, err
	}

	input := ecs.CreateDescribeVSwitchesRequest()

	if len(req.GetZone()) == 1 {
		input.ZoneId = req.GetZone()[0]
	}

	if len(req.GetSubnetId()) > 0 {
		input.VSwitchId = strings.Join(req.GetSubnetId(), ",")
	}

	output, err := instanceService.DescribeVSwitches(input)
	if err != nil {
		logger.Error(ctx, "DescribeVSwitches failed: %+v", err)
		return nil, err
	}

	if len(output.VSwitches.VSwitch) == 0 {
		logger.Error(ctx, "DescribeVSwitches failed with 0 output subnets")
		return nil, fmt.Errorf("DescribeVSwitches failed with 0 output subnets")
	}

	response := new(pb.DescribeSubnetsResponse)

	for _, vs := range output.VSwitches.VSwitch {
		subnet := &pb.Subnet{
			SubnetId: pbutil.ToProtoString(vs.VSwitchId),
			Name:     pbutil.ToProtoString(vs.VSwitchName),
			VpcId:    pbutil.ToProtoString(vs.VpcId),
			Zone:     pbutil.ToProtoString(vs.ZoneId),
		}
		response.SubnetSet = append(response.SubnetSet, subnet)
	}

	response.TotalCount = uint32(len(response.SubnetSet))

	return response, nil
}

func (p *ProviderHandler) CheckResourceQuotas(ctx context.Context, clusterWrapper *models.ClusterWrapper) error {
	roleCount := make(map[string]int)
	for _, clusterNode := range clusterWrapper.ClusterNodesWithKeyPairs {
		role := clusterNode.Role
		_, isExist := roleCount[role]
		if isExist {
			roleCount[role] = roleCount[role] + 1
		} else {
			roleCount[role] = 1
		}
	}

	return nil
}

func (p *ProviderHandler) DescribeVpc(ctx context.Context, runtimeId, vpcId string) (*models.Vpc, error) {
	instanceService, err := p.initInstanceService(ctx, runtimeId)
	if err != nil {
		logger.Error(ctx, "Init api service failed: %+v", err)
		return nil, err
	}

	input := ecs.CreateDescribeVpcsRequest()
	input.VpcId = vpcId

	output, err := instanceService.DescribeVpcs(input)
	if err != nil {
		logger.Error(ctx, "DescribeVpcs to failed: %+v", err)
		return nil, err
	}

	if len(output.Vpcs.Vpc) == 0 {
		logger.Error(ctx, "DescribeVpcs failed with 0 output instances")
		return nil, fmt.Errorf("DescribeVpcs failed with 0 output instances")
	}

	vpc := output.Vpcs.Vpc[0]

	return &models.Vpc{
		VpcId:   vpc.VpcId,
		Name:    vpc.VpcName,
		Status:  vpc.Status,
		Subnets: vpc.VSwitchIds.VSwitchId,
	}, nil
}

func (p *ProviderHandler) DescribeZones(ctx context.Context, url, credential string) ([]string, error) {
	zone := DefaultZone
	instanceService, err := p.initInstanceServiceWithCredential(ctx, url, credential, zone)
	if err != nil {
		logger.Error(ctx, "Init api service failed: %+v", err)
		return nil, err
	}

	input := ecs.CreateDescribeRegionsRequest()
	output, err := instanceService.DescribeRegions(input)
	if err != nil {
		logger.Error(ctx, "DescribeRegions failed: %+v", err)
		return nil, err
	}

	var zones []string
	for _, zone := range output.Regions.Region {
		zones = append(zones, zone.RegionId)
	}
	return zones, nil
}

func (p *ProviderHandler) DescribeKeyPairs(ctx context.Context, url, credential, zone string) ([]string, error) {
	instanceService, err := p.initInstanceServiceWithCredential(ctx, url, credential, zone)
	if err != nil {
		logger.Error(ctx, "Init api service failed: %+v", err)
		return nil, err
	}

	input := ecs.CreateDescribeKeyPairsRequest()
	output, err := instanceService.DescribeKeyPairs(input)
	if err != nil {
		logger.Error(ctx, "DescribeKeyPairs failed: %+v", err)
		return nil, err
	}

	var keys []string
	for _, key := range output.KeyPairs.KeyPair {
		keys = append(keys, key.KeyPairName)
	}
	return keys, nil
}

func (p *ProviderHandler) DescribeImage(ctx context.Context, runtimeId, imageName string) (string, error) {
	instanceService, err := p.initInstanceService(ctx, runtimeId)
	if err != nil {
		logger.Error(ctx, "Init api service failed: %+v", err)
		return "", err
	}

	input := ecs.CreateDescribeImagesRequest()
	input.ImageName = imageName

	output, err := instanceService.DescribeImages(input)
	if err != nil {
		logger.Error(ctx, "DescribeImages failed: %+v", err)
		return "", err
	}

	if len(output.Images.Image) == 0 {
		return "", fmt.Errorf("image with name [%s] not exist", imageName)
	}

	imageId := output.Images.Image[0].ImageId

	return imageId, nil
}

func (p *ProviderHandler) DescribeAvailabilityZoneBySubnetId(ctx context.Context, runtimeId, subnetId string) (string, error) {
	instanceService, err := p.initInstanceService(ctx, runtimeId)
	if err != nil {
		logger.Error(ctx, "Init api service failed: %+v", err)
		return "", err
	}

	input := ecs.CreateDescribeVSwitchesRequest()
	input.VSwitchId = subnetId
	output, err := instanceService.DescribeVSwitches(input)
	if err != nil {
		logger.Error(ctx, "DescribeVSwitches failed: %+v", err)
		return "", err
	}

	if len(output.VSwitches.VSwitch) == 0 {
		return "", fmt.Errorf("subnet [%s] not exist", subnetId)
	}

	zone := output.VSwitches.VSwitch[0].ZoneId

	return zone, nil
}
