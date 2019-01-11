// Copyright 2018 The OpenPitrix Authors. All rights reserved.
// Use of this source code is governed by a Apache license
// that can be found in the LICENSE file.

package runtime_provider

import (
	"context"
	"fmt"

	"openpitrix.io/openpitrix/pkg/models"
	"openpitrix.io/openpitrix/pkg/pb"
	"openpitrix.io/openpitrix/pkg/plugins/vmbased"
	"openpitrix.io/openpitrix/pkg/util/pbutil"
	"openpitrix.io/openpitrix/pkg/util/stringutil"
)

func (p *Server) ParseClusterConf(ctx context.Context, req *pb.ParseClusterConfRequest) (*pb.ParseClusterConfResponse, error) {
	versionId := req.GetVersionId().GetValue()
	runtimeId := req.GetRuntimeId().GetValue()
	conf := req.GetConf().GetValue()
	cluster := models.PbToClusterWrapper(req.GetCluster())
	cluster, err := vmbased.ParseClusterConf(ctx, versionId, runtimeId, conf, cluster)

	providerHandler := new(ProviderHandler)
	availabilityZone, err := providerHandler.DescribeAvailabilityZoneBySubnetId(ctx, runtimeId, cluster.Cluster.SubnetId)
	if err != nil {
		return nil, err
	}
	cluster.Cluster.Zone = availabilityZone
	return &pb.ParseClusterConfResponse{
		Cluster: models.ClusterWrapperToPb(cluster),
	}, err
}

func (p *Server) SplitJobIntoTasks(ctx context.Context, req *pb.SplitJobIntoTasksRequest) (*pb.SplitJobIntoTasksResponse, error) {
	taskLayer, err := vmbased.SplitJobIntoTasks(ctx, models.PbToJob(req.GetJob()))
	return &pb.SplitJobIntoTasksResponse{
		TaskLayer: models.TaskLayerToPb(taskLayer),
	}, err
}

func (p *Server) HandleSubtask(ctx context.Context, req *pb.HandleSubtaskRequest) (*pb.HandleSubtaskResponse, error) {
	providerHandler := new(ProviderHandler)
	task, err := vmbased.HandleSubtask(ctx, models.PbToTask(req.GetTask()), providerHandler)
	if err != nil {
		return nil, err
	} else {
		return &pb.HandleSubtaskResponse{
			Task: models.TaskToPb(task),
		}, nil
	}
}

func (p *Server) WaitSubtask(ctx context.Context, req *pb.WaitSubtaskRequest) (*pb.WaitSubtaskResponse, error) {
	providerHandler := new(ProviderHandler)
	task, err := vmbased.WaitSubtask(ctx, models.PbToTask(req.GetTask()), providerHandler)
	if err != nil {
		return nil, err
	} else {
		return &pb.WaitSubtaskResponse{
			Task: models.TaskToPb(task),
		}, nil
	}
}

func (p *Server) DescribeSubnets(ctx context.Context, req *pb.DescribeSubnetsRequest) (*pb.DescribeSubnetsResponse, error) {
	providerHandler := new(ProviderHandler)
	return providerHandler.DescribeSubnets(ctx, req)
}

func (p *Server) CheckResource(ctx context.Context, req *pb.CheckResourceRequest) (*pb.CheckResourceResponse, error) {
	providerHandler := new(ProviderHandler)
	err := providerHandler.CheckResourceQuotas(ctx, models.PbToClusterWrapper(req.GetCluster()))
	if err != nil {
		return &pb.CheckResourceResponse{
			Ok: pbutil.ToProtoBool(false),
		}, err
	} else {
		return &pb.CheckResourceResponse{
			Ok: pbutil.ToProtoBool(true),
		}, nil
	}
}

func (p *Server) DescribeVpc(ctx context.Context, req *pb.DescribeVpcRequest) (*pb.DescribeVpcResponse, error) {
	providerHandler := new(ProviderHandler)
	vpc, err := providerHandler.DescribeVpc(ctx, req.GetRuntimeId().GetValue(), req.GetVpcId().GetValue())
	if err != nil {
		return nil, err
	} else {
		return &pb.DescribeVpcResponse{
			Vpc: models.VpcToPb(vpc),
		}, nil
	}
}

func (p *Server) DescribeClusterDetails(ctx context.Context, req *pb.DescribeClusterDetailsRequest) (*pb.DescribeClusterDetailsResponse, error) {
	return &pb.DescribeClusterDetailsResponse{
		Cluster: new(pb.Cluster),
	}, nil
}

func (p *Server) ValidateRuntime(ctx context.Context, req *pb.ValidateRuntimeRequest) (*pb.ValidateRuntimeResponse, error) {
	providerHandler := new(ProviderHandler)
	runtimeUrl := req.RuntimeCredential.GetRuntimeUrl().GetValue()
	content := req.RuntimeCredential.GetRuntimeCredentialContent().GetValue()
	zone := req.GetZone().GetValue()
	zones, err := providerHandler.DescribeZones(ctx, runtimeUrl, content)
	if err != nil {
		return &pb.ValidateRuntimeResponse{
			Ok: pbutil.ToProtoBool(false),
		}, err
	}
	if zone == "" {
		return &pb.ValidateRuntimeResponse{
			Ok: pbutil.ToProtoBool(true),
		}, nil
	}
	if !stringutil.StringIn(zone, zones) {
		return &pb.ValidateRuntimeResponse{
			Ok: pbutil.ToProtoBool(false),
		}, fmt.Errorf("cannot access zone [%s]", zone)
	}
	return &pb.ValidateRuntimeResponse{
		Ok: pbutil.ToProtoBool(true),
	}, nil
}

func (p *Server) DescribeZones(ctx context.Context, req *pb.DescribeZonesRequest) (*pb.DescribeZonesResponse, error) {
	providerHandler := new(ProviderHandler)
	runtimeUrl := req.RuntimeCredential.GetRuntimeUrl().GetValue()
	content := req.RuntimeCredential.GetRuntimeCredentialContent().GetValue()
	zones, err := providerHandler.DescribeZones(ctx, runtimeUrl, content)
	return &pb.DescribeZonesResponse{
		Zones: zones,
	}, err
}
