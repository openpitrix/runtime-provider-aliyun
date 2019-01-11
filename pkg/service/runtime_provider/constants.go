// Copyright 2018 The OpenPitrix Authors. All rights reserved.
// Use of this source code is governed by a Apache license
// that can be found in the LICENSE file.

package runtime_provider

const (
	Provider       = "aliyun"
	ProviderConfig = `
api_server: ecs.aliyuncs.com
zone: .*
image_id: ubuntu_16_0402_64_20G_alibase_20180409.vhd
image_url: https://openpitrix.pek3a.qingstor.com/image/ubuntu.tar.gz
provider_type: vmbased
frontgate_conf: '{"app_id":"app-ABCDEFGHIJKLMNOPQRST","version_id":"appv-ABCDEFGHIJKLMNOPQRST","name":"frontgate","description":"OpenPitrixbuilt-infrontgateservice","subnet":"","nodes":[{"container":{"type":"docker","image":"openpitrix/openpitrix:metadata"},"count":1,"cpu":1,"memory":1024,"volume":{"size":20,"mount_point":"/data","filesystem":"ext4"}}]}'
`
)

const (
	DefaultVolumeClass   = 2
	DefaultZone          = "cn-shanghai"
	DefaultLoginPassword = "p12cHANgepwD"
)
