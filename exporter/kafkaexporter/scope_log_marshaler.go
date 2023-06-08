// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kafkaexporter

import (
	"encoding/json"
	"fmt"

	"github.com/Shopify/sarama"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
)

type scopeLogMarshaler struct {
}

type serviceInfo struct {
	ServiceName string `json:"serviceName"`
	SdkLanguage string `json:"sdkLanguage"`
	SdkVersion  string `json:"sdkVersion"`
}

type messageModel struct {
	ScopeLogs   []*plog.ScopeLogs `json:"logs"`
	ServiceInfo *serviceInfo      `json:"serviceInfo"`
}

func newscopeLogMarshaler() scopeLogMarshaler {
	return scopeLogMarshaler{}
}

func (r scopeLogMarshaler) Marshal(logs plog.Logs, topic string) ([]*sarama.ProducerMessage, error) {
	var messageModels []*messageModel
	for i := 0; i < logs.ResourceLogs().Len(); i++ {
		rl := logs.ResourceLogs().At(i)
		// scopeLogs array
		var scopeLogs []*plog.ScopeLogs
		for j := 0; j < rl.ScopeLogs().Len(); j++ {
			sl := rl.ScopeLogs().At(j)
			scopeLogs = append(scopeLogs, &sl)
		}
		// resource attributes map to serviceInfo
		serviceInfo := resolve(rl.Resource().Attributes())

		messageModels = append(messageModels, &messageModel{
			ScopeLogs:   scopeLogs,
			ServiceInfo: serviceInfo,
		})
	}

	// to json array
	bytes, err := json.Marshal(messageModels)

	if err != nil {
		fmt.Printf("scope log json Marshal err: %v", err)
		return nil, err
	}

	messages := []*sarama.ProducerMessage{{
		Topic: topic,
		Value: sarama.ByteEncoder(bytes),
	}}

	return messages, nil
}

func resolve(kvMap pcommon.Map) *serviceInfo {
	var serviceInfo = new(serviceInfo)
	snVal, findSn := kvMap.Get("service.name")
	if findSn {
		serviceInfo.ServiceName = snVal.AsString()
	}
	sdkLang, findSdkLang := kvMap.Get("telemetry.sdk.language")
	if findSdkLang {
		serviceInfo.SdkLanguage = sdkLang.AsString()
	}

	sdkVersion, findSdkVersion := kvMap.Get("telemetry.sdk.version")
	if findSdkVersion {
		serviceInfo.SdkVersion = sdkVersion.AsString()
	}

	return serviceInfo
}

func (r scopeLogMarshaler) Encoding() string {
	return "scope_log_json"
}
