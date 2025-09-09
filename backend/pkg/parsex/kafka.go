/*
 * Copyright 2025 coze-dev Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package parsex

import (
	"github.com/IBM/sarama"
	"github.com/coze-dev/coze-studio/backend/pkg/logs"
	"os"
)

func KafkaVersion(key string) sarama.KafkaVersion {
	mqVersion := os.Getenv(key)
	version, err := sarama.ParseKafkaVersion(mqVersion)
	if err != nil {
		logs.Warnf("Invalid kafak version value: %s, current using default: %s", mqVersion, version)
	}
	return version
}

func KafkaAuth(saslType string, user, password string, scramGen func() sarama.SCRAMClient, tokenProvider sarama.AccessTokenProvider, config *sarama.Config) error {
	switch saslType {
	case sarama.SASLTypePlaintext:
		config.Net.SASL.User = user
		config.Net.SASL.Password = password
	case sarama.SASLTypeOAuth:
		config.Net.SASL.TokenProvider = tokenProvider
	case sarama.SASLTypeSCRAMSHA256, sarama.SASLTypeSCRAMSHA512:
		config.Net.SASL.User = user
		config.Net.SASL.Password = password
		config.Net.SASL.SCRAMClientGeneratorFunc = scramGen
	case sarama.SASLTypeGSSAPI:
		config.Net.SASL.GSSAPI.ServiceName = user
		switch config.Net.SASL.GSSAPI.AuthType {
		case sarama.KRB5_USER_AUTH:
			config.Net.SASL.GSSAPI.Password = password
		case sarama.KRB5_KEYTAB_AUTH:
			config.Net.SASL.GSSAPI.KeyTabPath = password
		case sarama.KRB5_CCACHE_AUTH:
			config.Net.SASL.GSSAPI.CCachePath = password

		default:
			return nil
		}
	}
	return nil
}
