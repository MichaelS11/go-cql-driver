package cql

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/gocql/gocql"
)

// NewClusterConfig returns a new gocql ClusterConfig
func NewClusterConfig(hosts ...string) *gocql.ClusterConfig {
	clusterConfig := gocql.NewCluster(hosts...)
	if len(clusterConfig.Hosts) < 1 {
		clusterConfig.Hosts = []string{"127.0.0.1"}
	}
	return clusterConfig
}

// ClusterConfigToConfigString converts a gocql ClusterConfig to a config string
func ClusterConfigToConfigString(clusterConfig *gocql.ClusterConfig) string {
	clusterConfigDefault := gocql.NewCluster()
	stringConfig := ""

	if clusterConfig.Consistency != clusterConfigDefault.Consistency {
		stringConfig += "consistency=" + strconv.FormatUint(uint64(clusterConfig.Consistency), 10) + "&"
	}
	if clusterConfig.Timeout >= 0 {
		stringConfig += "timeout=" + clusterConfig.Timeout.String() + "&"
	}
	if clusterConfig.ConnectTimeout >= 0 {
		stringConfig += "connectTimeout=" + clusterConfig.ConnectTimeout.String() + "&"
	}
	if clusterConfig.NumConns > 1 {
		stringConfig += "numConns=" + strconv.FormatInt(int64(clusterConfig.NumConns), 10) + "&"
	}
	if clusterConfig.Authenticator != nil {
		passwordAuthenticator, ok := clusterConfig.Authenticator.(gocql.PasswordAuthenticator)
		if ok {
			if passwordAuthenticator.Username != "" {
				stringConfig += "username=" + passwordAuthenticator.Username + "&"
			}
			if passwordAuthenticator.Password != "" {
				stringConfig += "password=" + passwordAuthenticator.Password + "&"
			}
		}
	}

	if stringConfig == "" {
		stringConfig = strings.Join(clusterConfig.Hosts, ",")
	} else {
		stringConfig = strings.Join(clusterConfig.Hosts, ",") + "?" + stringConfig[:len(stringConfig)-1]
	}

	return stringConfig
}

// ConfigStringToClusterConfig converts a config string to a gocql ClusterConfig
func ConfigStringToClusterConfig(configString string) (*gocql.ClusterConfig, error) {
	clusterConfig := NewClusterConfig()
	configStringSplit := strings.SplitN(configString, "?", 2)

	if len(configStringSplit[0]) > 1 {
		hostsSplit := strings.Split(configStringSplit[0], ",")
		if len(hostsSplit) > 0 {
			clusterConfig.Hosts = make([]string, len(hostsSplit))
			for i := 0; i < len(hostsSplit); i++ {
				clusterConfig.Hosts[i] = strings.TrimSpace(hostsSplit[i])
			}
		}
	}

	passwordAuthenticator := gocql.PasswordAuthenticator{}

	if len(configStringSplit) > 1 && len(configStringSplit[1]) > 1 {
		dataSplit := strings.Split(configStringSplit[1], "&")
		if len(dataSplit) > 0 {
			for i := 0; i < len(dataSplit); i++ {
				settingSplit := strings.SplitN(dataSplit[i], "=", 2)
				if len(settingSplit) != 2 {
					return nil, fmt.Errorf("missing =")
				}
				key, value := strings.TrimSpace(settingSplit[0]), strings.TrimSpace(settingSplit[1])
				switch key {
				case "consistency":
					data, err := strconv.ParseUint(value, 10, 16)
					if err != nil {
						return nil, fmt.Errorf("failed for: %v = %v", key, value)
					}
					clusterConfig.Consistency = gocql.Consistency(data)
				case "timeout":
					data, err := time.ParseDuration(value)
					if err != nil {
						return nil, fmt.Errorf("failed for: %v = %v", key, value)
					}
					if data >= 0 {
						clusterConfig.Timeout = data
					}
				case "connectTimeout":
					data, err := time.ParseDuration(value)
					if err != nil {
						return nil, fmt.Errorf("failed for: %v = %v", key, value)
					}
					if data >= 0 {
						clusterConfig.ConnectTimeout = data
					}
				case "numConns":
					data, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, fmt.Errorf("failed for: %v = %v", key, value)
					}
					if data > 0 {
						clusterConfig.NumConns = int(data)
					}
				case "username":
					passwordAuthenticator.Username = value
					clusterConfig.Authenticator = passwordAuthenticator
				case "password":
					passwordAuthenticator.Password = value
					clusterConfig.Authenticator = passwordAuthenticator
				default:
					return nil, fmt.Errorf("invalid key: %v", key)
				}
			}
		}
	}

	return clusterConfig, nil
}
