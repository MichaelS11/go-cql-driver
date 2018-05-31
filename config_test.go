package cql

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/gocql/gocql"
)

type TestStringToConfigStruct struct {
	info          string
	configString  string
	clusterConfig *gocql.ClusterConfig
	err           error
}

func TestNewClusterConfig(t *testing.T) {
	clusterConfig := NewClusterConfig()
	if len(clusterConfig.Hosts) != 1 {
		t.Fatalf("len - received: %v - expected: %v ", len(clusterConfig.Hosts), 1)
	}
	if clusterConfig.Hosts[0] != "127.0.0.1" {
		t.Fatalf("Hosts - received: %v - expected: %v ", clusterConfig.Hosts[0], "127.0.0.1")
	}
}

func TestClusterConfigToConfigString(t *testing.T) {
	tests := []struct {
		info          string
		clusterConfig *gocql.ClusterConfig
		configString  string
	}{
		{info: "empty", clusterConfig: &gocql.ClusterConfig{}, configString: "?consistency=0&timeout=0s&connectTimeout=0s"},
		{info: "Consistency", clusterConfig: &gocql.ClusterConfig{Consistency: 1}, configString: "?consistency=1&timeout=0s&connectTimeout=0s"},
		{info: "Timeout < 0", clusterConfig: &gocql.ClusterConfig{Timeout: -1}, configString: "?consistency=0&connectTimeout=0s"},
		{info: "Timeout > 0", clusterConfig: &gocql.ClusterConfig{Timeout: 10 * time.Second}, configString: "?consistency=0&timeout=10s&connectTimeout=0s"},
		{info: "ConnectTimeout < 0", clusterConfig: &gocql.ClusterConfig{ConnectTimeout: -1}, configString: "?consistency=0&timeout=0s"},
		{info: "ConnectTimeout > 0", clusterConfig: &gocql.ClusterConfig{ConnectTimeout: 10 * time.Second}, configString: "?consistency=0&timeout=0s&connectTimeout=10s"},
		{info: "NumConns < 2", clusterConfig: &gocql.ClusterConfig{NumConns: 1}, configString: "?consistency=0&timeout=0s&connectTimeout=0s"},
		{info: "IgnorePeerAddr false DisableInitialHostLookup false", clusterConfig: &gocql.ClusterConfig{IgnorePeerAddr: false, DisableInitialHostLookup: false}, configString: "?consistency=0&timeout=0s&connectTimeout=0s"},
		{info: "IgnorePeerAddr true DisableInitialHostLookup false", clusterConfig: &gocql.ClusterConfig{IgnorePeerAddr: true, DisableInitialHostLookup: false}, configString: "?consistency=0&timeout=0s&connectTimeout=0s&ignorePeerAddr=true"},
		{info: "IgnorePeerAddr false DisableInitialHostLookup true", clusterConfig: &gocql.ClusterConfig{IgnorePeerAddr: false, DisableInitialHostLookup: true}, configString: "?consistency=0&timeout=0s&connectTimeout=0s&disableInitialHostLookup=true"},
		{info: "IgnorePeerAddr true DisableInitialHostLookup true", clusterConfig: &gocql.ClusterConfig{IgnorePeerAddr: true, DisableInitialHostLookup: true}, configString: "?consistency=0&timeout=0s&connectTimeout=0s&ignorePeerAddr=true&disableInitialHostLookup=true"},
		{info: "Host", clusterConfig: &gocql.ClusterConfig{Hosts: []string{"one"}}, configString: "one?consistency=0&timeout=0s&connectTimeout=0s"},
		{info: "Hosts", clusterConfig: &gocql.ClusterConfig{Hosts: []string{"one", "two", "three"}}, configString: "one,two,three?consistency=0&timeout=0s&connectTimeout=0s"},
	}
	for _, test := range tests {
		configString := ClusterConfigToConfigString(test.clusterConfig)
		if configString != test.configString {
			t.Errorf("configString - received: %#v - expected: %#v - info: %v", configString, test.configString, test.info)
		}
	}
}

func TestConfigStringToClusterConfig(t *testing.T) {
	tests := []TestStringToConfigStruct{
		{info: "missing =", configString: "?consistency", err: fmt.Errorf("missing =")},
		{info: "failed consistency", configString: "?consistency=", err: fmt.Errorf("failed for: consistency = ")},
		{info: "failed timeout", configString: "?timeout=", err: fmt.Errorf("failed for: timeout = ")},
		{info: "failed connectTimeout", configString: "?connectTimeout=", err: fmt.Errorf("failed for: connectTimeout = ")},
		{info: "failed numConns", configString: "?numConns=", err: fmt.Errorf("failed for: numConns = ")},
		{info: "failed ignorePeerAddr", configString: "?ignorePeerAddr=", err: fmt.Errorf("failed for: ignorePeerAddr = ")},
		{info: "failed disableInitialHostLookup", configString: "?disableInitialHostLookup=", err: fmt.Errorf("failed for: disableInitialHostLookup = ")},
		{info: "invalid key", configString: "?foo=bar", err: fmt.Errorf("invalid key: foo")},

		{info: "empty", configString: "", clusterConfig: NewClusterConfig()},
	}

	tests = append(tests, TestStringToConfigStruct{info: "empty", configString: "", clusterConfig: NewClusterConfig()})
	tests = append(tests, TestStringToConfigStruct{info: "Consistency 0", configString: "?consistency=0", clusterConfig: NewClusterConfig()})
	tests[len(tests)-1].clusterConfig.Consistency = 0
	tests = append(tests, TestStringToConfigStruct{info: "Consistency 1", configString: "?consistency=1", clusterConfig: NewClusterConfig()})
	tests[len(tests)-1].clusterConfig.Consistency = 1
	tests = append(tests, TestStringToConfigStruct{info: "Timeout < 0", configString: "?timeout=-1s", clusterConfig: NewClusterConfig()})
	tests = append(tests, TestStringToConfigStruct{info: "Timeout > 0", configString: "?timeout=1s", clusterConfig: NewClusterConfig()})
	tests[len(tests)-1].clusterConfig.Timeout = time.Second
	tests = append(tests, TestStringToConfigStruct{info: "ConnectTimeout < 0", configString: "?connectTimeout=-1s", clusterConfig: NewClusterConfig()})
	tests = append(tests, TestStringToConfigStruct{info: "ConnectTimeout > 0", configString: "?connectTimeout=1s", clusterConfig: NewClusterConfig()})
	tests[len(tests)-1].clusterConfig.ConnectTimeout = time.Second
	tests = append(tests, TestStringToConfigStruct{info: "NumConns < 1", configString: "?numConns=0", clusterConfig: NewClusterConfig()})
	tests = append(tests, TestStringToConfigStruct{info: "NumConns > 1", configString: "?numConns=2", clusterConfig: NewClusterConfig()})
	tests[len(tests)-1].clusterConfig.NumConns = 2
	tests = append(tests, TestStringToConfigStruct{info: "ignorePeerAddr true", configString: "?ignorePeerAddr=true", clusterConfig: NewClusterConfig()})
	tests[len(tests)-1].clusterConfig.IgnorePeerAddr = true
	tests = append(tests, TestStringToConfigStruct{info: "disableInitialHostLookup true", configString: "?disableInitialHostLookup=true", clusterConfig: NewClusterConfig()})
	tests[len(tests)-1].clusterConfig.DisableInitialHostLookup = true
	tests = append(tests, TestStringToConfigStruct{info: "Host", configString: "one", clusterConfig: NewClusterConfig()})
	tests[len(tests)-1].clusterConfig.Hosts = []string{"one"}
	tests = append(tests, TestStringToConfigStruct{info: "Hosts", configString: "one,two,three", clusterConfig: NewClusterConfig()})
	tests[len(tests)-1].clusterConfig.Hosts = []string{"one", "two", "three"}
	tests = append(tests, TestStringToConfigStruct{info: "Host & Consistency 0", configString: "one?consistency=0", clusterConfig: NewClusterConfig()})
	tests[len(tests)-1].clusterConfig.Consistency = 0
	tests[len(tests)-1].clusterConfig.Hosts = []string{"one"}
	tests = append(tests, TestStringToConfigStruct{info: "Hosts & Consistency 1", configString: "one,two,three?consistency=1", clusterConfig: NewClusterConfig()})
	tests[len(tests)-1].clusterConfig.Consistency = 1
	tests[len(tests)-1].clusterConfig.Hosts = []string{"one", "two", "three"}

	for _, test := range tests {
		clusterConfig, err := ConfigStringToClusterConfig(test.configString)
		if err == nil || test.err == nil {
			if err != test.err {
				t.Errorf("error - received: %v - expected: %v - info: %v", err, test.err, test.info)
				continue
			}
		} else if err.Error() != test.err.Error() {
			t.Errorf("error - received: %v - expected: %v - info: %v", err, test.err, test.info)
			continue
		}
		if !reflect.DeepEqual(clusterConfig, test.clusterConfig) {
			t.Errorf("clusterConfig - received: %#v - expected: %#v - info: %v", clusterConfig, test.clusterConfig, test.info)
		}
	}

}
