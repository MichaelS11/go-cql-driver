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
		{info: "empty", clusterConfig: &gocql.ClusterConfig{}, configString: "?consistency=any&timeout=0s&connectTimeout=0s&writeCoalesceWaitTime=0s"},
		{info: "Consistency", clusterConfig: &gocql.ClusterConfig{Consistency: 1}, configString: "?consistency=one&timeout=0s&connectTimeout=0s&writeCoalesceWaitTime=0s"},
		{info: "Timeout < 0", clusterConfig: &gocql.ClusterConfig{Timeout: -1}, configString: "?consistency=any&connectTimeout=0s&writeCoalesceWaitTime=0s"},
		{info: "Timeout > 0", clusterConfig: &gocql.ClusterConfig{Timeout: 10 * time.Second}, configString: "?consistency=any&timeout=10s&connectTimeout=0s&writeCoalesceWaitTime=0s"},
		{info: "ConnectTimeout < 0", clusterConfig: &gocql.ClusterConfig{ConnectTimeout: -1}, configString: "?consistency=any&timeout=0s&writeCoalesceWaitTime=0s"},
		{info: "ConnectTimeout > 0", clusterConfig: &gocql.ClusterConfig{ConnectTimeout: 10 * time.Second}, configString: "?consistency=any&timeout=0s&connectTimeout=10s&writeCoalesceWaitTime=0s"},
		{info: "Keyspace", clusterConfig: &gocql.ClusterConfig{Keyspace: "system"}, configString: "?consistency=any&timeout=0s&connectTimeout=0s&keyspace=system&writeCoalesceWaitTime=0s"},
		{info: "NumConns < 2", clusterConfig: &gocql.ClusterConfig{NumConns: 1}, configString: "?consistency=any&timeout=0s&connectTimeout=0s&writeCoalesceWaitTime=0s"},
		{info: "NumConns > 1", clusterConfig: &gocql.ClusterConfig{NumConns: 2}, configString: "?consistency=any&timeout=0s&connectTimeout=0s&numConns=2&writeCoalesceWaitTime=0s"},
		{info: "IgnorePeerAddr false DisableInitialHostLookup false", clusterConfig: &gocql.ClusterConfig{IgnorePeerAddr: false, DisableInitialHostLookup: false}, configString: "?consistency=any&timeout=0s&connectTimeout=0s&writeCoalesceWaitTime=0s"},
		{info: "IgnorePeerAddr true DisableInitialHostLookup false", clusterConfig: &gocql.ClusterConfig{IgnorePeerAddr: true, DisableInitialHostLookup: false}, configString: "?consistency=any&timeout=0s&connectTimeout=0s&ignorePeerAddr=true&writeCoalesceWaitTime=0s"},
		{info: "IgnorePeerAddr false DisableInitialHostLookup true", clusterConfig: &gocql.ClusterConfig{IgnorePeerAddr: false, DisableInitialHostLookup: true}, configString: "?consistency=any&timeout=0s&connectTimeout=0s&disableInitialHostLookup=true&writeCoalesceWaitTime=0s"},
		{info: "IgnorePeerAddr true DisableInitialHostLookup true", clusterConfig: &gocql.ClusterConfig{IgnorePeerAddr: true, DisableInitialHostLookup: true}, configString: "?consistency=any&timeout=0s&connectTimeout=0s&ignorePeerAddr=true&disableInitialHostLookup=true&writeCoalesceWaitTime=0s"},
		{info: "WriteCoalesceWaitTime 1s", clusterConfig: &gocql.ClusterConfig{WriteCoalesceWaitTime: time.Second}, configString: "?consistency=any&timeout=0s&connectTimeout=0s&writeCoalesceWaitTime=1s"},
		{info: "Authenticator empty", clusterConfig: &gocql.ClusterConfig{Authenticator: gocql.PasswordAuthenticator{}}, configString: "?consistency=any&timeout=0s&connectTimeout=0s&writeCoalesceWaitTime=0s"},
		{info: "Authenticator username", clusterConfig: &gocql.ClusterConfig{Authenticator: gocql.PasswordAuthenticator{Username: "alice@bob.com"}}, configString: "?consistency=any&timeout=0s&connectTimeout=0s&writeCoalesceWaitTime=0s&username=alice%40bob.com"},
		{info: "Authenticator username password", clusterConfig: &gocql.ClusterConfig{Authenticator: gocql.PasswordAuthenticator{Username: "alice@bob.com", Password: "top$ecret"}}, configString: "?consistency=any&timeout=0s&connectTimeout=0s&writeCoalesceWaitTime=0s&username=alice%40bob.com&password=top%24ecret"},
		{info: "Host", clusterConfig: &gocql.ClusterConfig{Hosts: []string{"one"}}, configString: "one?consistency=any&timeout=0s&connectTimeout=0s&writeCoalesceWaitTime=0s"},
		{info: "Hosts", clusterConfig: &gocql.ClusterConfig{Hosts: []string{"one", "two", "three"}}, configString: "one,two,three?consistency=any&timeout=0s&connectTimeout=0s&writeCoalesceWaitTime=0s"},
		{info: "SslOptions empty", clusterConfig: cfgWithSsl(&gocql.SslOptions{}), configString: "127.0.0.1?timeout=600ms&connectTimeout=600ms&numConns=2"},
		{info: "SslOptions caPath", clusterConfig: cfgWithSsl(&gocql.SslOptions{CaPath: "/some path.pem"}), configString: "127.0.0.1?timeout=600ms&connectTimeout=600ms&numConns=2&caPath=%2Fsome+path.pem"},
		{info: "SslOptions keyPath", clusterConfig: cfgWithSsl(&gocql.SslOptions{KeyPath: "/some+path.pem"}), configString: "127.0.0.1?timeout=600ms&connectTimeout=600ms&numConns=2&keyPath=%2Fsome%2Bpath.pem"},
		{info: "SslOptions certPath", clusterConfig: cfgWithSsl(&gocql.SslOptions{CertPath: "/some path.pem"}), configString: "127.0.0.1?timeout=600ms&connectTimeout=600ms&numConns=2&certPath=%2Fsome+path.pem"},
		{info: "SslOptions enableHostVerification", clusterConfig: cfgWithSsl(&gocql.SslOptions{EnableHostVerification: true}), configString: "127.0.0.1?timeout=600ms&connectTimeout=600ms&numConns=2&enableHostVerification=true"},
		{info: "SslOptions caPath keyPath certPath enableHostVerification", clusterConfig: cfgWithSsl(&gocql.SslOptions{CaPath: "/some path.pem", KeyPath: "/some+path.pem", CertPath: "/some path.pem", EnableHostVerification: true}), configString: "127.0.0.1?timeout=600ms&connectTimeout=600ms&numConns=2&enableHostVerification=true&keyPath=%2Fsome%2Bpath.pem&certPath=%2Fsome+path.pem&caPath=%2Fsome+path.pem"},
	}
	for _, test := range tests {
		configString := ClusterConfigToConfigString(test.clusterConfig)
		if configString != test.configString {
			t.Errorf("configString - received: %#v - expected: %#v - info: %v", configString, test.configString, test.info)
		}
	}
}

func cfgWith(customize func(*gocql.ClusterConfig)) *gocql.ClusterConfig {
	cfg := NewClusterConfig()
	customize(cfg)
	return cfg
}

func cfgWithAuth(auth gocql.PasswordAuthenticator) *gocql.ClusterConfig {
	cfg := NewClusterConfig()
	cfg.Authenticator = auth
	return cfg
}

func cfgWithSsl(sslCfg *gocql.SslOptions) *gocql.ClusterConfig {
	cfg := NewClusterConfig()
	cfg.SslOpts = sslCfg
	return cfg
}

func TestConfigStringToClusterConfig(t *testing.T) {
	tests := []TestStringToConfigStruct{
		// Missing `=`
		{info: "missing '=' consistency", configString: "?consistency", err: fmt.Errorf("missing =")},
		{info: "missing '=' keyspace", configString: "?keyspace", err: fmt.Errorf("missing =")},
		{info: "missing '=' timeout", configString: "?timeout", err: fmt.Errorf("missing =")},
		{info: "missing '=' connectTimeout", configString: "?connectTimeout", err: fmt.Errorf("missing =")},
		{info: "missing '=' numConns", configString: "?numConns", err: fmt.Errorf("missing =")},
		{info: "missing '=' ignorePeerAddr", configString: "?ignorePeerAddr", err: fmt.Errorf("missing =")},
		{info: "missing '=' disableInitialHostLookup", configString: "?disableInitialHostLookup", err: fmt.Errorf("missing =")},
		{info: "missing '=' writeCoalesceWaitTime", configString: "?writeCoalesceWaitTime", err: fmt.Errorf("missing =")},
		{info: "missing '=' username", configString: "?username", err: fmt.Errorf("missing =")},
		{info: "missing '=' password", configString: "?password", err: fmt.Errorf("missing =")},
		{info: "missing '=' enableHostVerification", configString: "?enableHostVerification", err: fmt.Errorf("missing =")},
		{info: "missing '=' caPath", configString: "?caPath", err: fmt.Errorf("missing =")},
		{info: "missing '=' certPath", configString: "?certPath", err: fmt.Errorf("missing =")},
		{info: "missing '=' keyPath", configString: "?keyPath", err: fmt.Errorf("missing =")},

		// Missing value
		{info: "empty consistency", configString: "?consistency=", err: fmt.Errorf("failed for: consistency = ")},
		{info: "empty keyspace", configString: "?keyspace=", err: fmt.Errorf("failed for: keyspace = ")},
		{info: "empty timeout", configString: "?timeout=", err: fmt.Errorf("failed for: timeout = ")},
		{info: "empty connectTimeout", configString: "?connectTimeout=", err: fmt.Errorf("failed for: connectTimeout = ")},
		{info: "empty numConns", configString: "?numConns=", err: fmt.Errorf("failed for: numConns = ")},
		{info: "empty ignorePeerAddr", configString: "?ignorePeerAddr=", err: fmt.Errorf("failed for: ignorePeerAddr = ")},
		{info: "empty disableInitialHostLookup", configString: "?disableInitialHostLookup=", err: fmt.Errorf("failed for: disableInitialHostLookup = ")},
		{info: "empty writeCoalesceWaitTime", configString: "?writeCoalesceWaitTime=", err: fmt.Errorf("failed for: writeCoalesceWaitTime = ")},
		{info: "empty ok username", configString: "?username=", clusterConfig: cfgWithAuth(gocql.PasswordAuthenticator{})},
		{info: "empty ok password", configString: "?password=", clusterConfig: cfgWithAuth(gocql.PasswordAuthenticator{})},
		{info: "empty enableHostVerification", configString: "?enableHostVerification=", err: fmt.Errorf("failed for: enableHostVerification = ")},
		{info: "empty ok caPath", configString: "?caPath=", clusterConfig: cfgWithSsl(&gocql.SslOptions{})},
		{info: "empty ok certPath", configString: "?certPath=", clusterConfig: cfgWithSsl(&gocql.SslOptions{})},
		{info: "empty ok keyPath", configString: "?keyPath=", clusterConfig: cfgWithSsl(&gocql.SslOptions{})},

		// QueryUnescape
		{info: "failed QueryUnescape username", configString: "?username=%GG", err: fmt.Errorf("failed for: username = %%GG")},
		{info: "failed QueryUnescape password", configString: "?password=%GG", err: fmt.Errorf("failed for: password = %%GG")},
		{info: "failed QueryUnescape caPath", configString: "?caPath=%GG", err: fmt.Errorf("failed for: caPath = %%GG")},
		{info: "failed QueryUnescape certPath", configString: "?certPath=%GG", err: fmt.Errorf("failed for: certPath = %%GG")},
		{info: "failed QueryUnescape keyPath", configString: "?keyPath=%GG", err: fmt.Errorf("failed for: keyPath = %%GG")},

		// ParseBool
		{info: "failed ParseBool ignorePeerAddr", configString: "?ignorePeerAddr=foobar", err: fmt.Errorf("failed for: ignorePeerAddr = foobar")},
		{info: "failed ParseBool disableInitialHostLookup", configString: "?disableInitialHostLookup=foobar", err: fmt.Errorf("failed for: disableInitialHostLookup = foobar")},
		{info: "failed ParseBool enableHostVerification", configString: "?enableHostVerification=foobar", err: fmt.Errorf("failed for: enableHostVerification = foobar")},

		// ParseDuration
		{info: "failed ParseDuration timeout", configString: "?timeout=42", err: fmt.Errorf("failed for: timeout = 42")},
		{info: "failed ParseDuration connectTimeout", configString: "?connectTimeout=42", err: fmt.Errorf("failed for: connectTimeout = 42")},
		{info: "failed ParseDuration writeCoalesceWaitTime", configString: "?writeCoalesceWaitTime=42", err: fmt.Errorf("failed for: writeCoalesceWaitTime = 42")},

		// Non errors
		{info: "empty", configString: "", clusterConfig: NewClusterConfig()},
		{info: "Consistency any", configString: "?consistency=any", clusterConfig: cfgWith(func(cfg *gocql.ClusterConfig) { cfg.Consistency = 0 })},
		{info: "Consistency one", configString: "?consistency=one", clusterConfig: cfgWith(func(cfg *gocql.ClusterConfig) { cfg.Consistency = 1 })},
		{info: "Timeout < 0", configString: "?timeout=-1s", clusterConfig: NewClusterConfig()},
		{info: "Timeout > 0", configString: "?timeout=1s", clusterConfig: cfgWith(func(cfg *gocql.ClusterConfig) { cfg.Timeout = time.Second })},
		{info: "ConnectTimeout < 0", configString: "?connectTimeout=-1s", clusterConfig: NewClusterConfig()},
		{info: "ConnectTimeout > 0", configString: "?connectTimeout=1s", clusterConfig: cfgWith(func(cfg *gocql.ClusterConfig) { cfg.ConnectTimeout = time.Second })},
		{info: "Keyspace", configString: "?keyspace=system", clusterConfig: cfgWith(func(cfg *gocql.ClusterConfig) { cfg.Keyspace = "system" })},
		{info: "NumConns < 1", configString: "?numConns=0", clusterConfig: NewClusterConfig()},
		{info: "NumConns > 1", configString: "?numConns=2", clusterConfig: cfgWith(func(cfg *gocql.ClusterConfig) { cfg.NumConns = 2 })},
		{info: "IgnorePeerAddr true", configString: "?ignorePeerAddr=true", clusterConfig: cfgWith(func(cfg *gocql.ClusterConfig) { cfg.IgnorePeerAddr = true })},
		{info: "DisableInitialHostLookup true", configString: "?disableInitialHostLookup=true", clusterConfig: cfgWith(func(cfg *gocql.ClusterConfig) { cfg.DisableInitialHostLookup = true })},
		{info: "WriteCoalesceWaitTime 1s", configString: "?writeCoalesceWaitTime=1s", clusterConfig: cfgWith(func(cfg *gocql.ClusterConfig) { cfg.WriteCoalesceWaitTime = time.Second })},
		{info: "Host", configString: "one", clusterConfig: cfgWith(func(cfg *gocql.ClusterConfig) { cfg.Hosts = []string{"one"} })},
		{info: "Hosts", configString: "one,two,three", clusterConfig: cfgWith(func(cfg *gocql.ClusterConfig) { cfg.Hosts = []string{"one", "two", "three"} })},
		{info: "Host & Consistency any", configString: "one?consistency=any", clusterConfig: cfgWith(func(cfg *gocql.ClusterConfig) { cfg.Consistency = 0; cfg.Hosts = []string{"one"} })},
		{info: "Hosts & Consistency one", configString: "one,two,three?consistency=one", clusterConfig: cfgWith(func(cfg *gocql.ClusterConfig) { cfg.Consistency = 1; cfg.Hosts = []string{"one", "two", "three"} })},
		// - optional PasswordAuthenticator
		{info: "PasswordAuthenticator Username", configString: "?username=alice%40bob.com", clusterConfig: cfgWithAuth(gocql.PasswordAuthenticator{Username: "alice@bob.com"})},
		{info: "PasswordAuthenticator Password", configString: "?password=top%24ecret", clusterConfig: cfgWithAuth(gocql.PasswordAuthenticator{Password: "top$ecret"})},
		{info: "PasswordAuthenticator", configString: "?username=alice%40bob.com&password=top%24ecret", clusterConfig: cfgWithAuth(gocql.PasswordAuthenticator{Username: "alice@bob.com", Password: "top$ecret"})},
		// - optional SslOptions
		{info: "SslOptions EnableHostVerification true", configString: "?enableHostVerification=true", clusterConfig: cfgWithSsl(&gocql.SslOptions{EnableHostVerification: true})},
		{info: "SslOptions CaPath", configString: "?caPath=/some%20path.pem", clusterConfig: cfgWithSsl(&gocql.SslOptions{CaPath: "/some path.pem"})},
		{info: "SslOptions CertPath", configString: "?certPath=/some+path.pem", clusterConfig: cfgWithSsl(&gocql.SslOptions{CertPath: "/some path.pem"})},
		{info: "SslOptions KeyPath", configString: "?keyPath=/some path.pem", clusterConfig: cfgWithSsl(&gocql.SslOptions{KeyPath: "/some path.pem"})},
		{info: "SslOptions", configString: "?caPath=/ca/path&certPath=/cert/path&keyPath=/key/path&enableHostVerification=1", clusterConfig: cfgWithSsl(&gocql.SslOptions{CaPath: "/ca/path", CertPath: "/cert/path", KeyPath: "/key/path", EnableHostVerification: true})},
	}

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
