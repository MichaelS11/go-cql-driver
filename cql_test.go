package cql

import (
	"database/sql/driver"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"testing"
	"time"
)

var (
	TestLogStderr         = log.New(os.Stderr, "cql ", log.Ldate|log.Ltime|log.LUTC|log.Llongfile)
	TestHostValid         string
	TestHostInvalid       string
	ConnectTimeoutValid   time.Duration
	ConnectTimeoutInvalid time.Duration
	TimeoutValid          time.Duration
)

func TestMain(m *testing.M) {
	code := setupForTesting()
	if code != 0 {
		os.Exit(code)
	}
	code = m.Run()
	os.Exit(code)
}

func setupForTesting() int {
	hostValid := flag.String("hostValid", "127.0.0.1", "a host where a Cassandra database is running")
	hostInvalid := flag.String("hostInvalid", "169.254.200.200", "a host where a Cassandra database is not running")
	connectTimeoutValidString := flag.String("connectTimeoutValid", "10s", "the connect timeout time duration for host valid tests (ClusterConfig.ConnectTimeout)")
	connectTimeoutInvalidString := flag.String("connectTimeoutInvalid", "1ms", "the connect timeout time duration for host invalid tests (ClusterConfig.ConnectTimeout)")
	timeoutValidString := flag.String("timeoutValid", "10s", "the timeout time duration for host valid tests (ClusterConfig.Timeout)")
	flag.Parse()
	TestHostValid = *hostValid
	TestHostInvalid = *hostInvalid
	var err error
	ConnectTimeoutValid, err = time.ParseDuration(*connectTimeoutValidString)
	if err != nil {
		fmt.Println("connectTimeoutValid ParseDuration error:", err)
		return 2
	}
	ConnectTimeoutInvalid, err = time.ParseDuration(*connectTimeoutInvalidString)
	if err != nil {
		fmt.Println("connectTimeoutInvalid ParseDuration error:", err)
		return 4
	}
	TimeoutValid, err = time.ParseDuration(*timeoutValidString)
	if err != nil {
		fmt.Println("timeoutValid ParseDuration error:", err)
		return 6
	}
	return 0
}

func TestDriverOpen(t *testing.T) {
	CqlDriver.Logger = nil
	conn, err := CqlDriver.Open("")
	if err != nil {
		t.Fatalf("Open error - received: %v - expected: %v ", err, nil)
	}
	if conn == nil {
		t.Fatal("conn is nil")
	}

	CqlDriver.Logger = TestLogStderr
	conn, err = CqlDriver.Open("")
	if err != nil {
		t.Fatalf("Open error - received: %v - expected: %v ", err, nil)
	}
	if conn == nil {
		t.Fatal("conn is nil")
	}

	conn, err = CqlDriver.Open("?blah")
	expectedError := "ConfigStringToClusterConfig error: missing ="
	if err == nil || err.Error() != expectedError {
		t.Fatalf("Open error - received: %v - expected: %v ", err, expectedError)
	}
	if conn != nil {
		t.Fatalf("Open conn - received: %v - expected: %v ", conn, nil)
	}
}

func testGetConnectionHostValid(t *testing.T) driver.Conn {
	conn, err := CqlDriver.Open(TestHostValid)
	if err != nil {
		t.Fatalf("Open error - received: %v - expected: %v ", err, nil)
	}
	if conn == nil {
		t.Fatal("conn is nil")
	}
	cqlConn := conn.(*cqlConnStruct)
	cqlConn.clusterConfig.ConnectTimeout = ConnectTimeoutValid
	cqlConn.clusterConfig.Timeout = TimeoutValid
	return conn
}

func testGetConnectionHostInvalid(t *testing.T) driver.Conn {
	conn, err := CqlDriver.Open(TestHostInvalid)
	if err != nil {
		t.Fatalf("Open error - received: %v - expected: %v ", err, nil)
	}
	if conn == nil {
		t.Fatal("conn is nil")
	}
	cqlConn := conn.(*cqlConnStruct)
	cqlConn.clusterConfig.ConnectTimeout = ConnectTimeoutInvalid
	cqlConn.logger = log.New(ioutil.Discard, "", 0)
	return conn
}
