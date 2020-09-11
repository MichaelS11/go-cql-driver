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
	TestLogStderr             = log.New(os.Stderr, "cql ", log.Ldate|log.Ltime|log.LUTC|log.Lshortfile)
	TestHostValid             string
	TestHostInvalid           string
	ConnectTimeoutValidString string
	ConnectTimeoutValid       time.Duration
	ConnectTimeoutInvalid     time.Duration
	TimeoutValidString        string
	TimeoutValid              time.Duration
	DisableDestructiveTests   bool
	KeyspaceName              = "cqltest"
	TableName                 = "cqltest_"
	EnableAuthentication      bool
	Username                  string
	Password                  string
	TestTimeNow               time.Time
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
	flag.StringVar(&TestHostValid, "hostValid", "127.0.0.1", "a host where a Cassandra database is running")
	flag.StringVar(&TestHostInvalid, "hostInvalid", "169.254.200.200", "a host where a Cassandra database is not running")
	flag.StringVar(&ConnectTimeoutValidString, "connectTimeoutValid", "20s", "the connect timeout time duration for host valid tests (ClusterConfig.ConnectTimeout)")
	connectTimeoutInvalidString := flag.String("connectTimeoutInvalid", "1ms", "the connect timeout time duration for host invalid tests (ClusterConfig.ConnectTimeout)")
	flag.StringVar(&TimeoutValidString, "timeoutValid", "10s", "the timeout time duration for host valid tests (ClusterConfig.Timeout)")
	flag.BoolVar(&DisableDestructiveTests, "disableDestructiveTests", false, "set to disable the destructive database tests on cqltest keyspace")
	flag.BoolVar(&EnableAuthentication, "enableAuthentication", false, "set to enable authentication when database requires username and password")
	flag.StringVar(&Username, "username", "cassandra", "the username to use when database requires username and password")
	flag.StringVar(&Password, "password", "cassandra", "the password to use when database requires username and password")
	flag.Parse()

	var err error
	ConnectTimeoutValid, err = time.ParseDuration(ConnectTimeoutValidString)
	if err != nil {
		fmt.Println("connectTimeoutValid ParseDuration error:", err)
		return 2
	}
	ConnectTimeoutInvalid, err = time.ParseDuration(*connectTimeoutInvalidString)
	if err != nil {
		fmt.Println("connectTimeoutInvalid ParseDuration error:", err)
		return 4
	}
	TimeoutValid, err = time.ParseDuration(TimeoutValidString)
	if err != nil {
		fmt.Println("timeoutValid ParseDuration error:", err)
		return 6
	}

	TestTimeNow = time.Now().UTC().Truncate(time.Millisecond)
	TableName += TestTimeNow.Format("20060102150405")

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
	openString := TestHostValid
	if EnableAuthentication {
		openString += "?username=" + Username + "&password=" + Password
	}
	conn, err := CqlDriver.Open(openString)
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
