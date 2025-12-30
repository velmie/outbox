//go:build integration

package testutil

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"

	_ "github.com/go-sql-driver/mysql"
)

const (
	mysqlImage          = "mysql:8.0.36"
	mysqlDatabase       = "outbox"
	mysqlUser           = "root"
	mysqlPassword       = "secret"
	cliContainerImage   = "alpine:3.20"
	cliContainerPath    = "/cli"
	cliExitTimeout      = 2 * time.Minute
	mysqlStartupTimeout = 2 * time.Minute
)

type MySQLContainer struct {
	Container testcontainers.Container
	Network   *testcontainers.DockerNetwork
	DB        *sql.DB
	DSN       string
}

func StartMySQLContainer(t *testing.T, ctx context.Context) MySQLContainer {
	t.Helper()

	net, err := network.New(ctx)
	if err != nil {
		t.Skipf("create network: %v", err)
	}
	t.Cleanup(func() {
		_ = net.Remove(ctx)
	})

	port := nat.Port("3306/tcp")
	req := testcontainers.ContainerRequest{
		Image:        mysqlImage,
		ExposedPorts: []string{string(port)},
		Env: map[string]string{
			"MYSQL_ROOT_PASSWORD": mysqlPassword,
			"MYSQL_DATABASE":      mysqlDatabase,
		},
		Networks: []string{net.Name},
		NetworkAliases: map[string][]string{
			net.Name: {"mysql"},
		},
		WaitingFor: wait.ForSQL(port, "mysql", func(host string, port nat.Port) string {
			return fmt.Sprintf(
				"%s:%s@tcp(%s:%s)/%s?parseTime=true&multiStatements=true",
				mysqlUser,
				mysqlPassword,
				host,
				port.Port(),
				mysqlDatabase,
			)
		}).WithStartupTimeout(mysqlStartupTimeout),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Skipf("start mysql container: %v", err)
	}
	t.Cleanup(func() {
		_ = container.Terminate(ctx)
	})

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("resolve host: %v", err)
	}
	mappedPort, err := container.MappedPort(ctx, port)
	if err != nil {
		t.Fatalf("resolve port: %v", err)
	}

	dsnHost := fmt.Sprintf(
		"%s:%s@tcp(%s:%s)/%s?parseTime=true&multiStatements=true",
		mysqlUser,
		mysqlPassword,
		host,
		mappedPort.Port(),
		mysqlDatabase,
	)
	db, err := sql.Open("mysql", dsnHost)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})

	containerDSN := fmt.Sprintf(
		"%s:%s@tcp(mysql:3306)/%s?parseTime=true&multiStatements=true",
		mysqlUser,
		mysqlPassword,
		mysqlDatabase,
	)

	return MySQLContainer{
		Container: container,
		Network:   net,
		DB:        db,
		DSN:       containerDSN,
	}
}

func BuildBinary(t *testing.T, pkg string) string {
	t.Helper()

	name := filepath.Base(pkg)
	if name == "." {
		wd, err := os.Getwd()
		if err != nil {
			t.Fatalf("resolve working dir: %v", err)
		}
		name = filepath.Base(wd)
	}
	bin := filepath.Join(t.TempDir(), name)
	cmd := exec.Command("go", "build", "-o", bin, pkg)
	cmd.Env = append(os.Environ(),
		"CGO_ENABLED=0",
		"GOOS=linux",
		"GOARCH="+runtime.GOARCH,
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("build %s: %v\n%s", pkg, err, string(out))
	}

	return bin
}

func RunCLIContainer(t *testing.T, ctx context.Context, networkName, binaryPath string, args []string) (int, string) {
	t.Helper()

	req := testcontainers.ContainerRequest{
		Image:      cliContainerImage,
		Entrypoint: []string{cliContainerPath},
		Cmd:        args,
		Networks:   []string{networkName},
		Files: []testcontainers.ContainerFile{
			{
				HostFilePath:      binaryPath,
				ContainerFilePath: cliContainerPath,
				FileMode:          0o755,
			},
		},
		WaitingFor: wait.ForExit().WithExitTimeout(cliExitTimeout),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("start cli container: %v", err)
	}
	t.Cleanup(func() {
		_ = container.Terminate(ctx)
	})

	logsReader, err := container.Logs(ctx)
	if err != nil {
		t.Fatalf("read cli logs: %v", err)
	}
	defer logsReader.Close()

	logs, err := io.ReadAll(logsReader)
	if err != nil {
		t.Fatalf("read cli logs: %v", err)
	}

	state, err := container.State(ctx)
	if err != nil {
		t.Fatalf("read cli state: %v", err)
	}

	return state.ExitCode, string(logs)
}
