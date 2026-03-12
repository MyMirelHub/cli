//go:build !windows && e2e

/*
Copyright 2025 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package standalone_test

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWorkflowListMongoStateStore(t *testing.T) {
	_, hasRuntimePinned := os.LookupEnv("DAPR_RUNTIME_PINNED_VERSION")
	_, hasDashboardPinned := os.LookupEnv("DAPR_DASHBOARD_PINNED_VERSION")
	managedInstall := hasRuntimePinned && hasDashboardPinned

	if isSlimMode() {
		t.Skip("skipping workflow tests in slim mode")
	}

	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker is required for mongo workflow e2e")
	}

	mongoPort, err := getFreeLocalPort()
	require.NoError(t, err)

	mongoContainer := fmt.Sprintf("dapr-e2e-wf-mongo-%d", time.Now().UnixNano())
	cleanupMongo := func() {
		_ = exec.Command("docker", "rm", "-f", mongoContainer).Run()
	}
	cleanupMongo()
	t.Cleanup(cleanupMongo)

	dockerPortArg := fmt.Sprintf("%d:27017", mongoPort)
	require.NoError(t, exec.Command("docker", "run", "-d", "--name", mongoContainer, "-p", dockerPortArg, "mongo:7", "--replSet", "rs0").Run())

	// Give mongod time to start accepting exec commands.
	time.Sleep(3 * time.Second)
	require.NoError(t, exec.Command("docker", "exec", mongoContainer, "mongosh", "--eval", "rs.initiate()").Run())

	for i := 0; i < 10; i++ {
		err := exec.Command("docker", "exec", mongoContainer, "mongosh", "--quiet", "--eval", "const h=db.hello(); quit(h.isWritablePrimary ? 0 : 1)").Run()
		if err == nil {
			break
		}
		if i == 9 {
			require.NoError(t, err)
		}
		time.Sleep(time.Second)
	}

	if managedInstall {
		cmdUninstall()
		ensureDaprInstallation(t)
		t.Cleanup(func() {
			must(t, cmdUninstall, "failed to uninstall Dapr")
		})
	} else {
		t.Log("running mongo workflow e2e with existing local dapr installation")
		ensureLocalDaprRuntimeInstallation(t)
	}

	runFilePath := createMongoWorkflowRunTemplate(t, mongoPort)
	appID := "test-workflow-mongodb"
	t.Cleanup(func() {
		_, _ = cmdStopWithAppID(appID)
	})

	go func() {
		o, _ := cmdRun("", "-f", runFilePath)
		t.Log(o)
	}()

	time.Sleep(6 * time.Second)
	_, err = cmdWorkflowRun(appID, "SimpleWorkflow", "--instance-id=mongo-e2e-1")
	require.NoError(t, err)

	time.Sleep(3 * time.Second)
	output, err := cmdWorkflowList(appID, "-o", "json")
	require.NoError(t, err, output)

	var list []map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(output), &list))

	found := false
	for _, item := range list {
		if item["instanceID"] == "mongo-e2e-1" {
			assert.Equal(t, "COMPLETED", item["runtimeStatus"])
			found = true
			break
		}
	}

	assert.True(t, found, "workflow list must include instance created in Mongo-backed store")
}

func getFreeLocalPort() (int, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer ln.Close()

	addr, ok := ln.Addr().(*net.TCPAddr)
	if !ok {
		return 0, fmt.Errorf("failed to resolve local TCP port")
	}

	return addr.Port, nil
}

func createMongoWorkflowRunTemplate(t *testing.T, mongoPort int) string {
	t.Helper()

	daprHTTPPort, err := getFreeLocalPort()
	require.NoError(t, err)
	daprGRPCPort, err := getFreeLocalPort()
	require.NoError(t, err)

	wd, err := os.Getwd()
	require.NoError(t, err)

	appDir := filepath.Join(wd, "..", "..", "apps", "workflow")
	tmpDir := t.TempDir()
	resourcesDir := filepath.Join(tmpDir, "resources")
	require.NoError(t, os.MkdirAll(resourcesDir, 0o755))

	component := fmt.Sprintf(`apiVersion: dapr.io/v1alpha1
kind: Component
metadata:
  name: statestore
spec:
  type: state.mongodb
  version: v1
  metadata:
    - name: host
      value: "localhost:%d"
    - name: databaseName
      value: "workflowCustomDB"
    - name: collectionName
      value: "workflowCustomCollection"
    - name: params
      value: "?replicaSet=rs0&directConnection=true"
    - name: actorStateStore
      value: "true"
`, mongoPort)
	require.NoError(t, os.WriteFile(filepath.Join(resourcesDir, "statestore.yaml"), []byte(component), 0o644))

	runTemplate := strings.Join([]string{
		"version: 1",
		"common:",
		fmt.Sprintf("  resourcesPath: %s", resourcesDir),
		"apps:",
		"- appID: test-workflow-mongodb",
		fmt.Sprintf("  appDirPath: %s", appDir),
		"  command: [\"go\", \"run\", \"app.go\"]",
		"  appLogDestination: console",
		"  daprdLogDestination: console",
		fmt.Sprintf("  daprHTTPPort: %d", daprHTTPPort),
		fmt.Sprintf("  daprGRPCPort: %d", daprGRPCPort),
		"  schedulerHostAddress: 127.0.0.1:50006",
		"  placementHostAddress: 127.0.0.1:50005",
	}, "\n") + "\n"

	runFilePath := filepath.Join(tmpDir, "test-workflow-mongodb.yaml")
	require.NoError(t, os.WriteFile(runFilePath, []byte(runTemplate), 0o644))
	return runFilePath
}

func ensureLocalDaprRuntimeInstallation(t *testing.T) {
	t.Helper()

	home, err := os.UserHomeDir()
	require.NoError(t, err)

	daprdPath := filepath.Join(home, ".dapr", "bin", "daprd")
	if _, err := os.Stat(daprdPath); err == nil {
		return
	}

	t.Log("daprd not found in ~/.dapr/bin, running 'dapr init' for local test prerequisites")
	output, err := cmdInit()
	require.NoError(t, err, output)
}
