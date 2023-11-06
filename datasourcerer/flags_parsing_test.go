package main

import (
	"flag"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

// This function will test configureFlags
func Test_ConfigureFlags_NoEnvironmentVariable_AbsFilePath(t *testing.T) {
	// Reset flag.CommandLine to avoid flag redefinition error when tests are run multiple times.
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)

	// Define the command-line arguments you want to test
	os.Args = []string{"cmd", "-template-dir", "/abs/loc/templates", "-test-dir", "/abs/loc/unittests", "-config-file", "/abs/loc/config.yaml"}

	// Call configureFlags, which parses the command-line flags
	cfg := configureFlags()
	flag.Parse()
	_ = cfg.configureDefaults(logger)

	assert.Equal(t, "/abs/loc/templates/", cfg.templateDir)
	assert.Equal(t, "/abs/loc/unittests/", cfg.unitTestDir)
	assert.Equal(t, "/abs/loc/config.yaml", cfg.configPath)
}

func Test_ConfigureFlags_NoEnvironmentVariable_RelativeFilePath(t *testing.T) {
	// Reset flag.CommandLine to avoid flag redefinition error when tests are run multiple times.
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)

	// Define the command-line arguments you want to test
	os.Args = []string{"cmd", "-template-dir", "./rel/loc/templates", "-test-dir", "./rel/loc/unittests", "-config-file", "./rel/loc/config.yaml"}

	// Call configureFlags, which parses the command-line flags
	cfg := configureFlags()
	flag.Parse()
	_ = cfg.configureDefaults(logger)

	assert.Regexp(t, `/.*?/rel/loc/templates/`, cfg.templateDir)
	assert.True(t, filepath.IsAbs(cfg.templateDir))
	assert.Regexp(t, `/.*?/rel/loc/unittests/`, cfg.unitTestDir)
	assert.True(t, filepath.IsAbs(cfg.unitTestDir))
	assert.Regexp(t, `/.*?/rel/loc/config.yaml`, cfg.configPath)
	assert.True(t, filepath.IsAbs(cfg.configPath))
}

func Test_ConfigureFlags_EnvironmentVariable_RelativeFilePath(t *testing.T) {
	// Reset flag.CommandLine to avoid flag redefinition error when tests are run multiple times.
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)

	// Define the command-line arguments you want to test
	os.Args = []string{"cmd", "-template-dir", "./rel/loc/templates", "-test-dir", "./rel/loc/unittests", "-config-file", "./rel/loc/config.yaml"}
	os.Setenv("DBT_PROJECT_DIR", "/abs/loc")

	// Call configureFlags, which parses the command-line flags
	cfg := configureFlags()
	flag.Parse()
	_ = cfg.configureDefaults(logger)

	assert.Equal(t, "/abs/loc/rel/loc/templates/", cfg.templateDir)
	assert.Equal(t, "/abs/loc/rel/loc/unittests/", cfg.unitTestDir)
	// assert.Equal(t, "/abs/loc/rel/loc/config.yaml", cfg.configPath)
}
