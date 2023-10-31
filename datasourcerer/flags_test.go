package main_test

import (
	"flag"
	"os"
	"testing"

	main "github.com/tsanton/dbt-unit-test-fusionizer"
)

func Test_Cases(t *testing.T) {
	// Backup original command line arguments
	origArgs := os.Args
	defer func() { os.Args = origArgs }()

	cases := main.Cases{}

	// Set up command line arguments for the test
	os.Args = []string{"cmd", "--case=foo", "-case=bar", "--c=baz", "-c=qux"}

	// Define and parse flags
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
	flag.Var(&cases, "case", "file name to target for test case parsing")
	flag.Var(&cases, "c", "file name to target for test case parsing")
	flag.Parse()

	// Check the results
	expected := []string{"foo", "bar", "baz", "qux"}
	if len(cases) != len(expected) {
		t.Fatalf("Expected %d names, got %d", len(expected), len(cases))
	}
	for i, arg := range cases {
		if arg != expected[i] {
			t.Errorf("Expected name %d to be %s, got %s", i, expected[i], arg)
		}
	}
}
