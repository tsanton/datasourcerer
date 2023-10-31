package unit_test

import (
	"log/slog"
	"os"
	"testing"
)

var logger *slog.Logger

func TestMain(m *testing.M) {
	var loggingLevel = new(slog.LevelVar)
	loggingLevel.Set(slog.LevelInfo)
	logger = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: loggingLevel}))
	slog.SetDefault(logger)

	exit := m.Run()

	os.Exit(exit)
}
