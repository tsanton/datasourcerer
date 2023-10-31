package templatecrawler

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type DataSourceReference struct {
	TestDefintionFilePath string
	DataSourceFilePath    string
	CallLine              int
	EndCallLine           int
}

type TemplateCrawler struct {
	logger               *slog.Logger
	workers              int
	initDir              string
	dataSourceReferences []chan string
	dbtFiles             []TestTemplateFile
}

func NewTestTemplateCrawler(logger *slog.Logger, workers int, initDir string) *TemplateCrawler {
	return &TemplateCrawler{
		logger:  logger,
		workers: workers,
		initDir: initDir,
	}
}

func (s *TemplateCrawler) DataSourceReferences() *[]chan string {
	return &s.dataSourceReferences
}

func (s *TemplateCrawler) GetTestTemplates() *[]TestTemplateFile {
	return &s.dbtFiles
}

type dbtFileJob struct {
	path string
}

func (s *TemplateCrawler) Crawl(c chan<- DataSourceReference) {
	s.dbtFiles = []TestTemplateFile{}

	jobs := make(chan dbtFileJob)
	var wg sync.WaitGroup

	// Create worker goroutines
	for i := 0; i < s.workers; i++ {
		go func() {
			for j := range jobs {
				err := s.processTestTemplateFile(j.path, c)
				if err != nil {

					s.logger.Error(fmt.Sprintf("error processing test template file '%s': %s", j.path, err.Error()))
				}
				wg.Done()
			}
		}()
	}

	s.logger.Info(fmt.Sprintf("looking for test files with prefix 'test_' in '%s'", s.initDir))
	err := filepath.Walk(s.initDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			s.logger.Error(fmt.Sprintf("error accessing a path %q: %v", path, err))
			close(c)
			return err
		}

		if info.IsDir() {
			return nil
		}

		// Match "test_" and ".sql" ignoring case
		lowerPath := strings.ToLower(info.Name())
		if strings.HasPrefix(lowerPath, "test_") && strings.HasSuffix(lowerPath, ".sql") {
			wg.Add(1)
			jobs <- dbtFileJob{path: path}
		}
		return nil
	})
	if err != nil {
		s.logger.Error("error walking the path: %v\n", err)
		close(c)
		return
	}

	wg.Wait()
	close(jobs) // Close jobs channel after all jobs have been processed
	close(c)
}

func (s *TemplateCrawler) processTestTemplateFile(path string, c chan<- DataSourceReference) error {
	fileWorker := NewTestTemplateFile(s.logger, s.initDir)
	fileWorker.ProccessFile(path)
	for _, dataSource := range *fileWorker.DataSourceReferences() {
		c <- dataSource
	}
	s.dbtFiles = append(s.dbtFiles, *fileWorker)
	return nil
}
