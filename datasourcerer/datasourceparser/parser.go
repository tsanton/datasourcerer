package datasourceparser

import (
	"fmt"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/tsanton/dbt-unit-test-fusionizer/formatter"
	"github.com/tsanton/dbt-unit-test-fusionizer/templatecrawler"
	"gopkg.in/yaml.v3"
)

type Parser[T formatter.IDataSourceFormatter] struct {
	logger             *slog.Logger
	workers            int
	dataSourceFiles    map[string]DataSourceFile
	defaultConfig      *formatter.Config
	formatterGenerator func(*slog.Logger, *formatter.Config) T
	mu                 sync.Mutex
}

func (s *Parser[T]) GetDataSources() *map[string]DataSourceFile {
	return &s.dataSourceFiles
}

func NewDatasourceParser[T formatter.IDataSourceFormatter](logger *slog.Logger, workers int, config *formatter.Config, constructor func(*slog.Logger, *formatter.Config) T) *Parser[T] {
	return &Parser[T]{
		logger:             logger,
		workers:            workers,
		defaultConfig:      config,
		formatterGenerator: constructor,
		dataSourceFiles:    make(map[string]DataSourceFile),
		mu:                 sync.Mutex{},
	}
}

type DataSourceFile struct {
	filePath    string
	lastChanged time.Time
	Formatter   formatter.IDataSourceFormatter
}

func (d *DataSourceFile) LastChanged() time.Time {
	return d.lastChanged
}

type dataSourceJob struct {
	dataSourceFilePath string
}

func (s *Parser[T]) Parse(c <-chan templatecrawler.DataSourceReference) {
	jobs := make(chan dataSourceJob)
	var wg sync.WaitGroup

	// Create worker goroutines
	for i := 0; i < s.workers; i++ {
		go func() {
			for j := range jobs {
				_, ok := s.dataSourceFiles[j.dataSourceFilePath]
				if !ok {
					err := s.processDataSource(j)
					if err != nil {
						s.logger.Debug(fmt.Sprintf("error processing data source '%s': %s", j.dataSourceFilePath, err.Error())) //Debug here is ok, because it's already logged in processDataSource
					}
				} else {
					s.logger.Debug(fmt.Sprintf("data source '%s' already processed", j.dataSourceFilePath))
				}
				wg.Done()
			}
		}()
	}

	// Push work onto the job queue
	for ref := range c {
		lowerPath := strings.ToLower(ref.DataSourceFilePath)
		wg.Add(1)
		jobs <- dataSourceJob{dataSourceFilePath: lowerPath}
	}
	wg.Wait()
	close(jobs) // Close jobs channel after all jobs have been processed
}

func (s *Parser[T]) processDataSource(job dataSourceJob) error {
	s.logger.Debug(fmt.Sprintf("processing data source '%s'", job.dataSourceFilePath))
	fileInfo, err := os.Stat(job.dataSourceFilePath)
	if err != nil {
		s.logger.Error(fmt.Sprintf("file '%s' not found", job.dataSourceFilePath))
		s.mu.Lock()
		s.dataSourceFiles[job.dataSourceFilePath] = DataSourceFile{
			filePath:    job.dataSourceFilePath,
			lastChanged: time.Date(9999, 12, 31, 23, 59, 0, 0, time.UTC),
			Formatter:   NewErrorFormatter(s.logger, fmt.Errorf("error reading file '%s': file not found", job.dataSourceFilePath)),
		}
		s.mu.Unlock()
		return err
	}

	config := &formatter.Config{}
	yamlFile, err := os.ReadFile(path.Join(filepath.Dir(job.dataSourceFilePath), ".datasourcerer.yaml"))
	if err == nil {
		s.logger.Debug(fmt.Sprintf("using config override '%s' to parse file '%s'", path.Join(filepath.Dir(job.dataSourceFilePath), ".datasourcerer.yaml"), job.dataSourceFilePath))
		err = yaml.Unmarshal(yamlFile, config)
		if err != nil {
			s.logger.Error(fmt.Sprintf("error reading config override '%s': %s", path.Join(filepath.Dir(job.dataSourceFilePath), ".datasourcerer.yaml"), err.Error()))
		}
		if config.Filetype == "csv" && !config.CSV.Validate() {
			s.logger.Error(fmt.Sprintf("csv config is not valid in directory '%s'. Using default CSV config", filepath.Dir(job.dataSourceFilePath)))
			config = &formatter.Config{
				Filetype: "csv",
				CSV:      formatter.NewDefaultCsvConfig(),
			}
		}
	} else {
		s.logger.Debug(fmt.Sprintf("using default config to parse file '%s'", job.dataSourceFilePath))
		config = s.defaultConfig
		if config.Filetype == "csv" && !config.CSV.Validate() {
			s.logger.Debug(fmt.Sprintf("using default csv config to parse file '%s'", job.dataSourceFilePath))
			config.CSV = formatter.NewDefaultCsvConfig()
		}
	}

	file, _ := os.Open(job.dataSourceFilePath)
	f := s.formatterGenerator(s.logger, config)
	err = f.Read(file)
	if err != nil {
		s.logger.Error(fmt.Sprintf("failed to parse data source '%s'. %s", job.dataSourceFilePath, err.Error()))
		s.mu.Lock()
		s.dataSourceFiles[job.dataSourceFilePath] = DataSourceFile{
			filePath:    job.dataSourceFilePath,
			lastChanged: fileInfo.ModTime(),
			Formatter:   NewErrorFormatter(s.logger, err),
		}
		s.mu.Unlock()
		return err
	}

	//read and parse the content here, and add it as a []byte to the DataSourceFile{}? Shouldn't due to lazy loading and lastChanged
	s.mu.Lock()
	s.dataSourceFiles[job.dataSourceFilePath] = DataSourceFile{
		filePath:    job.dataSourceFilePath,
		lastChanged: fileInfo.ModTime(),
		Formatter:   f,
	}
	s.mu.Unlock()
	return nil
}
