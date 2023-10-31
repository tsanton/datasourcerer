package generator

import (
	"bufio"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/samber/lo"
	"github.com/tsanton/dbt-unit-test-fusionizer/datasourceparser"
	"github.com/tsanton/dbt-unit-test-fusionizer/templatecrawler"
)

type Generator struct {
	logger       *slog.Logger
	outputAbsDir string
	workers      int
}

func NewTestGenerator(logger *slog.Logger, workerCount int, initDir string) *Generator {
	outputAbsDir, _ := filepath.Abs(initDir)
	return &Generator{
		logger:       logger,
		outputAbsDir: outputAbsDir,
		workers:      workerCount,
	}
}

type mergeFileJob struct {
	testFilePath     string
	testTemplateFile *templatecrawler.TestTemplateFile
	dataSources      *map[string]datasourceparser.DataSourceFile
}

func (s *Generator) Generate(testTemplateFiles *[]templatecrawler.TestTemplateFile, dataSourceFiles *map[string]datasourceparser.DataSourceFile) error {
	jobs := make(chan mergeFileJob)
	var wg sync.WaitGroup

	// Create worker goroutines
	for i := 0; i < s.workers; i++ {
		go func() {
			for j := range jobs {
				err := s.mergeFiles(j.testFilePath, j.testTemplateFile, j.dataSources)
				if err != nil {
					s.logger.Error(fmt.Sprintf("error generating test file '%s': %s", j.testFilePath, err.Error())) //Debug here is ok, because it's already logged in processDataSource
				} else {
					s.logger.Debug(fmt.Sprintf("successfully generated test file '%s'", j.testFilePath))
				}
				wg.Done()
			}
		}()
	}

	//TODO: now it's a blobal datasources -> we must move this into the for loop and only scope out the datasources for the current test template
	lastDataSourceTouched := time.Date(1900, time.January, 1, 0, 0, 0, 0, time.UTC)
	for _, ds := range *dataSourceFiles {
		if ds.LastChanged().After(lastDataSourceTouched) {
			lastDataSourceTouched = ds.LastChanged()
		}
	}
	s.logger.Debug(fmt.Sprintf("data source was last updated: '%s'", lastDataSourceTouched.Format(time.RFC3339)))

	s.logger.Info(fmt.Sprintf("total of %d test template(s) found", len(*testTemplateFiles)))
	for _, templateFile := range *testTemplateFiles {
		testFilePath := path.Join(s.outputAbsDir, templateFile.RelativeFilePath())
		exist, testFileLastUpdated := fileLastTouched(testFilePath)
		s.logger.Debug(fmt.Sprintf("test file '%s' exist: %t.", testFilePath, exist))
		if !exist {
			s.logger.Debug(fmt.Sprintf("test file '%s' does not exist. Creating...", testFilePath))
			wg.Add(1)
			jobs <- mergeFileJob{
				testFilePath:     testFilePath,
				testTemplateFile: &templateFile,
				dataSources:      dataSourceFiles,
			}
		} else if lastDataSourceTouched.After(templateFile.LastChanged()) && lastDataSourceTouched.After(testFileLastUpdated) {
			s.logger.Debug("data source file(s) altered after last test template generation. Recreating...")
			wg.Add(1)
			jobs <- mergeFileJob{
				testFilePath:     testFilePath,
				testTemplateFile: &templateFile,
				dataSources:      dataSourceFiles,
			}
		} else if testFileLastUpdated.Before(templateFile.LastChanged()) {
			s.logger.Debug("test template was altered after the last test generation. Recreating...")
			wg.Add(1)
			jobs <- mergeFileJob{
				testFilePath:     testFilePath,
				testTemplateFile: &templateFile,
				dataSources:      dataSourceFiles,
			}
		} else {
			s.logger.Debug(fmt.Sprintf("test file '%s' does not need regeneration", testFilePath))
		}
	}
	wg.Wait()
	close(jobs) // Close jobs channel after all jobs have been processed
	return nil
}

func (s *Generator) mergeFiles(targetFilePath string, templateFile *templatecrawler.TestTemplateFile, dataSources *map[string]datasourceparser.DataSourceFile) error {
	sourceFile, err := os.Open(templateFile.AbsFilePath())
	if err != nil {
		s.logger.Error(fmt.Sprintf("error opening source file: '%s'", err.Error()))
		return err
	}
	defer sourceFile.Close()

	// Create the directory if it does not exist
	s.logger.Debug(fmt.Sprintf("making directory (if not exists): '%s'", filepath.Dir(targetFilePath)))
	if err := os.MkdirAll(filepath.Dir(targetFilePath), 0755); err != nil {
		s.logger.Error(fmt.Sprintf("Error creating directory '%s': %s", filepath.Dir(targetFilePath), err.Error()))
		return err
	}

	s.logger.Debug(fmt.Sprintf("creating file: '%s'", targetFilePath))
	targetFile, err := os.Create(targetFilePath)
	if err != nil {
		s.logger.Error(fmt.Sprintf("Error creating target file: '%s'", err.Error()))
		return err
	}
	defer targetFile.Close()
	sourceReader := bufio.NewReader(sourceFile)
	targetWriter := bufio.NewWriter(targetFile)
	defer targetWriter.Flush()
	_, err = targetWriter.WriteString(strings.TrimSpace(`
/*###############################################
### Do NOT modify: generated by datasourcerer ###
###############################################*/
`) + "\n")
	if err != nil {
		return err
	}

	dataSourceReferences := templateFile.DataSourceReferences()
	var endCallLines []int
	var wrappedLines []int
	for _, dsr := range *dataSourceReferences {
		endCallLines = append(endCallLines, dsr.EndCallLine)
		if dsr.CallLine == dsr.EndCallLine {
			wrappedLines = append(wrappedLines, dsr.CallLine)
		}
	}
	sourceLineIndex := 1 // Initialize line counter
	for {
		line, readErr := sourceReader.ReadString('\n')

		if readErr != nil && readErr != io.EOF {
			s.logger.Error(fmt.Sprintf("error creating target file: '%s'", err.Error()))
			return err
		}
		if lo.Contains(endCallLines, sourceLineIndex) {
			dsr, found := lo.Find[templatecrawler.DataSourceReference](*dataSourceReferences, func(x templatecrawler.DataSourceReference) bool { return x.EndCallLine == sourceLineIndex })
			if !found {
				return fmt.Errorf("error finding data source reference for line %d", sourceLineIndex)
			}

			if lo.Contains(wrappedLines, sourceLineIndex) {
				s.logger.Debug(fmt.Sprintf("handling wrapped call and endline data insert in file '%s', line %d", targetFilePath, sourceLineIndex))
				err = handleWrappedLine(targetWriter, line, &dsr, dataSources)
				if err != nil {
					s.logger.Debug(fmt.Sprintf("error handling wrapped call and endline data insert in file '%s', line %d. Error: %s", targetFilePath, sourceLineIndex, err.Error()))
					return err
				}
			} else {
				s.logger.Debug(fmt.Sprintf("inserting data source in file '%s', line %d", targetFilePath, sourceLineIndex))
				err = insertDataSource(targetWriter, &dsr, dataSources)
				if err != nil {
					s.logger.Debug(fmt.Sprintf("error inserting data source in file: '%s', line: %d. Error: %s", targetFilePath, sourceLineIndex, err.Error()))
					return err
				}
				_, err := targetWriter.WriteString(line)
				if err != nil {
					s.logger.Debug(fmt.Sprintf("error writing line after inserted datasource in file '%s', line %d. Error: %s", targetFilePath, sourceLineIndex, err.Error()))
					return err
				}
			}
		} else {
			_, err := targetWriter.WriteString(line)
			if err != nil {
				s.logger.Debug(fmt.Sprintf("error writing line in file '%s', line %d. Error: %s", targetFilePath, sourceLineIndex, err.Error()))
				return err
			}
		}

		// End of file is reached.
		if readErr == io.EOF {
			break
		}
		sourceLineIndex++ // Increment line counter
	}
	return nil
}

func fileLastTouched(filePath string) (bool, time.Time) {
	file, err := os.Stat(filePath)
	if err != nil {
		return false, time.Date(9999, 12, 31, 0, 0, 0, 0, time.UTC)
	}
	return true, file.ModTime()
}

// handle same line call and endcall: {% call ..... %}{% endcall %}
func handleWrappedLine(targetWriter *bufio.Writer, line string, dataSourceReference *templatecrawler.DataSourceReference, dataSources *map[string]datasourceparser.DataSourceFile) error {
	endRegex := regexp.MustCompile(regexp.QuoteMeta("{% endcall %}"))
	loc := endRegex.FindStringIndex(line)
	if loc == nil {
		return fmt.Errorf("error finding '{%% endcall %%}' in line %s", line)
	} else {
		beforeMatch := line[:loc[0]]
		afterMatch := line[loc[0]:]

		// Calculate leading whitespace for consistent indentation
		leadingWhitespace := beforeMatch[:len(beforeMatch)-len(strings.TrimLeft(beforeMatch, " \t"))]

		// Append the line before the match, then append the content, and finally the rest of the line
		if _, err := targetWriter.WriteString(beforeMatch + "\n"); err != nil {
			return err
		}

		ds := (*dataSources)[dataSourceReference.DataSourceFilePath]
		if err := ds.Formatter.Write(targetWriter); err != nil {
			return err
		}

		indentedAfterMatch := leadingWhitespace + afterMatch
		if _, err := targetWriter.WriteString(indentedAfterMatch); err != nil {
			return err
		}

		return nil
	}
}

func insertDataSource(targetWriter *bufio.Writer, dataSourceReference *templatecrawler.DataSourceReference, dataSources *map[string]datasourceparser.DataSourceFile) error {
	ds := (*dataSources)[dataSourceReference.DataSourceFilePath]
	return ds.Formatter.Write(targetWriter)
}