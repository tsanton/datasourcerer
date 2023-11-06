package templatecrawler

import (
	"bufio"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

type TestTemplateFile struct {
	logger               *slog.Logger
	baseTemplateDir      string
	relativeFileDir      string
	absFileDir           string
	fileName             string
	lastChanged          time.Time
	dataSourceReferences []DataSourceReference
}

func (s *TestTemplateFile) RelativeFilePath() string {
	return filepath.Join(s.relativeFileDir, s.fileName)
}

func (s *TestTemplateFile) AbsFileDir() string {
	return s.absFileDir
}

func (s *TestTemplateFile) AbsFilePath() string {
	return filepath.Join(s.absFileDir, s.fileName)
}

func (s *TestTemplateFile) LastChanged() time.Time {
	return s.lastChanged
}

func (s *TestTemplateFile) DataSourceReferences() *[]DataSourceReference {
	return &s.dataSourceReferences
}

func NewTestTemplateFile(logger *slog.Logger, baseTemplateDir string) *TestTemplateFile {
	return &TestTemplateFile{
		logger:          logger,
		baseTemplateDir: baseTemplateDir,
	}
}

func (s *TestTemplateFile) ProccessFile(inputFilePath string) {
	var err error
	s.logger.Debug(fmt.Sprintf("processing test template file: '%s", inputFilePath))
	s.fileName = filepath.Base(inputFilePath)
	dir := filepath.Dir(inputFilePath)
	absDir, _ := filepath.Abs(dir)
	s.absFileDir = absDir
	s.relativeFileDir, err = filepath.Rel(s.baseTemplateDir, absDir)
	if err != nil {
		s.logger.Debug("error calculating relative path of template file to test directory")
	}

	file, err := s.openFile(inputFilePath)
	if err != nil {
		s.logger.Error(fmt.Sprintf("error processing test template file '%s': %s", inputFilePath, err.Error()))
	}
	defer file.Close()
	err = s.parseFile(file)
	if err != nil {
		s.logger.Error(fmt.Sprintf("error processing test template file '%s': %s", inputFilePath, err.Error()))
	}
}

func (s *TestTemplateFile) openFile(path string) (io.ReadCloser, error) {
	fileInfo, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("cannot get file info: %w", err)
	}
	s.lastChanged = fileInfo.ModTime()

	// Open the file
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("cannot open file: %w", err)
	}
	return file, nil
}

var funcCallRegex = regexp.MustCompile(`dbt_unit_testing.mock_ref|dbt_unit_testing.mock_source|dbt_unit_testing.expect`)
var endCallRegex = regexp.MustCompile(`{%\s*endcall\s*%}`)

var sourceFileRegex = regexp.MustCompile(`\s*(?:'|")source_file(?:'|")\s*:\s*(?:'|")([^']*)(?:'|")`)

var dbtInputFormatRegex = regexp.MustCompile(`\s*(?:'|")input_format(?:'|")\s*:\s*(?:'|")([^']*)(?:'|")`)

func lineEndsWithClosingTag(line string) bool {
	line = strings.TrimSpace(line)
	return strings.HasSuffix(line, "%}")
}

func (s *TestTemplateFile) parseFile(file io.ReadCloser) error {
	scanner := bufio.NewScanner(file)
	lineNumber := 0
	callStack := make([]int, 0)

	for scanner.Scan() {
		line := scanner.Text()
		lineNumber++
		if funcCallRegex.MatchString(line) {
		EndCallNotOneLiner:
			endsWithBlockCloser := lineEndsWithClosingTag(line)
			if !endsWithBlockCloser {
				if !scanner.Scan() {
					// Handle the case where there are no more lines to read
					break
				}
				line += scanner.Text()
				lineNumber++
				goto EndCallNotOneLiner
			}
			//Verfied: "% }" is considered malformed by DBT -> i.e. we can expect "%}"
			//Verified: in Jinja2 and DBT you cannot have a one-liner macro call without the expected {% endcall %}. The {% endcall %} statement is required to explicitly close the {% call %} block
			var sourceFile string
			sourceMatches := sourceFileRegex.FindStringSubmatch(line)
			if len(sourceMatches) >= 2 {
				// sourceFile = sourceMatches[1]
				//TODO: verify
				if filepath.IsAbs(sourceMatches[1]) {
					sourceFile = sourceMatches[1]
				} else {
					sourceFile, _ = filepath.Abs(filepath.Join(s.absFileDir, sourceMatches[1]))
				}
				s.logger.Debug(fmt.Sprintf("source file '%s' referenced in test template file '%s'", sourceFile, s.AbsFilePath()))
			}
			dbtInputFormatMatches := dbtInputFormatRegex.FindStringSubmatch(line)
			if len(dbtInputFormatMatches) >= 2 {
				if dbtInputFormatMatches[1] != "sql" {
					s.logger.Error("dbt_unit_testing templated by the datasourcerer supports sql input format only")
					//TODO: return error?
				}
			}
			if sourceFile != "" {
				ds := DataSourceReference{
					TestDefintionFilePath: s.absFileDir,
					DataSourceFilePath:    sourceFile,
					CallLine:              lineNumber,
				}
				if endCallRegex.MatchString(line) {
					ds.EndCallLine = lineNumber
					s.dataSourceReferences = append(s.dataSourceReferences, ds)
				} else {
					s.dataSourceReferences = append(s.dataSourceReferences, ds)
					callStack = append(callStack, len(s.dataSourceReferences)-1)
				}
			}
			//TODO: if not found endcall before union or end of file, empty result and return error
		} else if endCallRegex.MatchString(line) || strings.Contains(line, "UNION ALL") {
			if len(callStack) > 0 {
				index := callStack[len(callStack)-1]
				callStack = callStack[:len(callStack)-1]
				s.dataSourceReferences[index].EndCallLine = lineNumber
			}
		}
	}

	if err := scanner.Err(); err != nil {
		s.dataSourceReferences = []DataSourceReference{}
		return fmt.Errorf("error scanning file: %w", err)
	}
	return nil
}
