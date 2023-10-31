package testutils

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
	"github.com/sirkon/go-format"
)

type TestDirectories struct {
	RootDir string
	D1      string
	D2      string
}

/*
This function bootstraps the following directory structure for testing:

	b/
	│
	├── d1/
	│   └── d2/
*/
func bootstrapDir(pattern string) (*TestDirectories, error) {
	var err error
	d := &TestDirectories{}

	d.RootDir, err = os.MkdirTemp("", pattern)
	if err != nil {
		return nil, err
	}

	d.D1, err = os.MkdirTemp(d.RootDir, "d1")
	if err != nil {
		return nil, err
	}

	d.D2, err = os.MkdirTemp(d.D1, "d2")
	if err != nil {
		return nil, err
	}
	return d, nil
}

func CleanupDir(dir ...*TestDirectories) {
	for _, d := range dir {
		err := os.RemoveAll(d.RootDir)
		if err != nil {
			panic(err)
		}
	}
}

/*
Bootstraps the following directory structure for testing:

	datasourcerer
	|	|--- d1
	|	|    |--- d2
	outputs
	|   |--- d1
	|   |    |--- d2
*/
func BootstrapDirs() (*TestDirectories, *TestDirectories) {
	datasourcerer, err := bootstrapDir("datasourcerer")
	if err != nil {
		panic(err)
	}
	outputDir, err := bootstrapDir("outputs")
	if err != nil {
		panic(err)
	}
	return datasourcerer, outputDir
}

func CreateFile(dir, pattern, content string, formatValues format.Values) (*os.File, error) {
	// Create a temp file in the specified directory.
	var filePath string
	if strings.Count(pattern, "%s") > 0 {
		unique := uuid.New().String()
		filePath = filepath.Join(dir, fmt.Sprintf(pattern, unique))
	} else {
		filePath = filepath.Join(dir, pattern)
	}
	tempFile, err := os.Create(filePath)
	if err != nil {
		return nil, err
	}

	var writeContent string
	if len(formatValues) > 0 {
		writeContent = format.Formatm(content, formatValues)
	} else {
		writeContent = content
	}

	// Write the content to the temp file.
	_, err = tempFile.Write([]byte(writeContent))
	if err != nil {
		return nil, err
	}

	return tempFile, nil
}

func DeleteFile(tempFile *os.File) {
	// Delete the temp file.
	err := os.Remove(tempFile.Name())
	if err != nil {
		panic(err)
	}
}

func GetGeneratorFile(generatorBaseDir, baseTemplateDir, templateFilePath string) (string, error) {
	path, err := filepath.Rel(baseTemplateDir, templateFilePath)
	if err != nil {
		return "", err
	}
	fullPath := filepath.Join(generatorBaseDir, path)
	_, err = os.Stat(fullPath)
	if err != nil {
		return "", err
	}
	bytes, err := os.ReadFile(fullPath)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}
