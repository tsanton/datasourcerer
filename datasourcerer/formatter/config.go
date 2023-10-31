package formatter

type Config struct {
	Dialect  string          `yaml:"dialect"`
	Filetype ParserInputType `yaml:"filetype"`
	CSV      CsvConfig       `yaml:"csv"`
}

type CsvConfig struct {
	Separator        string `yaml:"separator"`        //This is the field delimiter. It's set to a comma (,) by default
	Comment          string `yaml:"comment"`          //This is the comment character. Lines beginning with this character are ignored. '#' by default
	TrimLeadingSpace bool   `yaml:"trimLeadingSpace"` //Trim leading space flag. Defaults to true
}

func (s *CsvConfig) Validate() bool {
	if s.Separator != "" && s.Comment != "" {
		return true
	}
	return false
}

func NewDefaultCsvConfig() CsvConfig {
	return CsvConfig{
		Separator:        ",",
		Comment:          "#",
		TrimLeadingSpace: true,
	}
}
