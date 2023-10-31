package formatter

type ICsvHeader interface {
	GetName() string

	GetWriter() func(value interface{}) ([]byte, error)

	ParseHeader(signature string) error
}
