package formatter

import (
	"io"
)

type IDataSourceFormatter interface {
	Read(r io.Reader) error
	Write(writer io.Writer) error
}
