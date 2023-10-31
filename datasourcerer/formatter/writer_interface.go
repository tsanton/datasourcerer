package formatter

import "io"

type IWriter interface {
	Write(w io.Writer, content []byte) error
}
