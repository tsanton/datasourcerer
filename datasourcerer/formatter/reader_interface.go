package formatter

import "io"

type IReader interface {
	Read(r io.Reader) ([]byte, error)
}
