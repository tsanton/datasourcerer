package variant

// import (
// 	"regexp"

// 	"github.com/tsanton/dbt-unit-test-fusionizer/formatter"
// )

// var _ formatter.ICsvHeader = &Number{}

// // Signature must contains "[boolean" (case insensitive) at any position and ends with ")]"
// var snowflakeNumberSignatureRegex = regexp.MustCompile(`^(\w+)\[number\((.*?)\)\]$`)

// const SnowflakeNumberSignature = "[number("

// type Number struct {
// 	fieldName string
// 	precision int
// 	scale     int
// }

// // GetName implements formatter.ICsvHeader
// func (m *Number) GetName() string {
// 	return m.fieldName
// }

// // GetWriter implements formatter.ICsvHeader.
// func (b *Number) GetWriter() func(value interface{}) []byte {
// 	return func(value interface{}) []byte {
// 		return []byte(value.(string))
// 	}
// }

// // GetWriter implements formatter.ICsvHeader.
// func (b *Number) ParseHeader(signature string) error {
// 	return nil
// }
