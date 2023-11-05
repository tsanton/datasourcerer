package utils

var TimeFormatMapper = map[string]string{
	"HH:mm:ss":         "15:04:05",              // Example: "14:30:45"
	"hh:mm:ss tt":      "03:04:05 PM",           // Example: "02:30:45 PM"
	"HH:mm":            "15:04",                 // Example: "14:30"
	"hh:mm tt":         "03:04 PM",              // Example: "02:30 PM"
	"HH:mm:ss.SSS":     "15:04:05.000",          // Example: "14:30:45.123"
	"hh:mm:ss.SSS tt":  "03:04:05.000 PM",       // Example: "02:30:45.123 PM"
	"HH:mm:ssZ":        "15:04:05Z07:00",        // Example: "14:30:45Z"
	"hh:mm:ss ttZ":     "03:04:05 PMZ07:00",     // Example: "02:30:45 PMZ"
	"HH:mm:ss.SSSZ":    "15:04:05.000Z07:00",    // Example: "14:30:45.123Z"
	"hh:mm:ss.SSS ttZ": "03:04:05.000 PMZ07:00", // Example: "02:30:45.123 PMZ"
}
