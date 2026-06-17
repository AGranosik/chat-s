package migrations

import "embed"

// FS holds the goose SQL migrations, embedded so the binary is self-contained
// (no migrations directory needs to ship alongside it).
//
//go:embed *.sql
var FS embed.FS
