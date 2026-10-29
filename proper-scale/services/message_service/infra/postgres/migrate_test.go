package postgres

import "testing"

func TestMigrationVersion(t *testing.T) {
	tests := []struct {
		name    string
		want    int64
		wantErr bool
	}{
		{name: "000001_create_message_storage.up.sql", want: 1},
		{name: "12_add_index.up.sql", want: 12},
		{name: "initial.up.sql", wantErr: true},
		{name: "000000_invalid.up.sql", wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := migrationVersion(test.name)
			if (err != nil) != test.wantErr {
				t.Fatalf("migrationVersion() error = %v, wantErr %v", err, test.wantErr)
			}
			if err == nil && got != test.want {
				t.Fatalf("migrationVersion() = %d, want %d", got, test.want)
			}
		})
	}
}

func TestParseMessageID(t *testing.T) {
	value := "0192f4d8-2c38-7a31-b5e4-40e8ef1d3ea6"
	parsed, err := parseMessageID(value)
	if err != nil {
		t.Fatal(err)
	}
	if !parsed.Valid || parsed.String() != value {
		t.Fatalf("parsed id = %q, valid %v; want %q", parsed.String(), parsed.Valid, value)
	}
	if _, err := parseMessageID("invalid"); err == nil {
		t.Fatal("parseMessageID() error = nil for invalid id")
	}
}
