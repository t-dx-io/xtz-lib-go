package migration

import "fmt"

const (
	// MigrationTableName is the name of the DB table where we keep track of the migrations.
	MigrationTableName = "xtz_migrations"
	COCKROACH          = "cockroach"
)

func GetMigration(storageType string) (map[string]string, string, error) {
	switch storageType {
	case COCKROACH:
		return CockroachMigrations, MigrationTableName, nil
	default:
		return nil, "", fmt.Errorf("unknown storage type %q for XTZ migrations", storageType)
	}
}
