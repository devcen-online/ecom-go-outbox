// Package migrations предоставляет golang-migrate совместимые миграции
// (up/down) для таблиц outbox_events и inbox_events (ERD-OB-001 §5).
// Первые миграции только аддитивные (CREATE TABLE); разрушающие изменения —
// двумя шагами add → backfill → drop.
//
// Использование (golang-migrate CLI или библиотека):
//
//	migrate -source iofs://migrations -database postgres://... up
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS
