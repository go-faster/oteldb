package ytlocal

import (
	"fmt"

	"github.com/google/uuid"
)

// GenerateCellID generates a cell ID from a cell tag and a name.
func GenerateCellID(cellTag int16, name string) string {
	id := uuid.NewMD5(uuid.UUID{}, []byte(name))

	// Encode cell tag.
	id[4] = byte(cellTag >> 8)
	id[5] = byte(cellTag & 0xff)

	// Encode master cell.
	const masterCellType = int16(601)
	id[6] = byte(masterCellType >> 8)
	id[7] = byte(masterCellType & 0xff)

	return fmt.Sprintf("%x-%x-%x-%x", id[12:16], id[8:12], id[4:8], id[0:4])
}
