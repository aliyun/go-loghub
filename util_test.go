package sls

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCursorEncode(t *testing.T) {
	assert.Equal(t, encodeCursor(1729070618428044655), "MTcyOTA3MDYxODQyODA0NDY1NQ==")
	cursor, err := decodeCursor("MTcyOTA3MDYxODQyODA0NDY1NQ==")
	assert.Nil(t, err)
	assert.Equal(t, cursor, int64(1729070618428044655))

	assert.Equal(t, encodeCursor(0), "MA==")
	cursor, err = decodeCursor("MA==")
	assert.Nil(t, err)
	assert.Equal(t, cursor, int64(0))
}
