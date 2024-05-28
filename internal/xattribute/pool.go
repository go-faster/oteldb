package xattribute

import "sync"

type stringSlice struct {
	val []string
}

func (s *stringSlice) Reset() {
	s.val = s.val[:0]
}

var stringSlicePool = &sync.Pool{
	New: func() any {
		return &stringSlice{
			val: make([]string, 0, 16),
		}
	},
}

func getStringSlice() *stringSlice {
	ss := stringSlicePool.Get().(*stringSlice)
	ss.Reset()
	return ss
}

func putStringSlice(ss *stringSlice) {
	stringSlicePool.Put(ss)
}
