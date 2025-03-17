package jsonexpr

import (
	"slices"

	"github.com/go-faster/oteldb/internal/logql"
)

// SelectorTree is a simple prefix tree of selectors. Efficient for matching multiple JSON paths.
type SelectorTree struct {
	root *selectorNode
}

// IsEmpty returns true if the tree is empty.
func (p SelectorTree) IsEmpty() bool {
	return p.root == nil || len(p.root.sub) == 0
}

type selectorNode struct {
	labels []logql.Label
	sub    map[Selector]*selectorNode
}

// MakeSelectorTree creates a new SelectorTree from the given paths.
func MakeSelectorTree(paths map[logql.Label]Path) SelectorTree {
	if len(paths) == 0 {
		return SelectorTree{}
	}

	root := &selectorNode{}
	for label, p := range paths {
		n := root
		for i, s := range p {
			nn, ok := n.sub[s]
			if !ok {
				nn = &selectorNode{}
			}

			if i == len(p)-1 {
				if !slices.Contains(nn.labels, label) {
					nn.labels = append(nn.labels, label)
				}
			}

			if n.sub == nil {
				n.sub = map[Selector]*selectorNode{}
			}
			n.sub[s] = nn
			n = nn
		}
	}
	return SelectorTree{
		root: root,
	}
}
